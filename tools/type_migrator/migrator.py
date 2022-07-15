import hashlib
import logging
import os
import s3fs
import time

from boto3 import Session
from pyarrow import compute as pc
from pyarrow import parquet as pq
from threading import Lock
from typing import NamedTuple, List, Type, Tuple, Set

from bi_etl.balances_eng.bts_core_balances.constants import ZERO
from bi_etl.glue_client import GlueClient
from bi_etl.utils import extract_partition_location
from tools.update_catalog.cli import update_table
from tools.update_catalog.parallel import run_parallel
from tools.update_catalog.s3utils import S3Uri

REGION = ''

logger = logging.getLogger(__name__)
file_logger = logging.getLogger('file_logging')
file_handler = logging.FileHandler('casting_diff.log')
file_logger.setLevel(logging.DEBUG)
file_logger.addHandler(file_handler)


class ColumnMigration(NamedTuple):
    column: str
    from_type: Type
    to_type: Type


class PartitionMigrationUris(NamedTuple):
    partition_location: S3Uri
    temp_location: S3Uri
    backup_location: S3Uri


def copy_to_local(s3: s3fs.S3FileSystem, src: S3Uri, dst: str):
    t = time.time()
    logger.info(f'Copying from {src} to {dst}...')
    s3.download(str(src), dst)
    finished_time = time.time() - t
    logger.info(f'Copied from {src} to {dst} in {finished_time} seconds')
    return finished_time


def hashit(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def migrate_columns(parquet_fn_path: str, src: S3Uri, migration_config: List[ColumnMigration],
                    safe_migration: bool):
    pq_dataset = pq.ParquetDataset(parquet_fn_path).read()
    arrow_schema = {k: v for k, v in zip(pq_dataset.schema.names, pq_dataset.schema.types)}
    migrations_performed = 0
    # Loop over columns and migrate data types (pyarrow does not support bulk data type migrations)
    for cm in migration_config:
        if cm.column not in arrow_schema:
            logger.warning(f'Column {cm.column} missing in parquet {src}')
            continue
        # Target column should be present
        original_column_schema = arrow_schema[cm.column]
        if original_column_schema == cm.to_type:
            # Already done
            logger.info(f'Column {cm.column} in {src} already migrated to {cm.to_type}')
        else:
            # Warning in case of from_type mismatch
            if original_column_schema != cm.from_type:
                # Unexpected type
                logger.warning(f'Column {cm.column} in {src} has an unxpected type {original_column_schema} '
                               f'(was expecting {cm.from_type})')
            logger.info(
                f'Column {cm.column} in {src} matches from_type {cm.from_type}, migrating to {cm.to_type}')
            # Migrate data type using pyarrow native conversion
            pq_convert = pq_dataset.column(cm.column).cast(cm.to_type, safe=safe_migration)

            # Check for data loss
            if not safe_migration:
                diff = pc.sum(pc.subtract(pq_dataset.column(cm.column), pq_convert))
                if pc.greater(diff, ZERO):
                    logger.warning(f'Casting incurred a data loss of {diff} in \'{cm.column}\'!')
                    file_logger.error(f'cast_diff={diff}')
                else:
                    logger.info(f'No data loss occured as a result of unsafe cast.')

            dataset_converted = pq_dataset.set_column(
                pq_dataset._ensure_integer_index(cm.column),
                cm.column,
                pq_convert)
            assert dataset_converted.column(cm.column).type == cm.to_type
            pq_dataset = dataset_converted
            logger.info(
                f'Column {cm.column} in {src} matches from_type {cm.from_type}, successfully migrated  '
                f'to {cm.to_type}')
            migrations_performed += 1
    return None if migrations_performed == 0 else pq_dataset


def migrate_parquet_multi(s3: s3fs.S3FileSystem, src: S3Uri, dst: S3Uri,
                          migration_configs: List[ColumnMigration], safe_migration: bool):
    t = time.time()
    success = True
    # Save original parquet locally
    tempfn = hashit(f'{str(src)}{str(dst)}{str(time.time())}{"".join(str(mc) for mc in migration_configs)}')
    copy_to_local(s3, src, tempfn)
    migrated_pqt = tempfn
    # Migrate columns
    try:
        migrated_pqt = migrate_columns(migrated_pqt, src, migration_configs, safe_migration=safe_migration)
    except Exception as e:
        logger.error(f'Error migrating {src} ({migration_configs}): {e}')
        migrated_pqt = None
        success = False
    migration_performed = False
    if migrated_pqt is not None:
        # Save to disk, if migration was performed
        outfn = f'{tempfn}_migrated'
        pq.write_table(table=migrated_pqt, where=outfn, use_deprecated_int96_timestamps=True)
        # Upload back to S3
        logger.info(f'Saved migrated parquet to {outfn}, uploading to {dst}...')
        s3.upload(outfn, str(dst))
        logger.info(f'Done, migrated {src} to {dst} in {"/".join([i.column for i in migration_configs])} in '
                    f'{int(time.time() - t)} seconds')
        migration_performed = True
        try:
            os.remove(outfn)
        except Exception as e:
            logger.info(f'Error removing local file {outfn} for src {src}: {e}')
    else:
        logger.info(f'Skipping migration of {src}, configs {migration_configs} ')

    try:
        os.remove(tempfn)
    except Exception as e:
        logger.info(f'Error removing local file {tempfn} for src {src}: {e}')
    return migration_performed, success


def get_etl_part(table_name: str, u: S3Uri):
    """
    """
    tokns = str(u).split('/')
    assert sum(1 if t == table_name else 0 for t in tokns) == 1, \
        f'Expecting exactly one occuerence of table_name {table_name} in {u}'
    return '/'.join(tokns[tokns.index(table_name):])


def get_partition_uris_glue_table(db: str, table_name: str, dest_base: S3Uri, backup_base: S3Uri):
    g = GlueClient(db, 's3://', REGION)
    parts = list(g.get_partitions(database=db, table_name=table_name))
    assert parts, f'No partitions obtained for {db}.{table_name}'
    logger.info(f'Listed {len(parts)} partitions for {db}.{table_name}')
    partition_etl_map = {p: get_etl_part(table_name, p) for p in
                         [S3Uri(extract_partition_location(p)) for p in parts]}
    return [PartitionMigrationUris(p, dest_base.append(partition_etl_map[p]),
                                   backup_base.append(partition_etl_map[p])) for p in partition_etl_map]


def get_partition_uris_s3_path(s3_loc: S3Uri, dest_base: S3Uri, backup_base: S3Uri):
    session = Session(region_name=REGION)
    s3_resource = session.resource('s3')
    buckt = s3_resource.Bucket(s3_loc.bucket)
    objects = buckt.objects.filter(Prefix=s3_loc.key)
    parquet_map = [('/'.join(pqfn.key.split('/')[:-1]) + '/',
                    S3Uri('s3://' + buckt.name + '/' + '/'.join(pqfn.key.split('/')[:-1]) + '/')) for pqfn in
                   objects]
    return [PartitionMigrationUris(p[1], dest_base.append(p[0]), backup_base.append(p[0])) for p in
            parquet_map]


def para_migrate(s3, migration_uris, cols, hot_run, workers, backwards, safe_mig) \
        -> Tuple[bool, List[Tuple[str, str]], List[Tuple[str, str]]]:
    lock, to_copy, encountered_errors = Lock(), set(), set()

    def run(puris):
        def f():
            migration_success, src_dst_pairs = migrate_partition(s3, puris, cols, hot_run, safe_mig)
            with lock:
                if migration_success:
                    if src_dst_pairs:
                        to_copy.update(src_dst_pairs)
                else:
                    encountered_errors.update(src_dst_pairs)

        return f

    tasks = [run(puri) for puri in
             sorted(list(migration_uris), key=lambda x: x.partition_location, reverse=True)]
    if backwards:
        tasks = tasks[::-1]
    logger.info(f'Prepared {len(tasks)} tasks, will process using {workers} workers')
    para_run_success = run_parallel(tasks, workers=workers)

    return para_run_success, list(to_copy), list(encountered_errors)


def copy_back(s3, src_dst_pairs: List[Tuple[str, str]], workers: int):
    def task_factory(src, dst):
        def f():
            assert len(s3.ls(str(dst))) == 1
            s3.cp(str(dst), str(src))
            logger.info(f'Copied migrated {str(dst)} back to source {str(src)}')

        return f

    tasks = [task_factory(src, dst) for (src, dst) in src_dst_pairs]
    logger.info(f'Will copy back {len(tasks)} parquet files.')
    return run_parallel(tasks, workers=workers)


def glue_based_migration(dest_base: S3Uri, backup_base: S3Uri, column_migs: List[ColumnMigration],
                         db: str = None, table_name: str = None, db_loc: S3Uri = None,
                         workers: int = 8,
                         hot_run: bool = False, backwards: bool = False, is_sensitive: bool = False,
                         safe_mig: bool = False):
    t = time.time()
    logger.info(
        f'Migrating parquets in {db}.{table_name} Glue table (DB loc {db_loc}), columns {column_migs}, '
        f'{workers} workers, hot_run={hot_run}, backwards={backwards}'
        f' (backup: {backup_base}, tmp: {dest_base})')
    migration_uris = get_partition_uris_glue_table(db, table_name, dest_base, backup_base)
    logger.info(f'Loaded {len(migration_uris)} PartitonMigrationUris for {db}.{table_name};'
                f'will migrate {len(column_migs)} columns: {column_migs}. HOT_RUN={hot_run}')

    success = process_migration_uris(migration_uris, column_migs, workers, backwards, hot_run, is_sensitive,
                                     safe_mig)
    if not success:
        raise RuntimeError
    else:
        logger.info(f'Successfully processed {len(migration_uris)} migration uris for {column_migs} '
                    f'for {db}.{table_name} in {int(time.time() - t)}  seconds.')
        logger.info(f'Recreating Glue table {db}.{table_name} @ {db_loc}...')
        if hot_run:
            drop_and_recreate(db, table_name, db_loc)
        else:
            logger.info(f'Skipping recreation of {db}.{table_name} (hot_run={hot_run})')
        logger.info(
            f'Migration of {db}.{table_name} for {column_migs} done  in {int(time.time() - t)} seconds')


def s3_path_based_migration(dest_base: S3Uri, backup_base: S3Uri, column_migs: List[ColumnMigration],
                            s3_path: S3Uri = None,
                            workers: int = 8,
                            hot_run: bool = False, backwards: bool = False, is_sensitive: bool = False,
                            safe_mig: bool = False):
    t = time.time()
    logger.info(f'Migrating parquets in {s3_path} tree, columns {column_migs}, '
                f'{workers} workers, hot_run={hot_run}, backwards={backwards}'
                f' (backup: {backup_base}, tmp: {dest_base})')
    migration_uris = get_partition_uris_s3_path(s3_path, dest_base, backup_base)
    logger.info(f'Loaded {len(migration_uris)} PartitonMigrationUris in {s3_path};  '
                f'will migrate {len(column_migs)} columns: {column_migs}. HOT_RUN={hot_run}')
    success = process_migration_uris(migration_uris, column_migs, workers, backwards, hot_run, is_sensitive,
                                     safe_mig)
    if not success:
        raise RuntimeError
    else:
        logger.info(f'Successfully processed {len(migration_uris)} migration uris for {column_migs} '
                    f'@ location tree {s3_path}  in {int(time.time() - t)}  seconds.')


def process_migration_uris(migration_uris: List[PartitionMigrationUris],
                           column_migs: List[ColumnMigration],
                           workers: int, backwards: bool, hot_run: bool, is_sensitive: bool = False,
                           safe_mig: bool = False):
    s3 = s3fs.S3FileSystem(
        session=Session(region_name=REGION, profile_name='elevated') if is_sensitive else Session(
            region_name=REGION),
        config_kwargs={'max_pool_connections': workers * 4})
    para_run_success, to_copy_pairs, errors_pairs \
        = para_migrate(s3, migration_uris, column_migs, hot_run, workers, backwards, safe_mig)
    do_copy = True
    if not para_run_success:
        do_copy = False
        logger.error(
            f'Encountered errors in parallel run of {migration_uris} using {column_migs}, will not copy over')
    elif errors_pairs:
        do_copy = False
        logger.error(f'Encountered errors in miration of {migration_uris} using {column_migs} '
                     f'for {len(errors_pairs)} pairs ({str(errors_pairs)[:500]}...) will not copy over')
    successful_copy = True
    if do_copy and hot_run:
        successful_copy = copy_back(s3, to_copy_pairs, workers)
    return do_copy and successful_copy


def drop_and_recreate(db: str, table: str, db_loc: S3Uri, is_sensitive: bool = False):
    g = GlueClient(db, 's3://', REGION)

    if g.table_exists(table, db):
        g.delete_table(table, db)
        logger.info(f'{db}.{table} dropped')
        update_table(
            boto3_session=Session(region_name=REGION, profile_name='elevated') if is_sensitive
            else Session(region_name=REGION),
            database=db,
            db_location=db_loc.with_trailing_slash(),
            tables=[table]
        )
        logger.info(f'{db}.{table} dropped and recreated in Glue')

    else:
        logger.warning(f'{db}.{table} does not exist')


def migrate_partition(s3, puris: PartitionMigrationUris, cols: List[ColumnMigration], hot_run: bool = False,
                      safe_mig: bool = True) -> Tuple[bool, Set[Tuple[str, str]]]:
    total_success, any_migration_performed = True, set()
    for pqt in s3.ls(str(puris.partition_location)):
        pqt_fn = pqt.split('/')[-1]
        src = puris.partition_location.append(pqt_fn)
        dst = puris.temp_location.append(pqt_fn)
        bck = puris.backup_location.append(pqt_fn)
        migration_performed, success = migrate_parquet_multi(s3, src, dst, cols, safe_migration=safe_mig)
        if hot_run and migration_performed:
            s3.cp(str(src), str(bck))
            logger.info(f'Backed up {str(src)} to {str(bck)}')
            if not success:
                total_success = False
                logger.error(f'Failed migrating {pqt} ({puris}) using {cols}')
            else:
                any_migration_performed.add((str(src), str(dst)))
    return total_success, any_migration_performed

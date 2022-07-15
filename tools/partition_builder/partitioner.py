import logging
import os
import s3fs
import pyarrow as pa
from boto3 import Session
from pyarrow.compute import ascii_lpad, call_function
from pyarrow.parquet import ParquetFile, write_to_dataset, write_table
from threading import Lock
from typing import NamedTuple, List

from tools.update_catalog.parallel import run_parallel
from tools.type_migrator.migrator import copy_to_local, hashit

REGION = ''
logger = logging.getLogger(__name__)


class PartitionSpec(NamedTuple):
    tbl_col: str
    part_name: str
    part_func: str = ''
    dmh: bool = False


def s3_parquet_lister(s3_path: str, s3: s3fs.S3FileSystem):
    paths = []

    def list_items(p=s3_path):
        tmp_p = s3.ls(p, detail=True)
        for l in tmp_p:
            if l['type'] == 'directory':
                list_items(l['name'])
            else:
                paths.append(l['name'])

    list_items()
    assert len(paths) > 0, f'No files found @ {s3_path}, recheck S3 address.'
    logger.info(f'Found {len(paths)} parquet files in \'{s3_path}\'')

    return paths


def partition_wrangler(pa_table: pa.lib.Table, spec: PartitionSpec, **kwargs) -> pa.lib.Table:
    """
    Transform a pyarrow table by expanding/reducing it with new partition columns. The partition can be
    generated with a desired function by passing in the func argument. Get the list of all available functions
     from pyarrow.compute.list_functions().
        * pa_tbl [pyarrow.Table] - PyArrow Table data structure
        * spec [NamedTuple] - Placeholder item containing specifications for each partition:
            - tbl_col: Source table column name to be used in partition generations
            - part_name: Name of the new partition
            - part_func: Nmme of the function to be used in the transformation
            - dmh: True is partition needs to be '0' padded
        * **kwargs - additional parameters passed to fine tune the function call (see documentation for
            pyarrow.compute.call_function)
    """
    # append a newly created partition column
    if spec.part_func:
        logger.info(f'Partition will be generated using the following PartitionSpec arguments: '
                    f'{", ".join([str(i) for i in list(spec._asdict().values())])}')
        partition_tbl = pa_table.append_column(
            spec.part_name, call_function(name=spec.part_func, args=[pa_table.column(spec.tbl_col)], **kwargs)
        )
        logger.info(f'Created partition \'{spec.part_name}\' using table column \'{spec.tbl_col}\'')
    else:
        partition_tbl = pa_table.append_column(spec.part_name, [pa_table.column(spec.tbl_col)])
        logger.info(f'Set table column \'{spec.tbl_col}\' to partition \'{spec.part_name}\'')
    # if day/month/hour left-pad single number values with 0
    if bool(spec.dmh):
        dm_col = partition_tbl.column(spec.part_name).cast(pa.string())
        dm_col = ascii_lpad(dm_col, width=2, padding='0')
        logger.info(f'Partition \'{spec.part_name}\' was successfully zero-padded')
        dataset_converted = partition_tbl.set_column(
            partition_tbl._ensure_integer_index(spec.part_name),
            spec.part_name,
            dm_col)

        return dataset_converted.combine_chunks()
    else:
        return partition_tbl.combine_chunks()


def parquet_modifier(s3: s3fs, s3_address: str, spec: List[PartitionSpec]) -> str:
    logger.info(f'Starting migration of \'{s3_address}\'...')
    temp_pq = hashit(f'{s3_address}')
    copy_to_local(s3, s3_address, temp_pq)
    try:
        pa_tbl = ParquetFile(temp_pq).read()
    except pa.lib.ArrowInvalid as e:
        logger.error(f'ERROR: file is not a .parquet type ({type(temp_pq)}): {e}')
        logger.warning(f'Skipping file \'{s3_address.split("/")[-1]}\'')
    # change the partitions
    for s in spec:
        pa_tbl = partition_wrangler(pa_tbl, s)
    pa_split = f'{temp_pq}_split'
    write_table(table=pa_tbl, where=pa_split, use_deprecated_int96_timestamps=True)
    logger.info(f'Parquet file {temp_pq} has been successfully split and saved as f{pa_split}')

    try:
        os.remove(temp_pq)
    except Exception as e:
        logger.error(f'ERROR: removing local file {temp_pq} for source {s3_address} failed: {e}')

    return pa_split


def splitter_main(s3_loc: str, spec: List[PartitionSpec], upload_to: str, is_sensitive: bool,
                  workers: int, use_fs: bool) -> None:
    s3 = s3fs.S3FileSystem(
        session=Session(region_name=REGION, profile_name='elevated') if is_sensitive else Session(
            region_name=REGION),
        config_kwargs={'max_pool_connections': workers * 4})
    parquets_to_modify = s3_parquet_lister(s3_loc, s3)
    lock = Lock()

    def split_fact(s3l):
        def f():
            with lock:
                split_file = parquet_modifier(s3, s3l, spec)
            write_to_dataset(table=ParquetFile(split_file).read(), root_path=upload_to,
                             partition_cols=[s.part_name for s in spec],
                             partition_filename_cb=lambda x: f'{s3l.split("/")[-1]}',
                             compression='SNAPPY', filesystem=s3 if use_fs else None)
            logger.info(f'{s3l.split("/")[-1]} has been partitioned by {"--".join([s.part_name for s in spec])} '
                        f'and uploaded to \'{upload_to}\'')

        return f

    tasks = [split_fact(i) for i in parquets_to_modify]
    logger.info(f'Generated {len(tasks)} tasks that will be run in parallel using {workers} workers')
    run_parallel(tasks, workers=workers)


if __name__ == '__main__':
    # Test run
    from tools.partition_builder.cli import parse_spec_string

    spc = parse_spec_string("")
    splitter_main(
        s3_loc='s3://',
        spec=spc, upload_to='',
        is_sensitive=True, use_fs=False, workers=4
    )

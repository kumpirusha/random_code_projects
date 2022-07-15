import s3fs
import time
import json
import logging

from boto3 import client, Session
from pyarrow import parquet as pq
from typing import Union

from bi_etl.utils import configure_console_logging, extract_partition_location
from bi_etl.glue_client import GlueClient
from tools.update_catalog.parallel import run_parallel
from tools.update_catalog.s3utils import S3Uri

REGION = ''

logger = logging.getLogger(__name__)
configure_console_logging()


def compare_pq_schema(s3: s3fs, last_pq: str, test_pq: str, mismatch_container: list):
    """
    Function checks if two parquet schemas match. Discrepancies are returned as parquet/schema names/schema
    types tuples.
    """

    last = pq.ParquetDataset(path_or_paths=last_pq, filesystem=s3, metadata_nthreads=8).read()
    test = pq.ParquetDataset(path_or_paths=test_pq, filesystem=s3, metadata_nthreads=8).read()

    cols = set(last.column_names)
    cols.update(test.column_names)

    last_meta = {col: dtype for col, dtype in zip(last.schema.names, last.schema.types)}
    test_meta = {col: dtype for col, dtype in zip(test.schema.names, test.schema.types)}
    new_meta = dict()

    for col in cols:
        try:
            new_meta[col] = (test_meta[col], last_meta[col], last_meta[col] == test_meta[col])
        except KeyError:
            logger.warning(
                f'Column \'{col}\' does not exist in the most recent parquet: {last_pq}. Skipping check...')

    mismatch = {k: (v[0].__str__(), v[1].__str__()) for k, v in new_meta.items() if not new_meta[k][-1]}

    if mismatch:
        logger.warning('Parquet schemas do not match for column(s) {c} in {p}. Adding to list...'.format(
            c=', '.join([i.upper() for i in list(mismatch.keys())]), p=test_pq))
        mismatch_container.append({test_pq: mismatch})
    else:
        logger.info(f'Parquet schemas for {last_pq} and {test_pq} match.')


def task_factory(last_pq, test_pq, schema_mis, workers, is_sensitive: bool):
    def f():
        s3 = s3fs.S3FileSystem(
            session=Session(region_name=REGION, profile_name='elevated' if is_sensitive else None),
            config_kwargs={'max_pool_connections': workers * 2}
        )
        compare_pq_schema(s3, last_pq, test_pq, schema_mis)

    return f


def get_partition_parquets(db: str, table_name: str):
    g = GlueClient(db, 's3://', REGION)
    parts = list(g.get_partitions(database=db, table_name=table_name))
    assert parts, f'No partitions obtained for {db}.{table_name}'
    logger.info(f'Listed {len(parts)} partitions for {db}.{table_name}')

    # List all parquet files for selected table and sort them
    part_pqs = [S3Uri(extract_partition_location(p)) for p in parts]
    part_pqs.sort()

    return part_pqs


def parquet_schema_checker(db: str, table: str, s3_loc: S3Uri, workers: int, is_sensitive: bool,
                           scan_partitions: bool):
    session = Session(region_name=REGION)

    if scan_partitions:
        part_pq = get_partition_parquets(db=db, table_name=table)
        clt = session.client('s3')
        pq_list = []
        for i in part_pq:
            list_obj = clt.list_objects_v2(Bucket=i.bucket, Prefix=i.key)
            parq_clean = [f's3://{i.bucket}/{p["Key"]}' for p in list_obj['Contents']]
            pq_list.extend(parq_clean)
        pq_list.sort()

    else:
        s3_resource = session.resource('s3')
        buckt = s3_resource.Bucket(s3_loc.bucket)
        objects = buckt.objects.filter(Prefix=s3_loc.key)

        # List all parquet files for selected table and sort them
        pq_list = ['s3://' + pq.bucket_name + '/' + pq.key for pq in objects]
        pq_list.sort()

    assert len(pq_list) > 1, 'The path {s3_loc} contains only one parquet file. Skipping checker...'.format(
        s3_loc=s3_loc)

    # Compare table schemas
    schema_disc = []
    tasks = [task_factory(pq_list[-1], pq_list[p], schema_disc, workers, is_sensitive) for p in
             range(len(pq_list) - 1)]
    logger.info(f'Prepared {len(tasks)} tasks, will process using {workers} workers')

    t = time.time()
    run_parallel(tasks, workers)

    if not schema_disc:
        logger.info(f'{len(pq_list)} parquet files have been scanned in {int(time.time() - t)} seconds. Zero '
                    f'inconsistencies found.')
    else:
        logger.warning(
            f'{len(pq_list)} parquet files for {s3_loc} have been scanned in {int(time.time() - t)} seconds. '
            f'{len(schema_disc)} inconsistencies found.')
    return [schema_disc, pq_list[-1]]


def result_manager(database: str, table: str, s3_loc: S3Uri, checker_output: list,
                   upload_to_s3: Union[None, S3Uri]):
    """
    Function creates a JSON file from the table info and checker results which is then uploaded to the desired
    S3 location.
        *database[str] - table database name
        *table[str] - name of table to be checked
        *s3_loc[str] - parquet files S3 location
        *checker_outputlist([list, None], str) - results of the parquet_schema_checker(); can be None if no d
            iscrepancies are found
        *report_s3_loc[list] - location of the report in the form of a list of strings: [filename, ]
        *save_locally[bool] - if True file is saved locally else it's uploaded to selected S3 location
    """

    if checker_output[0]:
        import os

        json_temp = {
            'database': database,
            'table': table,
            'latest_parquet': checker_output[1],
            'parquet_file_location': str(s3_loc),
            'report_file_location': os.getcwd() + '/' + table if not upload_to_s3 else str(upload_to_s3),
            'checker_result': checker_output[0],
            'discrepancies': len(checker_output[0])
        }

        with open(table, 'w') as f:
            json.dump(json_temp, f)

        if upload_to_s3:
            import os

            try:
                s3_client = client('s3')
                s3_client.upload_file(
                    Filename=table,
                    Bucket=upload_to_s3.bucket,
                    Key=upload_to_s3.append(table).key)
                logger.info(f'File "{table}" has been successfully uploaded to: {S3Uri(upload_to_s3)}".')
                os.remove(table)
            except FileNotFoundError as e:
                logger.error(
                    f'Error while uploading the file {table} to "{upload_to_s3}": {e}')
    else:
        logger.info(f'No parquet data type discrepancies found in {database}.{table}.')


def main_run(argmts):
    import argparse

    parser = argparse.ArgumentParser(
        description='A parquet file misfit detector checks the parquet schemas of individual tables on S3 and '
                    'compares them with each other. If the data types do not exactly match the parquet in '
                    'question is added to an output list which can then be manually checked for inconsistencies. '
                    'The newest parquet file in the table is always declared as a benchmark against which all '
                    'others are then compared.'
    )
    parser.add_argument('-d', '--database',
                        help='Database name',
                        type=str,
                        required=True)
    parser.add_argument('-t', '--table',
                        help='Table name',
                        type=str,
                        required=True)
    parser.add_argument('-a', '--s3-address',
                        help='Location of table S3 files.',
                        type=S3Uri,
                        required=True)
    parser.add_argument('-S', '--sensitive',
                        help='Use \'elevated\' profile.',
                        default=False,
                        action='store_true')
    parser.add_argument('-ps', '--partition-scan',
                        help='Scan only active partitions instead of all parquet files.',
                        default=False,
                        action='store_true')
    parser.add_argument('-w', '--workers',
                        help='Number of workers to run in parallel',
                        type=int,
                        default=8)
    parser.add_argument('-u', '--s3_upload',
                        help='If the parameter is passed it must include the S3 folder location to which the'
                             'report will be uploaded.',
                        type=S3Uri,
                        default=None)
    args = parser.parse_args(argmts)
    result_manager(
        database=args.database,
        table=args.table,
        s3_loc=args.s3_address,
        checker_output=parquet_schema_checker(
            db=args.database,
            table=args.table,
            s3_loc=args.s3_address,
            workers=args.workers,
            is_sensitive=args.sensitive,
            scan_partitions=args.partition_scan
        ),
        upload_to_s3=args.s3_upload
    )


if __name__ == '__main__':
    import sys

    main_run(sys.argv[1:])

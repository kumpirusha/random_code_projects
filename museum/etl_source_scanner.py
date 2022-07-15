from tools.profiler.common import extract_etl_jobs
from tools.update_catalog.s3utils import S3Uri
from tools.common.aws_operations import AthenaAgent
from tools.dq_monitor.dq_job import get_named_rows

import boto3
from botocore.exceptions import ClientError
from collections import defaultdict

LOCAL_AWS_REGION = ''
LOCAL_S3_STG_DIR = ''


def bi_athena_etl_scanner(schema_tables_query: str,
                          project='bi_etl.athena',
                          output='checker',
                          aws_region: str = LOCAL_AWS_REGION,
                          s3_stg_dir: S3Uri = LOCAL_S3_STG_DIR):
    """
    Function compares existing 'etl' schema on Athena against production 'bitstamp' database in order
    to find the following inconsistencies:
        * missing references to existing snapshot tables
        * incorrect references to 'manual_input' and 'market_data' schemas**

    **Actual schema names are 'manual_upload' and 'bts_market_data' but in ETL construction process
    the alternative 'manual_input' and 'market_data' should be used,
    :param schema_tables_query: SQL DDL statement listing database tables.
    :param project: Name of the project scope for source table extraction.
    :param output: Define the desired output of the function. The function can either act as a checker for
    source tables or a crawler which lists all procedures and their respective source tables.
    :return: Sorted dict
    """
    # Obtain a list of snapshoted production tables and job tables
    jobs_list = extract_etl_jobs(project)
    job_tables = [jobs_list[job].dest_table + '.' + src for job in jobs_list for src in
                  jobs_list[job].source_tables]
    get_schema_tables, schema_errors = get_named_rows(aa=AthenaAgent(aws_region, str(s3_stg_dir)),
                                                      sql=schema_tables_query)
    if len(get_schema_tables) > 0 and not schema_errors:
        snapshotted_tbls_list = ['_'.join(i.get('tab_name').split('_')[:-1]) for i in
                                 get_schema_tables]
        # Compare the production table list to each ETL source tables to find discrepancies
        etl_source_discrepancies = [(t.split('.')[0], t.split('.')[2]) for t in job_tables if
                                    len(t.split('.')) > 2 and t.split('.')[
                                        2] in snapshotted_tbls_list]
        # Optional output - list all source tables for each procedure in 'etl' schema
        athena_etl_source = [tuple(t.split('.')) for t in job_tables]

        etl_source_dd = defaultdict(list)
        [etl_source_dd[k].append(v) for k, v in etl_source_discrepancies]
        # Check ETL schema discrepancies
        source_table_diff = [i.split('.') for i in job_tables if
                             'manual_upload' in i or 'bts_market_data' in i]

        return sorted(etl_source_dd.items()), source_table_diff if output == 'checker' else athena_etl_source
    else:
        print('Empty SQL query result. Check \'get_athena_data()\' input parameters.')


def bi_etl_source_table_crawler(db_schema: str, etl_tables: list):
    """
    Function finds all source tables for 'bi-etl-sync' by crawling through selected databases. Used
    to help identify source tables in BI 'etl' project.
    :param db_schema: A database (schema) to be crawled through.
    :param etl_tables: Input list of 'bi-etl' source tables obtained from 'extract_etl_jobs()'
    :return: A list of tuples containing the following data:
                1. database name
                2. table name
                3. column name
    """
    glue_client = boto3.client('glue')
    nt = ''
    schema_adjust = {
        'manual_input': 'manual_upload',
        'market_data': 'bts_market_data'
    }
    schema = db_schema if db_schema not in ('manual_input', 'market_data') else schema_adjust.get(
        db_schema)
    bi_etl_tables = [i[1] for i in etl_tables if i[0] == schema]
    source_tables_columns = []
    try:
        while True:
            response = glue_client.get_tables(DatabaseName=schema, NextToken=nt)
            [source_tables_columns.append((schema, tables['Name'])) for tables in
             response['TableList']]
            nt = response.get('NextToken')
            if nt is None:
                break
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print(f'Incorrect database schema entry - \'{schema}\'.')
        else:
            print(e.response['Error']['Code'])

    return [t for t in source_tables_columns if t[1] in bi_etl_tables]


def main():
    import csv

    src_tables = set()
    etl_jobs = extract_etl_jobs('bi_etl.athena')
    for job_name in etl_jobs:
        src_tables.update(set(etl_jobs[job_name].source_tables))
    input_tables = [t.split('.') for t in src_tables if '.' in t]
    etl_missing_snap, etl_source_error = bi_athena_etl_scanner(
        schema_tables_query='SHOW TABLES FROM bitstamp \'*snapshots\'',
        output='checker')
    # etl_inputs = bi_athena_etl_scanner(
    #     schema_tables_query='SHOW TABLES FROM bitstamp \'*snapshots\'',
    #     output='crawler')

    nt = '\n\t'
    print(f'ETL procedures missing the reference to snapshoted tables:'
          f'\n\t{nt.join([str(i) + " - " + j[0] + ": " + str(j[1]) for i, j in enumerate(etl_missing_snap)])}')
    print('-' * 100)
    print(f'ETL procedures with incorrect references to source tables:'
          f'\n\t{nt.join([str(i) + " - " + j[0] + ": " + str(j[1]) for i, j in enumerate(etl_source_error)])}')

    # Helper function used in identification of new source tables for 'bi-etl/athena'
    bi_etl_sources = [bi_etl_source_table_crawler(db_schema=i, etl_tables=input_tables) for i in
                      set([i[0] for i in input_tables])]
    bi_dq_tables = set([j[0] + '.' + j[1] for i in bi_etl_sources for j in i if
                        j[0] not in ('mapping', 'manual_upload')])

    # Write to local file
    with open('/Users/_/data_engineering/bi_dq_checker/source_tbls.csv', 'w') as t:
        writer = csv.writer(t)
        writer.writerow(['source_table'])
        writer.writerows([[i] for i in bi_dq_tables])


if __name__ == '__main__':
    main()

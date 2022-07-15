import boto3
import json
import logging

from json.decoder import JSONDecodeError
from tools.update_catalog.s3utils import S3Uri
from bi_etl.utils import configure_console_logging

logger = logging.getLogger(__name__)
configure_console_logging()


# Fetch JSON output from schema_misfit_detector.py
def json_fetcher(s3_address: str):
    s3_loc = S3Uri(s3_address)
    s3_sess = boto3.Session()
    s3_res = s3_sess.resource('s3')

    buckt = s3_res.Bucket(s3_loc.bucket)
    objects = buckt.objects.filter(Prefix=s3_loc.key)
    json_list = [i.get()['Body'].read() for i in objects]

    assert len(json_list) > 0, logger.error(f'The file {s3_loc} is empty or does not exist. Ending script.')

    misfit_jsons = []
    for i in json_list:
        try:
            jload = json.loads(i)
            misfit_jsons.append(jload)
        except JSONDecodeError as e:
            logger.warning(f'{e}: Wrong JSON format.')

    return misfit_jsons


# Extract relevant columns to migrate for each checked table
def ctm_json_parser(json_res):
    file_loc = json_res['parquet_file_location']
    db, tbl = json_res['database'], json_res['table']
    cols_to_migrate = []
    for i in json_res['checker_result']:
        cols_to_migrate.extend(list(list(i.values())[0].items()))

    cols_to_migrate = [(i[0], *i[1]) for i in cols_to_migrate]

    logger.info(f'Found {len(cols_to_migrate)} dtype discrepancies in {file_loc}.')

    return file_loc, db, tbl, list(set(cols_to_migrate))


# Parse the parameter for type_migrator.py into the template
def script_generator(type_mig_list: tuple, workers: int):
    mig_list = [s.replace(',', '').replace(')', '').replace('(', ' ').replace(')', '') if s.startswith(
        'decimal') else s.replace('double', 'float64') for tup in type_mig_list[-1] for s in tup]
    assert len(mig_list) % 3 == 0, f'Incorrect number of parameters in migrator script: {i[1]}'
    lot = []
    for n in range(2, len(mig_list), 3):
        lot.append(', '.join([i for i in mig_list[n - 2:n + 1]]))
    type_mig_template = 'python -m tools.type_migrator table -L {s3_loc} -d {database} -t {table} ' \
                        '-s "{mig_spec}" -w {workers} -H;'.format(s3_loc=type_mig_list[0],
                                                                  database=type_mig_list[1],
                                                                  table=type_mig_list[2],
                                                                  mig_spec='; '.join([i for i in lot]),
                                                                  workers=workers)

    logger.info(f'The following script has been generated: {type_mig_template}')

    return type_mig_template


def main_run(argmts):
    import argparse

    parser = argparse.ArgumentParser(
        description='Generate a script file for migrator tool to automate the migration process. The tool '
                    'takes the results of the misfit_detector and parses them into an acceptable form/script '
                    'which the data type migrator can then ingest.'
    )
    parser.add_argument('-s3', '--s3-address',
                        help='The location of the misfit_detector results.',
                        type=str,
                        required=True)
    parser.add_argument('-o', '--output-location',
                        help='Save the result of the script to a selected location (local machine).',
                        type=str,
                        required=True)
    parser.add_argument('-w', '--workers',
                        help='The number of workdes to be used in migration script.',
                        type=int,
                        default=8)
    args = parser.parse_args(argmts)

    res_json = json_fetcher(str(args.s3_address))
    checker_results_clean = []

    for i in res_json:
        checker_results_clean.append(tuple(ctm_json_parser(i)))

    migrator_scripts = []
    for res in checker_results_clean:
        migrator_scripts.append(script_generator(res, args.workers))

    with open(str(args.output_location), 'w') as parser:
        for line in migrator_scripts:
            parser.write(f'{line}\n')
    parser.close()


if __name__ == '__main__':
    import sys

    main_run(sys.argv[1:])

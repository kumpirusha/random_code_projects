import argparse
import datetime
import logging
from typing import List

import pyarrow as pa
import sys

from tools.common import cli
from tools.type_migrator.migrator import ColumnMigration, glue_based_migration, s3_path_based_migration
from tools.update_catalog.s3utils import S3Uri

logger = logging.getLogger(__name__)


def strip_tokenizer(s: str, separator: str) -> List[str]:
    return [t.strip() for t in s.strip().split(separator) if t.strip()]


def parse_spec_string(s: str) -> List[ColumnMigration]:
    """
    >>> parse_spec_string('col_name1, decimal128 20 8, float64; col_name2, int64, string')
    [ColumnMigration(column='col_name1', from_type=Decimal128Type(decimal128(20, 8)), to_type=DataType(double)),
     ColumnMigration(column='col_name2', from_type=DataType(int64), to_type=DataType(string))]
    """
    tokns = strip_tokenizer(s, ';')
    column_mig_specs = []
    for t in tokns:
        col_mig_tokns = strip_tokenizer(t, ',')
        assert len(col_mig_tokns) == 3, \
            f'Expecting three part spec for column migration (column_name, from_type, to_type),' \
            f' got {len(col_mig_tokns)}: {col_mig_tokns} (from "{t}")'
        col_name = col_mig_tokns[0]
        dtypes = []
        for tp_str in [col_mig_tokns[1], col_mig_tokns[2]]:
            tp = tuple(tp_str.split())
            if len(tp) == 1:
                dtp = getattr(pa, tp[0])()
            else:
                dtp, *params = tp
                assert len(params) <= 2, print('Too many parameters passed (max 2 possible).')
                if 'decimal' in dtp:
                    dtp = getattr(pa, tp[0])(int(params[0]), int(params[1]))
                else:
                    dtp = getattr(pa, tp[0])(int(params[0]))
            dtypes.append(dtp)
        column_mig_specs.append(ColumnMigration(col_name, dtypes[0], dtypes[1]))
    return column_mig_specs


def get_parser_params():
    run_ts = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    cli_params = (
        cli.ParserParam('-d', '--database', help='Database name', type=str, required=True),
        cli.ParserParam('-t', '--table', help='Table name', type=str, required=True),
        cli.ParserParam('-L', '--location', type=S3Uri, help='Database location', required=True),
        cli.ParserParam('-p', '--s3-path', help='Alternative parquet file determination via S3 address.',
                        type=S3Uri, required=True),
        cli.ParserParam('-da', '--destination-address',
                        help='Temporary S3 location for migrated parquet files.',
                        type=S3Uri,
                        default=S3Uri('s3://bi-temp-data/temp/migration/').append(run_ts)),
        cli.ParserParam('-ba', '--backup-address', help='Backup files S3 location.',
                        default=S3Uri('s3://bi-temp-data/temp/migration_backup/').append(run_ts),
                        type=S3Uri),
        cli.ParserParam('-s', '--spec', help='Spec for column(s) migration in the form of '
                                             '"column_name1, from_type1, to_type1; column_name2, from_type2, to_type2",'
                                             'e.g. "col_name1, decimal128 20 8, float64; col_name2, int64, string"',
                        type=str, required=True),
        cli.ParserParam('-w', '--workers', help='Number of workers to run in parallel', type=int, default=8),
        cli.ParserParam('-b', '--backwards', default=False, action='store_true',
                        help='Process from younger to older dates'),
        cli.ParserParam('-H', '--hot-run', default=False, action='store_true',
                        help='Hot run - overwrite original parquets with migrated ones'),
        cli.ParserParam('-S', '--sensitive', default=False, action='store_true',
                        help='Affected data is sensitive'))

    parser = cli.Parser(params=('--destination-address', '--backup-address',
                                '--workers', '--backwards', '--hot-run'),
                        help='This is a parquet file column data type migrator. '
                             'It enables migration of column data types in data lake tables in '
                             'order to prevent HIVE_PARTITION_SCHEMA_MISMATCH errors.')

    subparsers = (
        cli.SubParser('table',
                      params=('--database', '--table', '--location', '--spec', '--workers', '--backwards',
                              '--hot-run', '--destination-address', '--backup-address', '--sensitive'),
                      help='Migrate columns for the specified glue table and recreate partitions',
                      func=run_glue_table_migration
                      ),
        cli.SubParser('s3loc',
                      params=('--s3-path', '--spec', '--workers', '--backwards',
                              '--hot-run', '--destination-address', '--backup-address', '--sensitive'),
                      help='Migrate columns in parquets in the specified S3 location tree',
                      func=run_s3_loc_migration
                      ),

    )

    return parser, subparsers, cli_params


def run_glue_table_migration(args):
    glue_based_migration(dest_base=args.destination_address, column_migs=parse_spec_string(args.spec),
                         backup_base=args.backup_address, db=args.database, table_name=args.table, db_loc=args.location,
                         hot_run=args.hot_run, workers=args.workers, backwards=args.backwards,
                         is_sensitive=args.sensitive)


def run_s3_loc_migration(args):
    s3_path_based_migration(dest_base=args.destination_address, backup_base=args.backup_address,
                            column_migs=parse_spec_string(args.spec), s3_path=args.s3_path,
                            hot_run=args.hot_run, workers=args.workers, backwards=args.backwards,
                            is_sensitive=args.sensitive)


def get_parser() -> argparse.ArgumentParser:
    return cli.create_arg_parser(*get_parser_params())


def main(args):
    parser = get_parser()
    parsed_args = parser.parse_args(args)
    if 'func' not in parsed_args:
        parser.print_help()
    else:
        parsed_args.func(parsed_args)


if __name__ == '__main__':
    main(sys.argv[1:])

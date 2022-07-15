import argparse
import logging
import sys
import time
from typing import List

from tools.partition_builder.partitioner import PartitionSpec, splitter_main
from tools.type_migrator.cli import strip_tokenizer

logger = logging.getLogger(__name__)

"""
This is a partition builder tool which creates new partitions on existing parquet files and uploads the files 
to a selected S3 destination with complete partition hive path. 
"""


def parse_spec_string(s: str) -> List[PartitionSpec]:
    list_of_specs = [i.split(',') for i in strip_tokenizer(s, ';')]
    ps = []

    for spec in list_of_specs:
        assert 3 <= len(spec) <= 4, f'Spec has an incorrect number of arguments - {len(spec)}'
        if len(spec) == 3:
            ss = [s.strip() for s in spec]
            ps.append(PartitionSpec(*ss))
        else:
            ss = [s.strip() for s in spec[:-1]]
            ss.append(True)
            ps.append(PartitionSpec(*ss))
    return ps


def path_trimmer(s3_addrs: str):
    return s3_addrs[:-1] if s3_addrs.endswith('/') else s3_addrs


def get_parser_args(args):
    parser = argparse.ArgumentParser(
        description='Partition constructor tool lets you change the scope of partitions in a given table by '
                    'replacing the existing partitions with new ones generated from spec inputs.')
    parser.add_argument('-p', '--s3-path', help='Parquet file location.', type=str, required=True),
    parser.add_argument('-s', '--spec', help='Spec parameters for each partition in the form of comma '
                                             'seperated args and semicolon seperated specs:'
                                             '\n"arg1, arg2, ...; arg1, arg2, ...;".',
                        type=str, required=True),
    parser.add_argument('-u', '--upload-to', help='Upload destination folder (local or cloud). Note: this is '
                                                  'the table path only without partitions.',
                        type=str, required=True),
    parser.add_argument('-S', '--sensitive', default=False, action='store_true',
                        help='Affected data is obfuscated.'),
    parser.add_argument('-w', '--workers', help='Number of workers to run in parallel', type=int, default=2)
    parser.add_argument('-fs', '--filesystem', help='Use a filesystem if you intent on uploading the files to'
                                                    ' a S3 bucket.', action='store_false', default=True)
    args = parser.parse_args(args)

    return args


def run_partition_construction(args):
    t = time.time()
    splitter_main(s3_loc=path_trimmer(args.s3_path), spec=parse_spec_string(args.spec),
                  upload_to=path_trimmer(args.upload_to), is_sensitive=args.sensitive, workers=args.workers,
                  use_fs=args.filesystem)
    finished_time = time.time() - t
    logger.info(f'Parquet transformation and upload complete in {finished_time:.1f} sec.')


def main(args):
    parsed_args = get_parser_args(args)
    run_partition_construction(parsed_args)


if __name__ == '__main__':
    main(sys.argv[1:])

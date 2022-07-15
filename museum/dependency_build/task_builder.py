import pandas as pd
from boto3 import Session
from itertools import repeat

from museum.dependency_build import dependency_search as ds
from tools.update_catalog.catalog_inspector import CatalogInspector


def df_cleaner(cols_dict: dict, table: str = None):
    df = pd.DataFrame.from_dict(cols_dict)
    if 'Comment' in df:
        clean_df = df.drop('Comment', axis=1).rename(columns={'Name': 'Column'})
    else:
        clean_df = df.rename(columns={'Name': 'Column'})
    if table:
        clean_df['Table'] = table

    return clean_df


def get_etl_schema_structure(schema: str):
    glue_session = Session()
    ci = CatalogInspector(cl=glue_session)

    db = ci.get_tables(schema)
    tables = {t['Name']: t['StorageDescriptor']['Columns'] for t in db}
    list_of_df = [df_cleaner(v, k) for k, v in tables.items()]

    if len(list_of_df) > 0:
        return pd.concat([t for t in list_of_df])


# split tasks into batches of ~10 etls
def batch_maker(job_sequence: dict):
    runs = {}

    for lev, b in job_sequence.items():
        # create batch sizing
        if len(b) > 10:
            batches, remainder = len(b) // 10, len(b) % 10
            batches_enum = list(repeat(10, batches))
            batches_enum.append(remainder)
        else:
            batches_enum = [len(b)]

        # generate batch
        for e, i in enumerate(batches_enum):
            batch = b[:i]
            b = b[i:]
            if b == '':
                break
            runs[lev.split('_')[-1] + str(e)] = batch

    return runs


def main():
    # working case for BI recalc
    dep_tool = ds.DependencySearch('bi_etl.athena')
    dep_tool.job_extractor()
    athena_jobs_clean = dep_tool.job_cleaner('etl')

    # build list of jobs to migrate
    etl_schema = get_etl_schema_structure('etl')
    etl_migs = etl_schema[etl_schema['Type'].str.contains('dec')]
    etl_migs = etl_migs.replace(to_replace='_snapshots$', value='', regex=True).drop_duplicates()
    etl_tables = etl_migs['Table'].drop_duplicates()
    etl_tables.to_csv(
        '/Users/jaka.vrhovnik/data_engineering/data_dump/migration_etl_tables.csv',
        index=False
    )

    dep_tool.job_list = list(etl_tables)
    col_mig_dependencies = dep_tool.object_dependency_builder(athena_jobs_clean)

    # Zero dep ETLs
    zero_dep_etls = [t.table_name for t in athena_jobs_clean if
                     t.dependencies == [] and t.table_name in dep_tool.job_list]
    zero_dep_etls.sort()

    # Find other task dependecy runs
    ds.circular_dependency_finder(dep_list=col_mig_dependencies)
    col_mig_runs, html = ds.etl_run_sequencer(
        dependencies=[d for d in col_mig_dependencies if d.table_name not in zero_dep_etls])

    with open('temp/etl_report.txt', 'w') as f:
        f.write(html)
        f.close()

    # Define migration batches
    bi_batch = batch_maker(col_mig_runs)

    # create a df and join info on columns and data types and ready for export
    batch_df = [df_cleaner(v, k) for k, v in bi_batch.items()]
    batch_df = pd.concat([t for t in batch_df]).rename(columns={0: "Table", "Table": "Level"})

    zero_dep_batch = pd.DataFrame(columns=['Table'], data=zero_dep_etls)
    zero_dep_batch['Level'] = '00'

    # tidy up the df
    batches_final = pd.concat([batch_df, zero_dep_batch])
    batches_final = batches_final.merge(etl_migs, on='Table')
    batches_final.to_csv(
        '/Users/_/data_engineering/data_dump/bi_mig_tasks.csv',
        index=False
    )


if __name__ == '__main__':
    main()

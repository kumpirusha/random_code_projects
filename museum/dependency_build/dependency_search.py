import logging
from typing import NamedTuple, List

from tools.profiler.common import extract_etl_jobs

logger = logging.getLogger(__name__)


class ETL(NamedTuple):
    table_name: str
    dependencies: list


class DependencyObj(NamedTuple):
    table_name: str
    upstream: list
    downstream: list


def circular_dependency_finder(dep_list: List[DependencyObj]):
    for i in dep_list:
        up = set(i.upstream)
        down = set(i.downstream)
        intersec = up.intersection(down)

        if intersec:
            logger.error(f'Circular dependency found in \'{i.table_name}\'.')
        else:
            logger.info(f'No circular dependencies found in \'{i.table_name}\'.')


class DependencySearch(object):
    def __init__(self, schema: str, job_list: list = None):
        self.schema = schema
        self.job_list = job_list
        self.jobs = None

    def job_extractor(self) -> None:
        self.jobs = extract_etl_jobs(self.schema)

    def job_cleaner(self, only_schema: str = None) -> List[ETL]:
        clean_jobs = []

        for i, j in self.jobs.items():
            if only_schema:
                job = j.dest_table.replace(only_schema + '.', '').replace('_snapshots', '')
                source = [t.replace(only_schema + '.', '').replace('_snapshots', '') for t in
                          j.source_tables if t.split('.')[0] == only_schema or len(t.split('.')) == 1]
                etl = ETL(job, source)
                clean_jobs.append(etl)
            else:
                job = j.dest_table.replace('_snapshots', '')
                source = [t.replace('_snapshots', '') for t in j.source_tables]
                etl = ETL(job, source)
                clean_jobs.append(etl)

        return clean_jobs

    @staticmethod
    def downstream_dependency_finder(job_schema: List[ETL], table: ETL):

        def finder_func(o: ETL, dep: ETL) -> None:
            dep_tables = [i.table_name for i in job_schema if
                          o.table_name in i.dependencies and i.table_name not in checked_tbls]
            checked_tbls.append(o.table_name)
            if dep_tables:
                dep.dependencies.extend(dep_tables)
                for t in dep_tables:
                    t_dep = [i.table_name for i in job_schema if
                             t in i.dependencies and i.table_name not in checked_tbls]
                    n_obj = ETL(t, t_dep)
                    logger.info(
                        f'Looking for downstream dependencies of table \'{n_obj.table_name}\' in {n_obj.dependencies}')
                    finder_func(n_obj, dep)
            else:
                logger.info(
                    f'No further sub-dependencies for \'{o.table_name}\' - {len(dep.dependencies)} table(s) '
                    f'found.')

        dep_container = ETL(table.table_name, [])
        checked_tbls = []
        try:
            finder_func(table, dep_container)
            dc_clean = ETL(dep_container.table_name, list(set(dep_container.dependencies)))
            return dc_clean
        except KeyError as e:
            logger.error(f'Error, the following record does not exist: {e}')

    @staticmethod
    def upstream_dependency_finder(job_schema: List[ETL], table: ETL):

        def finder_func(o: ETL, dep: ETL) -> None:
            checked_tbls.append(o.table_name)
            if o.dependencies:
                for t in o.dependencies:
                    t_dep = [i.dependencies for i in job_schema if
                             i.table_name == t and i.table_name not in checked_tbls]
                    if t_dep:
                        n_obj = ETL(t, t_dep[0])
                        dep.dependencies.append(n_obj.table_name)
                        dep.dependencies.extend(n_obj.dependencies)
                        logger.info(
                            f'Looking for upstream dependencies of table \'{n_obj.table_name}\' in {n_obj.dependencies}.')
                        finder_func(n_obj, dep)
                    else:
                        dep.dependencies.append(t)
                        logger.info(f'No dependencies found for \'{t}\'.')
            else:
                logger.warning(
                    f'No further dependencies for \'{o.table_name}\' - {len(o.dependencies)} tables found.')

        dep_container = ETL(table.table_name, [])
        checked_tbls = []
        try:
            finder_func(table, dep_container)
            dc_clean = ETL(dep_container.table_name, list(set(dep_container.dependencies)))

            return dc_clean
        except KeyError as e:
            logger.error(f'Error, the following record does not exist: {e}')

    def object_dependency_builder(self, job_schema: List[ETL]):
        if self.job_list:
            obj = [j for j in job_schema if j.table_name in self.job_list]
        else:
            obj = job_schema

        res = []

        for tb in obj:
            dd = self.downstream_dependency_finder(job_schema, tb)
            ud = self.upstream_dependency_finder(job_schema, tb)
            dep_obj = DependencyObj(table_name=dd.table_name,
                                    downstream=dd.dependencies,
                                    upstream=ud.dependencies)
            res.append(dep_obj)

        return res


def etl_run_sequencer(dependencies: List[DependencyObj]):
    seq_holder = {}

    # Build a dict of local table interconnections for tables whose dependencies have been checked in
    # 'dependencies' param
    relevant_tbl = [i.table_name for i in dependencies]
    seq_dep = []
    for do in dependencies:
        rel_up = []
        rel_down = []
        for t in relevant_tbl:
            if t in do.upstream:
                rel_up.append(t)
            if t in do.downstream:
                rel_down.append(t)
        seq_dep.append(DependencyObj(do.table_name, rel_up, rel_down))

    seq_holder['level_1'] = [i.table_name for i in seq_dep if not i.downstream]
    logger.info(f'{len(seq_holder["level_1"])} tables added to Level 1.')
    level = 2
    included = []

    def seq():
        nonlocal level

        deps = [i for i in seq_dep if i.downstream and i.table_name not in included]
        # print([i.table_name for i in deps])

        if deps:
            logger.info(f'Looking for dependency level {level} ETLs in '
                        f'{", ".join(i.table_name for i in deps[:3] if i.table_name not in included)}...')
            dep_up = set([j for i in deps for j in i.upstream])
            # Interdependencies for next iteration
            interdep = [i for i in deps if i.table_name in dep_up]
            logger.info(f'Found {len(interdep)} interdependent table(s).')
            seq_holder['level_' + str(level)] = [i.table_name for i in deps if i.table_name not in dep_up]
            included.extend([i.table_name for i in deps if i.table_name not in dep_up])
            logger.info(f'Added table(s) {", ".join(t.table_name for t in interdep)} to level {level} run.')
            level += 1
            seq()
        else:
            logger.warning(f'No further run levels found. Final depth is {level}.')

    seq()

    # generate a simple report
    rs = 'ETL RERUN REPORT'
    re = 'END OF REPORT'
    sep = '=' * 50
    nl = '\n'
    print(sep)
    print(f'{rs:^50}')
    print(sep)
    for i in list(seq_holder.keys()):
        print('L{n} ETL run:\n\t- {l}'.format(n=i.split('_')[-1],
                                              l='\n\t- '.join([t for t in seq_holder[i]])))
        print('-' * 50)
    print(f'{re:^50}')
    print(sep)

    # generate a txt report
    body = []
    for i in list(seq_holder.keys()):
        body.append('L{n} ETL run:\n\t- {l}'.format(n=i.split('_')[-1],
                                                    l='\n\t- '.join([t for t in seq_holder[i]])))
        body.append('-' * 50)
    txt_string = f'''{sep}{rs:^50}
    {sep}
    {nl.join([i for i in body[:-1]])}
    {sep}
    {re:^50}
    {sep}'''

    return seq_holder, txt_string

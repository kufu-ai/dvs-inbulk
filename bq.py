from string import Template
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import time
import pandas as pd

class DryRunJob:
    def to_dataframe(self):
        return pd.DataFrame(columns=['var'], data=['<<DUMMY_VAR>>'])

    def done(self):
        return True

    def error_result(self):
        return None

class BigQuery:
    WAIT_INTERVAL = 5

    def __init__(self, conf, dryrun=False):
        self.conf = conf
        self.dryrun = dryrun

    @property
    def client(self):
        if not hasattr(self, '__client'):
            self.__client = bigquery.Client.from_service_account_json(self.conf['init']['credential-file'])
        return self.__client

    def query(self, q, job_config=None):
        if self.dryrun:
            print('==================\n' + q + '\n==================\n')
            return DryRunJob()
        return self.client.query(q, job_config)

    def fetch_var_query(self, database, table, field, mode):
        if mode == 'meta':
            query = '''
                select
                    timestamp_millis(${field}) var
                from `${database}.__TABLES__`
                where table_id = '${table}'
                limit 1
            '''
            return Template(query).substitute(database=database, table=table, field=field)
        elif mode in ('max', 'min'):
            query = '''
                select
                    ${mode}(${field}) var
                from ${database}.${table}
                limit 1
            '''
            return Template(query).substitute(
                    database=database,
                    table=table,
                    field=field,
                    mode=mode)

    def fetch_var(self, name, database, table, field, mode, default=None):
        query = self.fetch_var_query(database, table, field, mode)
        try:
            job = self.query(query)
            df = job.to_dataframe()
            return df['var'][0]
        except (KeyError, NotFound):
            if default == None:
                raise AttributeError(f"Cannot fetch var: {name}")
            return default
        except:
            raise AttributeError(f"Cannot fetch var: {name}")

    def fetched_vars(self):
        if hasattr(self, '__fetched_vars'):
            return self.__fetched_vars

        if not self.conf['in']['vars']:
            return {}

        self.__fetched_vars = {}
        for var in self.conf['in']['vars']:
            self.__fetched_vars[var['name']] = self.fetch_var(**var)
        return self.__fetched_vars

    @property
    def template_query(self):
        return Template(self.conf['in']['query'])

    def formatted_query(self):
        return self.template_query.substitute(**self.fetched_vars())

    def table_id(self, project, database, table):
        return '.'.join([project, database, table])

    def write_disposition(self, mode):
        if mode not in ('append', 'replace', 'merge'):
            raise AttributeError(f"out.mode is invalid value: mode should be in ('append', 'replace', 'merge'), but got {mode}")

        if mode == 'append':
            return bigquery.job.WriteDisposition.WRITE_APPEND
        return bigquery.job.WriteDisposition.WRITE_TRUNCATE

    def job_config(self):
        conf = self.conf['out']
        return bigquery.job.QueryJobConfig(
                destination=self.table_id(conf['project'], conf['database'], conf['table']),
                write_disposition=self.write_disposition(conf['mode']),
                create_disposition=bigquery.job.CreateDisposition.CREATE_IF_NEEDED,
                )

    def is_exists(self, table):
        try:
            self.client.get_table(table)
            return True
        except NotFound:
            return False

    def decorated_query(self, query):
        conf = self.conf['out']
        table_id = self.table_id(conf['project'], conf['database'], conf['table'])
        if conf['mode'] != 'merge' or not self.is_exists(table_id):
            return query

        tmpl = '''
        with
            unify as (
                select
                    *,
                    2 _order,
                from ${table}
                union all
                (
                    select
                        *,
                        1 _order,
                    from (
                    ${query}
                    )
                )
            ),
            ranks as (
                select
                    * except(_order),
                    row_number() over(partition by ${keys} order by ${order} ) as last_record,
                from unify
            )
            select
                * except(last_record)
            from ranks
            where
                last_record = 1
        '''
        variables = {
                'table': table_id,
                'query': query,
                'keys': ', '.join(conf['merge']['keys']),
                'order': self.order_string(conf['merge']),
        }
        return Template(tmpl).substitute(**variables)

    def order_string(self, conf):
        if 'order' not in conf.keys():
            return '_order'

        def fmt(column, desc=False):
            mode = 'desc' if desc else 'asc'
            return f"{column} {mode}"
        return ', '.join([fmt(order['column'], order['desc']) for order in conf['order']])

    def start_job(self):
        q = self.formatted_query()
        q = self.decorated_query(q)
        job = self.query(q, job_config=self.job_config())
        return job

    def wait_job(self):
        job = self.start_job()

        if self.dryrun:
            return
        while not job.done():
            time.sleep(self.WAIT_INTERVAL)

        if job.error_result != None:
            raise RuntimeError(job.error_result)


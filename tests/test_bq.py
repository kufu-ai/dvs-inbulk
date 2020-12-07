from bq import BigQuery
from jsonschema.exceptions import ValidationError
import copy
import pandas as pd
import pytest

class TestBigQuery:
    conf = {
            'init': {
                'service': 'bigquery',
                'credential-file': 'dummy',
             },
            'in': {
                'query': 'select * from Test.users where id > ${last_id} and _partitiontime > ${last_updated}',
                'vars': [
                    {
                        'name': 'last_id',
                        'database': 'Test',
                        'table': 'users',
                        'field': 'id',
                        'mode': 'max',
                    },
                    {
                        'name': 'last_updated',
                        'database': 'Test',
                        'table': 'users',
                        'field': 'last_modified_time',
                        'mode': 'meta',
                    },
                ],
            },
            'out': {
                'project': 'project',
                'database': 'database',
                'table': 'table',
                'mode': 'replace',
            },
    }

    def test_fetch_var_query__meta(self):
        bq = BigQuery(self.conf, dryrun=True)
        query = bq.fetch_var_query('database', 'table', 'last_modified_time', 'meta')
        assert query == '''
                select
                    timestamp_millis(last_modified_time) var
                from `database.__TABLES__`
                where table_id = 'table'
                limit 1
'''

    def test_fetch_var_query__max(self):
        bq = BigQuery(self.conf, dryrun=True)
        query = bq.fetch_var_query('database', 'table', 'id', 'max')
        assert query == '''
                select
                    max(id) var
                from database.table
                limit 1
'''

    def test_fetch_var_query__invalid_mode(self):
        bq = BigQuery(self.conf, dryrun=True)
        query = bq.fetch_var_query('database', 'table', 'id', ';drop table users;')
        assert query == None

    def test_fetch_vars__exists(self, mocker):
        bq = BigQuery(self.conf, dryrun=True)
        mock = mocker.Mock(to_dataframe=lambda : pd.DataFrame(columns=['var'], data=['testdata']))
        mocker.patch.object(BigQuery, 'query', lambda *args, **kwargs: mock)
        assert bq.fetch_var('max_id', 'database', 'table', 'id', 'max') == 'testdata'

    def test_fetch_vars__not_exists(self, mocker):
        with pytest.raises(AttributeError):
            bq = BigQuery(self.conf, dryrun=True)
            mock = mocker.Mock(to_dataframe=lambda : pd.DataFrame())
            mocker.patch.object(BigQuery, 'query', lambda *args, **kwargs: mock)
            bq.fetch_var('max_id', 'database', 'table', 'id', 'max') == 'testdata'

    def test_fetch_vars__with_default(self, mocker):
        bq = BigQuery(self.conf, dryrun=True)
        mock = mocker.Mock(to_dataframe=lambda : pd.DataFrame())
        mocker.patch.object(BigQuery, 'query', lambda *args, **kwargs: mock)
        assert bq.fetch_var('max_id', 'database', 'table', 'id', 'max', 'default') == 'default'

    def test_fetched_vars__with_conf(self, mocker):
        bq = BigQuery(self.conf, dryrun=True)
        mock = mocker.Mock(to_dataframe=lambda : pd.DataFrame(columns=['var'], data=['testdata']))
        mocker.patch.object(BigQuery, 'query', lambda *args, **kwargs: mock)
        assert bq.fetched_vars() == { 'last_id': 'testdata', 'last_updated': 'testdata' }

    def test_fetched_vars__without_conf(self, mocker):
        conf = copy.deepcopy(self.conf)
        del conf['in']['vars']
        bq = BigQuery(conf, dryrun=True)
        mock = mocker.Mock(to_dataframe=lambda : pd.DataFrame(columns=['var'], data=['testdata']))
        mocker.patch.object(BigQuery, 'query', lambda *args, **kwargs: mock)
        assert bq.fetched_vars() == {}

    def test_formatted_query(self, mocker):
        bq = BigQuery(self.conf, dryrun=True)
        mock = mocker.Mock(to_dataframe=lambda : pd.DataFrame(columns=['var'], data=['testdata']))
        mocker.patch.object(BigQuery, 'query', lambda *args, **kwargs: mock)
        assert bq.formatted_query() == 'select * from Test.users where id > testdata and _partitiontime > testdata'

    def test_order_string(self, mocker):
        bq = BigQuery(self.conf, dryrun=True)
        conf = {
                    'order': [
                        { 'column': 'col1', 'desc': True },
                        { 'column': 'col2' },
                    ],
                }
        assert bq.order_string(conf) == 'col1 desc, col2 asc'
        assert bq.order_string({}) == '_order'

    def test_decorated_query(self, mocker):
        conf = copy.deepcopy(self.conf)
        conf['out']['mode'] = 'merge'
        conf['out']['merge'] = {
                    'order': [
                        { 'column': 'col1', 'desc': True },
                        { 'column': 'col2' },
                    ],
                    'keys': ['id'],
                }

        bq = BigQuery(conf, dryrun=True)
        mock = mocker.Mock(to_dataframe=lambda : pd.DataFrame(columns=['var'], data=['testdata']))
        mocker.patch.object(BigQuery, 'query', lambda *args, **kwargs: mock)
        mocker.patch.object(BigQuery, 'is_exists', lambda *args, **kwargs: True)
        assert bq.decorated_query('query') == '''
        with
            unify as (
                select
                    *,
                    2 _order,
                from project.database.table
                union all
                (
                    select
                        *,
                        1 _order,
                    from (
                    query
                    )
                )
            ),
            ranks as (
                select
                    * except(_order),
                    row_number() over(partition by id order by col1 desc, col2 asc ) as last_record,
                from unify
            )
            select
                * except(last_record)
            from ranks
            where
                last_record = 1
'''

    def test_decorated_query__not_merge(self, mocker):
        conf = copy.deepcopy(self.conf)
        bq = BigQuery(conf, dryrun=True)
        mock = mocker.Mock(to_dataframe=lambda : pd.DataFrame(columns=['var'], data=['testdata']))
        mocker.patch.object(BigQuery, 'query', lambda *args, **kwargs: mock)
        mocker.patch.object(BigQuery, 'is_exists', lambda *args, **kwargs: True)
        assert bq.decorated_query('query') == 'query'

    def test_decorated_query__table_not_exists(self, mocker):
        conf = copy.deepcopy(self.conf)
        conf['out']['mode'] = 'merge'
        conf['out']['merge'] = {
                    'order': [
                        { 'column': 'col1', 'desc': True },
                        { 'column': 'col2' },
                    ],
                    'keys': ['id'],
                }

        bq = BigQuery(conf, dryrun=True)
        mock = mocker.Mock(to_dataframe=lambda : pd.DataFrame(columns=['var'], data=['testdata']))
        mocker.patch.object(BigQuery, 'query', lambda *args, **kwargs: mock)
        mocker.patch.object(BigQuery, 'is_exists', lambda *args, **kwargs: False)
        assert bq.decorated_query('query') == 'query'

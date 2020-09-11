from datetime import date
from typing import List

TRANSACTION_COLUMNS = {'plaid_transaction_id': 't.plaid_transaction_id',
                       'plaid_account_id': 't.plaid_account_id',
                       'plaid_category_id': 't.plaid_category_id',
                       'category': 't.category',
                       'name': 't.name',
                       'type': 't.type',
                       'amount': 't.amount::float',
                       'iso_currency_code': 't.iso_currency_code',
                       'date': 't.date',
                       'pending': 't.pending',
                       'pending_transaction_id': 't.pending_transaction_id',
                       'deleted': 't.deleted',
                       'account_name': 'a.name',
                       'account_type': 'a.type',
                       'account_subtype': 'a.subtype',
                       'plaid_item_id': 'a.plaid_item_id'}
import pandas as pd


class TransactionLoader:
    def __init__(self, connection, fields: List = TRANSACTION_COLUMNS.keys()) -> None:
        self.fields = fields
        self.connection = connection

    def __build_query(self, items: List, from_date: date, to_date: date, deleted=False, account_list=None):
        column_names = [TRANSACTION_COLUMNS[x] for x in self.fields]
        columns = ','.join(column_names)
        sql = f'SELECT {columns} from TRANSACTION t, ACCOUNT a where a.plaid_item_id IN %s and a.plaid_account_id = t.plaid_account_id '
        params = (tuple(items),)
        if from_date is not None:
            sql = sql + ' and t.date >= %s '
            params = params + (from_date,)
        if to_date is not None:
            sql = sql + ' and t.date <= %s '
            params = params + (to_date,)
        if account_list:
            sql = sql + ' and t.plaid_account_id in %s '
            params = params + (tuple(account_list),)
        sql = sql + f'and deleted = {str(deleted).lower()}'
        sql = sql + ' order by date'
        return sql, params

    def load_tuples(self, items: list, from_date: date = None, to_date: date = None, deleted=False, account_list=None):
        query, params = self.__build_query(items, from_date, to_date, deleted, account_list)
        cur = self.connection.cursor()
        cur.execute(query, params)
        tuples = cur.fetchall()
        cur.close()
        return tuples

    def load_df(self, items: List, from_date: date = None, to_date: date = None, deleted=False, account_list=None):
        tuples = self.load_tuples(items, from_date, to_date, deleted, account_list)
        df = pd.DataFrame(tuples, columns=self.fields)
        if 'date' in df.columns:
            df = df.astype(dtype={'date': 'datetime64[ns]'})
        return df

    def load_all_as_df(self):
        column_names = [TRANSACTION_COLUMNS[x] for x in self.fields]
        columns = ','.join(column_names)
        query = f'SELECT {columns} from TRANSACTION t, Account a WHERE a.plaid_account_id = t.plaid_account_id '
        cur = self.connection.cursor()
        cur.execute(query)
        tuples = cur.fetchall()
        cur.close()
        df = pd.DataFrame(tuples, columns=self.fields)
        if 'date' in df.columns:
            df = df.astype(dtype={'date': 'datetime64[ns]'})
        return df

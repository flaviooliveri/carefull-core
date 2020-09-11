from __future__ import annotations

import os
from datetime import date
from enum import Enum, auto
from typing import List

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

COLUMN_NAMES = (
'unique_mem_id', 'unique_account_id', 'unique_transaction_id', 'amount', 'currency', 'description', 'transaction_date',
'post_date', 'transaction_base_type', 'transaction_category_name', 'primary_merchant_name', 'secondary_merchant_name',
'city', 'state', 'zip_code', 'transaction_origin', 'factual_category', 'factual_id', 'file_created_date',
'optimized_transaction_date', 'yodlee_transaction_status', 'mcc_raw', 'mcc_inferred', 'swipe_date',
'panel_file_created_date', 'update_type', 'is_outlier', 'change_source', 'account_type', 'account_source_type',
'account_score', 'user_score', 'lag', 'is_duplicate', 'user_type', 'file')
CATEGORIES = ('unique_mem_id', 'unique_account_id', 'currency', 'transaction_base_type', 'transaction_category_name',
              'primary_merchant_name', 'secondary_merchant_name', 'city', 'state', 'zip_code', 'transaction_origin',
              'factual_category', 'yodlee_transaction_status', 'mcc_raw', 'mcc_inferred', 'update_type', 'is_outlier',
              'change_source', 'account_type', 'account_source_type', 'is_duplicate', 'user_type', 'file')
DATES = ('transaction_date', 'post_date', 'file_created_date', 'optimized_transaction_date', 'swipe_date',
         'panel_file_created_date')


class DatasourceType(Enum):
    P = auto()
    Y = auto()


def get_engine():
    db_string = 'postgresql+psycopg2://{user}:{password}@{account}/{dbname}'.format(
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        account=os.environ.get("DB_TRANSACTIONS_HOST", "localhost"),
        dbname=os.environ.get("DB_TRANSACTIONS_NAME")
    )
    return create_engine(db_string)


class TransactionLoader:
    def __init__(self, engine=None, fields=COLUMN_NAMES) -> None:
        self.fields = fields
        self.engine = get_engine() if engine is None else engine
        self.query = f"select {','.join(fields)} from transactions where 1=1"
        self.params = {}

    def from_date(self, year, month, day) -> TransactionLoader:
        a_date = date(year, month, day)
        self.query = self.query + " and optimized_transaction_date >= %(from_date)s"
        self.params['from_date'] = a_date
        return self

    def to_date(self, year, month, day) -> TransactionLoader:
        a_date = date(year, month, day)
        self.query = self.query + " and optimized_transaction_date <= %(to_date)s"
        self.params['to_date'] = a_date
        return self

    def seniors(self):
        self.query = self.query + " and user_type = 'S'"
        return self

    def random(self):
        self.query = self.query + " and user_type = 'R'"
        return self

    def bank(self):
        self.query = self.query + " and file = 'B'"
        return self

    def credit_card(self):
        self.query = self.query + " and file = 'C'"
        return self

    def for_users(self, mem_id_list):
        self.query = self.query + " and unique_mem_id in %(mem_id_list)s"
        self.params['mem_id_list'] = tuple(mem_id_list)
        return self

    def for_accounts(self, account_id_list):
        self.query = self.query + " and unique_account_id in %(account_id_list)s"
        self.params['account_id_list'] = tuple(account_id_list)
        return self

    def read(self):
        dates = [x for x in self.fields if x in DATES]
        dframe = pd.read_sql_query(self.query, self.engine, parse_dates=dates, params=self.params)
        for category in [x for x in dframe.columns if x in CATEGORIES]:
            dframe[category] = dframe[category].astype('category')
        return dframe


def as_plaid_df(df):
    rename_map = {'unique_mem_id': 'plaid_item_id',
                  'description': 'name',
                  'unique_transaction_id': 'plaid_transaction_id',
                  'transaction_category_name': 'plaid_category_id',
                  'optimized_transaction_date': 'date',
                  'unique_account_id': 'plaid_account_id'}
    for name in df.columns:
        if name.lower() == 'transaction_base_type':
            if 'amount' in [x.lower() for x in df.columns]:
                df.loc[df[name] == 'credit', 'amount'] = df.loc[df[name] == 'credit', 'amount'] * -1
    return df.rename(columns=rename_map)



def connect():
    return psycopg2.connect(**{
        'host': os.environ.get("DB_HOST"),
        'database': os.environ.get('DB_NAME'),
        'user': os.environ.get("DB_USER"),
        'password': os.environ.get("DB_PASSWORD")
    })


def connect_transactions_db():
    return psycopg2.connect(**{
        'host': os.environ.get("DB_TRANSACTIONS_HOST", 'localhost'),
        'database': os.environ.get('DB_TRANSACTIONS_NAME'),
        'user': os.environ.get("DB_USER"),
        'password': os.environ.get("DB_PASSWORD")
    })

PLAID_TRANSACTION_COLUMNS = {'plaid_transaction_id': 't.plaid_transaction_id',
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


class PlaidTransactionLoader:
    def __init__(self, connection=connect(), fields: List = PLAID_TRANSACTION_COLUMNS.keys()) -> None:
        self.fields = fields
        self.connection = connection

    def __build_query(self, items: List, from_date: date, to_date: date, deleted=False, account_list=None):
        column_names = [PLAID_TRANSACTION_COLUMNS[x] for x in self.fields]
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
        column_names = [PLAID_TRANSACTION_COLUMNS[x] for x in self.fields]
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

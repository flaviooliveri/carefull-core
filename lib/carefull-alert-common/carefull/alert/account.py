from dataclasses import dataclass
from typing import List
from datetime import date


@dataclass
class Account:
    account_id: str
    item_id: str
    name: str
    official_name: str
    type: str
    subtype: str
    mask: str
    institution_name: str


class AccountRepo:

    SELECT_SQL = '''
        SELECT a.plaid_account_id, a.plaid_item_id, a.name, a.official_name, a.type, a.subtype, a.mask, ins.name 
        FROM account a, item i, institution ins
        WHERE a.plaid_item_id = %s
        and a.plaid_item_id = i.plaid_item_id
        and i.plaid_institution_id = ins.plaid_institution_id
        
    '''

    SELECT_MIN_DATE = '''
        SELECT min(date)
        FROM transaction
        where plaid_account_id = %s
    '''

    def __init__(self, conn) -> None:
        self.conn = conn

    def find_by_item_id(self, item_id: str) -> List[Account]:
        cursor = self.conn.cursor()
        cursor.execute(self.SELECT_SQL, (item_id,))
        result = [Account(*x) for x in cursor.fetchall()]
        cursor.close()
        return result

    def find_min_trx_date(self, account_id: str) -> date:
        cursor = self.conn.cursor()
        cursor.execute(self.SELECT_MIN_DATE, (account_id,))
        result = cursor.fetchone()[0]
        cursor.close()
        return result

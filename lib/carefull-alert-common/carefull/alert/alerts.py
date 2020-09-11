from dataclasses import dataclass
from datetime import datetime, date
from enum import Enum, auto
from typing import Optional


class AlertType(Enum):
    LATE_BILL = auto()
    UNKNOWN_VENDOR = auto()
    SPENDING = auto()
    UNUSUAL_TRANSFER = auto()
    HIGH_AMOUNT = auto()


class AlertStatus(Enum):
    NEW = auto()
    NOTIFIED = auto()
    ACK = auto()
    ARCHIVED = auto()


@dataclass
class Alert:
    id: Optional[int]
    item_id: str
    transaction_id: str
    type: str
    data: str
    status: str
    created_at: datetime = datetime.now()


class AlertRepository:

    def __init__(self, con) -> None:
        super().__init__()
        self.con = con

    def insert_alert(self, alert: Alert) -> Alert:
        sql = '''
        INSERT INTO ALERT (PLAID_ITEM_ID, PLAID_TRANSACTION_ID, TYPE, DATA, STATUS, CREATED_AT) 
        values (%s, %s, %s, %s, %s, %s) RETURNING id
        '''
        cur = self.con.cursor()
        cur.execute(sql, (alert.item_id, alert.transaction_id, alert.type, alert.data, alert.status, alert.created_at))
        alert_id = cur.fetchone()[0]
        alert.id = alert_id
        self.con.commit()
        cur.close()
        return alert

    def alert_exists(self, transaction_id: str, alert_type: str) -> bool:
        sql = '''
            SELECT plaid_transaction_id
            FROM ALERT
            WHERE plaid_transaction_id = %s
            AND type = %s
        '''
        cur = self.con.cursor()
        cur.execute(sql, (transaction_id, alert_type))
        exists = cur.fetchone() is not None
        cur.close()
        return exists

    def alert_exists_by_date(self, item_id: str, a_date: date, alert_type: str) -> bool:
        sql = '''
            SELECT plaid_transaction_id
            FROM ALERT
            WHERE created_at::date = %s
            AND type = %s
            AND plaid_item_id = %s
        '''
        cur = self.con.cursor()
        cur.execute(sql, (a_date, alert_type, item_id))
        exists = cur.fetchone() is not None
        cur.close()
        return exists

    def find_alert_by_id(self, alert_id: int) -> Alert:
        sql = '''
            SELECT  ID, PLAID_ITEM_ID, PLAID_TRANSACTION_ID, TYPE, DATA, STATUS, CREATED_AT
            FROM ALERT
            where ID = %s
        '''
        cur = self.con.cursor()
        cur.execute(sql, (alert_id,))
        data = cur.fetchone()
        cur.close()
        return Alert(*data) if data is not None else None

    def update_alert_status(self, alert_id: int, status: AlertStatus) -> None:
        update_sql = '''
            UPDATE ALERT
            set status = %s
            where id = %s
        '''
        cur = self.con.cursor()
        cur.execute(update_sql, (status.name, alert_id))
        self.con.commit()
        cur.close()

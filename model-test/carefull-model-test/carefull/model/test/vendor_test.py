import logging
from enum import Enum
from typing import Dict

from carefull.model.data.db_loader import connect, connect_transactions_db
from carefull.model.import_name_vendor.import_name_vendor import is_eligible
from carefull.model.test.tag import TagRepo
from model.common.text import normalize_transaction_name
from model.vendor.vendor import load_vendor_model, NameVendorRepo, VendorModel

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()


VENDOR_CACHE = {}

EXCLUDE_CATEGORIES = ('21008000', '21009000', '15001000', '21012001', '16001000', '21007000', '21012002', '18020007', '21001000', '10000000', '10002000', '21011000')

def get_vendors():
    if 'vendors' not in VENDOR_CACHE:
        logger.info("Loading vendors")
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("select id, name from vendor")
                VENDOR_CACHE['vendors'] = {row[0]: row[1] for row in cur.fetchall()}
        logger.info(f"{len(VENDOR_CACHE['vendors'])} vendors loaded")
    return VENDOR_CACHE['vendors']


class VendorTestElementResult(Enum):
    TP = 'True Positive',
    FP = 'False Positive',
    TN = 'True Negative',
    FN = 'False Negative',


class ResultCounter:

    def __init__(self) -> None:
        self.items: Dict[VendorTestElementResult, int] = {
            VendorTestElementResult.TP: 0,
            VendorTestElementResult.FP: 0,
            VendorTestElementResult.FN: 0,
            VendorTestElementResult.TN: 0
        }

    def add(self, a_value: VendorTestElementResult):
        self.items[a_value] += 1

    @property
    def total_items(self):
        return sum([v for v in self.items.values()])

    def print(self, logger):
        total = self.total_items
        logger.info(f'Items: {total}')
        for k, v in sorted(self.items.items(), key=lambda item: item[1], reverse=True):
            logger.info(f"{k.value[0]}: {v/total * 100:.2f}% - {v}/{total}")


class VendorTestElement:

    def __init__(self) -> None:
        self.__from_model = None
        self.__from_tag = None

    def model(self, value):
        if value and value == value:
            self.__from_model = value
        return self

    def tag(self, value):
        if value and value == value:
            self.__from_tag = value
        return self

    def compute(self) -> VendorTestElementResult:
        if self.__from_model is None:
            if self.__from_tag is None:
                return VendorTestElementResult.TN
            else:
                return VendorTestElementResult.FN
        else:
            if normalize_transaction_name(self.__from_model) == normalize_transaction_name(self.__from_tag):
                return VendorTestElementResult.TP
            else:
                return VendorTestElementResult.FP


class TransactionYNameRepo:

    SQL = '''
        SELECT t.description
        from transactions t
        where t.unique_transaction_id = %s
    '''

    def __init__(self, con) -> None:
        super().__init__()
        self.con = con

    def find_name(self, t_id):
        cursor = self.con.cursor()
        cursor.execute(self.SQL, (t_id,))
        name = cursor.fetchone()[0]
        cursor.close()
        return name


class TransactionPNameRepo:

    SQL = '''
        SELECT t.name, t.plaid_category_id
        from transaction t
        where t.plaid_transaction_id = %s
    '''

    def __init__(self, con) -> None:
        super().__init__()
        self.con = con

    def find_name(self, t_id):
        cursor = self.con.cursor()
        cursor.execute(self.SQL, (t_id,))
        name_category = cursor.fetchone()
        cursor.close()
        return name_category


def test_vendors_from_tag(from_date=None, in_model=False):
    connection_transactions = connect_transactions_db()
    connection_carefull = connect()
    tag_repo = TagRepo(connection_transactions)
    name_repo = TransactionPNameRepo(connection_carefull)
    tags = tag_repo.find_all_tags_by_name('vendor', from_date, in_model)
    model_info = load_vendor_model(True)
    repo = NameVendorRepo(connection=connection_carefull)
    vendor_model = VendorModel(model_info, repo)
    results = ResultCounter()
    i = 1
    for tag in tags:
        description, plaid_category_id = name_repo.find_name(tag.transaction_id)
        if plaid_category_id not in EXCLUDE_CATEGORIES:
            from_model = vendor_model.extract_vendor(description)
            vendor = get_vendors()[from_model] if from_model else from_model
            result = VendorTestElement().model(vendor).tag(tag.value).compute()
            if result in [VendorTestElementResult.FN, VendorTestElementResult.FP]:
                logger.info(f"{result.name} - {vendor} - {tag.value} ({description})")
            results.add(result)
            if i % 100 == 0:
                results.print(logger)
            i += 1
    logger.info("--- TOTAL ---")
    results.print(logger)
    connection_carefull.close()
    connection_transactions.close()


def description_iterator(bigger_than):
    with connect_transactions_db() as connection:
        with connection.cursor(name='description_iterator') as cursor:
            cursor.itersize = 10000
            sql = '''
                SELECT description, primary_merchant_name 
                FROM transactions t, users u
                where t.unique_mem_id = u.unique_mem_id
                and u.user_group >= %s
            '''
            cursor.execute(sql, (bigger_than, ))
            for row in cursor:
                yield row


def test_vendors_from_y(user_group_bigger_than='89'):
    connection_carefull = connect()
    model_info = load_vendor_model(True)
    repo = NameVendorRepo(connection=connection_carefull)
    vendor_model = VendorModel(model_info, repo)
    results = ResultCounter()
    i = 1
    for row in description_iterator(user_group_bigger_than):
        description = row[0]
        cleaned_description = normalize_transaction_name(description)
        if is_eligible(cleaned_description):
            from_model = vendor_model.extract_vendor(description)
            vendor = get_vendors()[from_model] if from_model else from_model
            result = VendorTestElement().model(vendor).tag(row[1]).compute()
            if result in [VendorTestElementResult.FN, VendorTestElementResult.FP]:
                logger.info(f"{result.name} - {vendor} - {row[1]} ({description}) '{normalize_transaction_name(description)}'")
            results.add(result)
            if i % 100 == 0:
                results.print(logger)
            i += 1
    logger.info("--- TOTAL ---")
    results.print(logger)
    connection_carefull.close()


if __name__ == '__main__':
    test_vendors_from_tag(in_model=False)

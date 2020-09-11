import csv
import os
import logging

from carefull.model.data.db_loader import connect
from carefull.model.import_name_vendor.vendor_common import VendorRepo, NormalizedTxNameVendorRepo
from model.common.text import normalize_transaction_name

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

NOT_IN = {'funds transfer', 'check', 'transfer from', 'transfer to', 'pension', 'deposit', 'loans', 'payment',
          'dividend', 'transfer to savings', 'dirdep', 'dir dep', 'trnsfr', 'fee service charge', 'pr payment',
          'savings', 'transfer', 'direct dep', 'reg salary', 'salary', 'teller deposit', 'autopay payment',
          'ach electronic debit paypal inst xfer', 'recurring transfer', 'foreign fee', 'online transfer', 'online pmt',
          'online banking transfer', 'epayment', 'online transfer to', 'online pmt to', 'online banking transfer to',
          'dir dep dir dep', 'payment check 2789 fpl payment ctr bill pymt', 'recurring transfer to', 'payment da',
          'interest payment', 'ach electronic debit check pymt', 'ach electronic debit on', 'access check check check',
          'incoming wire transfer', 'online transfer to savings', 'ach electronic debit bp check pymt',
          'transfer to savings savings', 'transfer from savings savings', 'payment mobl', 'withdrawal', 'payment cbol',
          'ach electronic credit cash reward', 'financial bill payment', 'bill payment'}
STARTS_WITH = ('home mtg', 'mortgage', 'foreign transaction fee', 'mtg pmt', 'nfcu ach', 'ach electronic debit payment',
               'benefit payment', 'online transfer from checkingfeb 14 06 49 tfr in xxxxxxxonline reference 3602onli')
CONTAINS = ('mortgage', 'cash withdrawal', 'payroll', 'paypal', 'atm withdrawal', 'withdrawal atm')


def is_eligible(description: str):
    if not description:
        return False
    if description in NOT_IN:
        return False
    for x in STARTS_WITH:
        if description.startswith(x):
            return False
    for x in CONTAINS:
        if description.find(x) != -1:
            return False
    return True


def import_name_vendor_csv(path, apply_extra_filter=False):
    logger.info("Starting...")
    connection = connect()
    cursor = connection.cursor()
    vendor_repo = VendorRepo()
    normalized_tx_name_repo = NormalizedTxNameVendorRepo()
    with open(path, newline='') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
        i = 0
        for row in csv_reader:
            i += 1
            vendor_name = row[1]
            if vendor_name:
                normalized_name = normalize_transaction_name(row[0])
                if not apply_extra_filter or is_eligible(normalized_name):
                    vendor_id = vendor_repo.find_or_create(vendor_name, cursor)
                    normalized_tx_name_repo.delete_by_name(normalized_name, cursor)
                    normalized_tx_name_repo.create(normalized_name, vendor_id, cursor)
            if i % 1000 == 0:
                logger.info(f"{i} rows processed - committing...")
                connection.commit()
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("End")


if __name__ == '__main__':
    path = os.path.join(os.getcwd(), "plaid_name_vendor_20200713.csv")
    import_name_vendor_csv(path)

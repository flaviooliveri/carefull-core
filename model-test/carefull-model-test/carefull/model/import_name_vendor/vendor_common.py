from dataclasses import dataclass


@dataclass
class Vendor:
    id: str
    name: str


class NormalizedTxNameVendorRepo:

    DELETE_BY_NAME = "delete from normalize_tx_name_vendor where normalize_name = %s"
    INSERT = "insert into normalize_tx_name_vendor (normalize_name, vendor_id) values (%s, %s)"

    def delete_by_name(self, name, cursor):
        cursor.execute(self.DELETE_BY_NAME, (name,))

    def create(self, name, vendor_id, cursor):
        cursor.execute(self.INSERT, (name,vendor_id))


class VendorRepo:

    BY_NAME = "select id, name from vendor where name = %s"

    INSERT = "insert into vendor (name) values (%s) returning id"

    def find_by_name(self, name, cursor) -> Vendor:
        cursor.execute(self.BY_NAME, (name,))
        vendor_tuple = cursor.fetchone()
        return Vendor(*vendor_tuple) if vendor_tuple else None

    def create(self, name, cursor) -> int:
        cursor.execute(self.INSERT, (name, ))
        return cursor.fetchone()

    def find_or_create(self, name, cursor) -> int:
        vendor = self.find_by_name(name, cursor)
        return vendor.id if vendor else self.create(name, cursor)

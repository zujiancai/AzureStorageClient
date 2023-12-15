from batch_job.table_store import TableStore, UpdateMode
from typing_extensions import TypedDict
import unittest


class EntityType(TypedDict):
    PartitionKey: str
    RowKey: str
    value: str
    status: str


@unittest.skip("Skip Azure Storage Emulator dependent tests")
class TestTableStore(unittest.TestCase):
    def setUp(self):
        self.conn_str = "UseDevelopmentStorage=true" # Needs to start Azure Storage Emulator for this to work
        self.table_name = "TestTable"
        self.table_store = TableStore(self.conn_str, self.table_name)
        table = self.table_store.create_if_not_exist()
        self.assertIsNotNone(table)
        self.assertEqual(table.table_name, self.table_name)

    def tearDown(self):
        self.table_store.delete_table()
        self.table_store = None

    def test_insert_get_and_delete(self):
        pk1 = "pk1"
        rk1 = "rk1"
        value1 = "test1"
        data1: EntityType = {"PartitionKey": pk1, "RowKey": rk1, "value": value1}
        
        # upsert/insert
        usr = self.table_store.insert_entity(data1)
        self.assertIsNotNone(usr)

        # get inserted entity
        ger = self.table_store.get_entity(pk1, rk1)
        self.assertIsNotNone(ger)
        self.assertEqual(ger.get("value"), value1)

        # delete entity
        self.table_store.delete_entity(pk1, rk1)

        # get no entity after deletion
        gdr = self.table_store.get_entity(pk1, rk1)
        self.assertIsNone(gdr)

    def test_insert_multiple_and_query(self):
        pk1 = "test_10001"
        pk2 = "test_10002"
        data2: EntityType = {"PartitionKey": pk1, "RowKey": "20231101103103_test_10001", "value": "test2"}
        data3: EntityType = {"PartitionKey": pk1, "RowKey": "20231101123103_test_10001", "value": "test3"}
        data4: EntityType = {"PartitionKey": pk1, "RowKey": "20231101143103_test_10001", "value": "test4"}
        data5: EntityType = {"PartitionKey": pk1, "RowKey": "20231101163103_test_10001", "value": "test5"}
        data6: EntityType = {"PartitionKey": pk2, "RowKey": "20231101133105_test_10002", "value": "test6"}
        
        for entity in [data2, data3, data4, data5, data6]:
            usr = self.table_store.insert_entity(entity)
            self.assertIsNotNone(usr)

        qr = self.table_store.query_entities(pk1, "20231101123104")
        self.assertIsNotNone(qr)
        self.assertEqual(len(qr), 2)
        self.assertEqual(qr[0].get('value'), 'test4')
        self.assertEqual(qr[1].get('value'), 'test5')

    def test_upsert_for_update(self):
        pk2 = "test_10002"
        rk7 = "20231101133106_test_10002"
        data7: EntityType = {"PartitionKey": pk2, "RowKey": rk7, "value": "test7", "status": "new"}
        data8: EntityType = {"PartitionKey": pk2, "RowKey": rk7, "value": "test8"}
        data9: EntityType = {"PartitionKey": pk2, "RowKey": rk7, "status": "merged"}

        # insert data7
        isr7 = self.table_store.insert_entity(data7)
        self.assertIsNotNone(isr7)
        ger7 = self.table_store.get_entity(pk2, rk7)
        self.assertIsNotNone(ger7)
        self.assertEqual(ger7.get("value"), "test7")

        # insert with data8 fails as same rowKey already exists from data7
        isr8 = self.table_store.insert_entity(data8)
        self.assertIsNone(isr8)

        # upsert data8, replace data7 by default
        usr8 = self.table_store.upsert_entity(data8)
        self.assertIsNotNone(usr8)
        ger8 = self.table_store.get_entity(pk2, rk7)
        self.assertIsNotNone(ger8)
        self.assertEqual(ger8.get("value"), "test8")
        self.assertIsNone(ger8.get("status")) # In replace mode, status is removed as data8 has no status

        # upsert data9, use merge explicitly
        umr9 = self.table_store.upsert_entity(data9, UpdateMode.MERGE)
        self.assertIsNotNone(umr9)
        ger9 = self.table_store.get_entity(pk2, rk7)
        self.assertIsNotNone(ger9)
        self.assertEqual(ger9.get("value"), "test8") # value from data8 is retained as merge mode is used
        self.assertEqual(ger9.get("status"), "merged")

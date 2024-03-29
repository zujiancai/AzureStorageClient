from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from azure.data.tables import TableServiceClient, TableClient, UpdateMode


class TableStore(object):
    def __init__(self, conn_str: str, table_name: str):
        self.connection_string = conn_str
        self.table_name = table_name

    def create_if_not_exist(self) -> TableClient:
        with TableServiceClient.from_connection_string(self.connection_string) as table_service_client:
            return table_service_client.create_table_if_not_exists(table_name=self.table_name)
        
    def delete_table(self):
        with TableServiceClient.from_connection_string(self.connection_string) as table_service_client:
            return table_service_client.delete_table(table_name=self.table_name)
        
    def insert_entity(self, data) -> bool:
        with TableClient.from_connection_string(self.connection_string, self.table_name) as table:
            try:
                return table.create_entity(entity=data)
            except ResourceExistsError:
                return None

    def upsert_entity(self, data, update_mode: UpdateMode = UpdateMode.REPLACE):
        with TableClient.from_connection_string(self.connection_string, self.table_name) as table:
            return table.upsert_entity(mode=update_mode, entity=data)

    def delete_entity(self, partition_key, row_key):
        with TableClient.from_connection_string(self.connection_string, self.table_name) as table:
            return table.delete_entity(row_key=row_key, partition_key=partition_key)

    def get_entity(self, partition_key, row_key):
        with TableClient.from_connection_string(self.connection_string, self.table_name) as table:
            try:
                return table.get_entity(row_key=row_key, partition_key=partition_key)
            except ResourceNotFoundError:
                return None
        
    def query_entities(self, partition_key, rk_continuation_token=""):
        with TableClient.from_connection_string(self.connection_string, self.table_name) as table:
            parameters = { "pk": partition_key, "rkt": rk_continuation_token }
            query_filter = "PartitionKey eq @pk and RowKey gt @rkt"
            return list(table.query_entities(query_filter, parameters=parameters))

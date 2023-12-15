from typing import TypedDict

from batch_job.job_data import JobData
from batch_job.table_store import TableStore, UpdateMode


class InMemoryTableStore(TableStore):
    def __init__(self, conn_str: str, table_name: str):
        super().__init__(conn_str, table_name)
        self._entities = {}

    def create_if_not_exist(self) -> None:
        pass
        
    def delete_table(self):
        self._entities = {}
        
    def insert_entity(self, data: TypedDict) -> bool:
        if data["PartitionKey"] not in self._entities:
            self._entities[data["PartitionKey"]] = {}
        if data["RowKey"] in self._entities[data["PartitionKey"]]:
            return False
        self._entities[data["PartitionKey"]][data["RowKey"]] = data
        return True

    def upsert_entity(self, data: TypedDict, update_mode: UpdateMode = UpdateMode.REPLACE):
        if data["PartitionKey"] not in self._entities:
            self._entities[data["PartitionKey"]] = {}
        if update_mode == UpdateMode.REPLACE:
            self._entities[data["PartitionKey"]][data["RowKey"]] = data
        elif update_mode == UpdateMode.MERGE:
            self._entities[data["PartitionKey"]][data["RowKey"]].update(data)

    def delete_entity(self, partition_key, row_key):
        if partition_key in self._entities:
            if row_key in self._entities[partition_key]:
                del self._entities[partition_key][row_key]

    def get_entity(self, partition_key, row_key):
        if partition_key in self._entities:
            if row_key in self._entities[partition_key]:
                return self._entities[partition_key][row_key]
        
    def query_entities(self, partition_key, rk_continuation_token=""):
        if partition_key in self._entities:
            return list([ entity for row_key, entity in self._entities[partition_key].items() if row_key > rk_continuation_token ])
        return []


class MockJobData(JobData):
    def __init__(self, conn_str: str):
        self.info_store = InMemoryTableStore(conn_str, "JobInfo")
        self.run_store = InMemoryTableStore(conn_str, "JobRun")
    
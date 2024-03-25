import os

from batch_job.job_data import JobData
from batch_job.table_store import TableStore, UpdateMode
from batch_job.blob_store import BlobStore


class InMemoryTableStore(TableStore):
    def __init__(self, conn_str: str, table_name: str):
        super().__init__(conn_str, table_name)
        self._entities = {}

    def create_if_not_exist(self) -> None:
        pass
        
    def delete_table(self):
        self._entities = {}
        
    def insert_entity(self, data) -> bool:
        if data["PartitionKey"] not in self._entities:
            self._entities[data["PartitionKey"]] = {}
        if data["RowKey"] in self._entities[data["PartitionKey"]]:
            return False
        self._entities[data["PartitionKey"]][data["RowKey"]] = data
        return True

    def upsert_entity(self, data, update_mode: UpdateMode = UpdateMode.REPLACE):
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
    

class LocalBlobStore(BlobStore):
    def __init__(self, connection_string):
        super().__init__(connection_string)
        self.local_files = {
            "test_container1/test_blob1": "test_file1",
            "test_container2/test_blob2": "test_file2",
        }

    def create_blob_client(self, container_name, blob_name):
        pass

    def get_blob_id(self, container_name, blob_name):
        return f"{container_name}/{blob_name}"

    def upload(self, container_name, blob_name, file_path) -> bool:
        blob_id = self.get_blob_id(container_name, blob_name)
        if os.path.exists(file_path) and not blob_id in self.local_files:
            self.local_files[blob_id] = file_path
            return True
        return False

    def download(self, container_name, blob_name, file_path) -> bool:
        blob_id = self.get_blob_id(container_name, blob_name)
        if blob_id in self.local_files:
            blob_path = self.local_files[blob_id]
            if file_path != blob_path:
                with open(blob_path, "rb") as data:
                    with open(file_path, "wb") as f:
                        f.write(data.read())
            return True
        return False
    
    def exists(self, container_name, blob_name) -> bool:
        blob_id = self.get_blob_id(container_name, blob_name)
        return blob_id in self.local_files

    def delete(self, container_name, blob_name) -> bool:
        blob_id = self.get_blob_id(container_name, blob_name)
        if blob_id in self.local_files:
            del self.local_files[blob_id]

    def clean_up(self, container_name, least_blob_name: str) -> list[str]:
        deleted = []
        for blob_id in list(self.local_files.keys()):
            if blob_id.split('/')[0] == container_name:
                if blob_id < least_blob_name:
                    deleted.append(blob_id)
        for blob_id in deleted:
            del self.local_files[blob_id]
        return deleted

    def lease_blob(self, container_name, blob_name, lease_duration=15):
        pass


class MockJobData(JobData):
    def __init__(self, conn_str: str):
        self.info_store = InMemoryTableStore(conn_str, "JobInfo")
        self.run_store = InMemoryTableStore(conn_str, "JobRun")
        self.blob_store = LocalBlobStore(conn_str)

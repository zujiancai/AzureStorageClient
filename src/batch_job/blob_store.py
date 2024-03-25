from azure.core.exceptions import HttpResponseError
from azure.storage.blob import BlobServiceClient
import os


class BlobStore:
    def __init__(self, connection_string):
        self._connection_string = connection_string

    def create_blob_client(self, container_name, blob_name):
        blob_service_client = BlobServiceClient.from_connection_string(self._connection_string)

        container = blob_service_client.get_container_client(container_name)
        if not container.exists():
            container.create_container()

        return container.get_blob_client(blob_name)

    def upload(self, container_name, blob_name, file_path) -> bool:
        blob_client = self.create_blob_client(container_name, blob_name)
        if os.path.exists(file_path) and not blob_client.exists():
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, blob_type="BlockBlob")
            return True
        return False

    def download(self, container_name, blob_name, file_path) -> bool:
        blob_client = self.create_blob_client(container_name, blob_name)
        if blob_client.exists():
            with open(file_path, "wb") as data:
                download_stream = blob_client.download_blob()
                data.write(download_stream.readall())
            return True
        return False
    
    def exists(self, container_name, blob_name) -> bool:
        blob_client = self.create_blob_client(container_name, blob_name)
        return blob_client.exists()

    def delete(self, container_name, blob_name) -> bool:
        blob_client = self.create_blob_client(container_name, blob_name)
        if blob_client.exists():
            blob_client.delete_blob()

    def clean_up(self, container_name, least_blob_name: str) -> list[str]:
        blob_service_client = BlobServiceClient.from_connection_string(self._connection_string)
        container = blob_service_client.get_container_client(container_name)
        deleted = []
        if container.exists():
            for blob in container.list_blob_names():
                if blob < least_blob_name:
                    container.delete_blob(blob)
                    deleted.append(blob)
        return deleted

    def lease_blob(self, container_name, blob_name, lease_duration=15):
        blob_client = self.create_blob_client(container_name, blob_name)
        if blob_client.exists():
            try:
                return blob_client.acquire_lease(lease_duration=lease_duration)
            except HttpResponseError:
                pass

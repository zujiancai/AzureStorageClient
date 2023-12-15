from azure.core.exceptions import HttpResponseError
import os
import unittest
from batch_job.blob_store import BlobStore


@unittest.skip("Skip Azure Storage Emulator dependent tests")
class TestBlobStore(unittest.TestCase):
    def setUp(self):
        # Needs to start Azure Storage Emulator for this to work
        self.blob_store = BlobStore('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;')
        self.container_name = 'testcontainer'
        self.file_path_original = 'original_data.txt'
        
        # create a test file locally
        with open(self.file_path_original, 'wt') as f:
            f.write('This is a test file.')

    def tearDown(self):
        # remove the test file locally if it still exists
        if os.path.exists(self.file_path_original):
            os.remove(self.file_path_original)

    def test_upload_download_and_delete(self):
        blob_name = 'testblob'
        file_path_download = 'download_data.txt'

        # upload and download a blob
        self.blob_store.upload(self.container_name, blob_name, self.file_path_original)
        self.blob_store.download(self.container_name, blob_name, file_path_download)

        # compare downloaded with original
        with open(self.file_path_original, 'r') as f1:
            with open(file_path_download, 'r') as f2:
                self.assertEqual(f1.read(), f2.read())

        # delete the blob and downloaded file to clean up
        self.blob_store.delete(self.container_name, blob_name)
        blob = self.blob_store.create_blob_client(self.container_name, blob_name)
        self.assertFalse(blob.exists())

        os.remove(file_path_download)

    def test_upload_existing_blob(self):
        blob_name = 'testblob2'

        # upload a blob
        self.blob_store.upload(self.container_name, blob_name, self.file_path_original)
        blob = self.blob_store.create_blob_client(self.container_name, blob_name)
        self.assertTrue(blob.exists())

        # upload again and will fail
        self.assertFalse(self.blob_store.upload(self.container_name, blob_name, self.file_path_original))

        # delete the blob to clean up
        self.blob_store.delete(self.container_name, blob_name)
        blob = self.blob_store.create_blob_client(self.container_name, blob_name)
        self.assertFalse(blob.exists())

    def test_download_non_existing_blob(self):
        blob_name = 'notexistingblob1'
        file_path_download = 'not_existing_download.txt'

        self.assertFalse(self.blob_store.download(self.container_name, blob_name, file_path_download))
        self.assertFalse(os.path.exists(file_path_download))

    def test_delete_non_existing_blob(self):
        blob_name = 'notexistingblob2'

        self.assertFalse(self.blob_store.delete(self.container_name, blob_name))

    def test_lease_blob_success(self):
        blob_name = 'leaseblob1'
        
        # try lease a blob, if blob does not exist (no lease returns), create the blob and lease it
        lease = self.blob_store.lease_blob(self.container_name, blob_name)
        if not lease:
            self.blob_store.upload(self.container_name, blob_name, self.file_path_original)
            lease = self.blob_store.lease_blob(self.container_name, blob_name)
        
        self.assertIsNotNone(lease)

        # clean up the lease and the blob
        lease.release()
        self.blob_store.delete(self.container_name, blob_name)

    def test_lease_blob_competing(self):
        blob_name = 'leaseblob2'

        # try lease a blob, if blob does not exist (no lease returns), create the blob and lease it
        lease1 = self.blob_store.lease_blob(self.container_name, blob_name)
        if not lease1:
            self.blob_store.upload(self.container_name, blob_name, self.file_path_original)
            lease1 = self.blob_store.lease_blob(self.container_name, blob_name, 1)

        self.assertIsNotNone(lease1)

        # try lease the blob again and will fail for race condition
        with self.assertRaises(HttpResponseError):
            lease2 = self.blob_store.lease_blob(self.container_name, blob_name, 1)

        # after lease1 is released, lease2 can be acquired
        lease1.release()
        lease2 = self.blob_store.lease_blob(self.container_name, blob_name)
        self.assertIsNotNone(lease2)

        # clean up the lease and the blob
        lease2.release()
        self.blob_store.delete(self.container_name, blob_name)

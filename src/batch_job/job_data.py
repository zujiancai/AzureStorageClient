from datetime import datetime
import os
from typing import Callable
from typing_extensions import TypedDict

from batch_job import TEMP_DIR
from batch_job.blob_store import BlobStore
from batch_job.table_store import TableStore


class JobStatus:
    Pending = 'pending'      # The job is just created without checking dependencies, or dependencies are not ready yet (need to wait for dependencies).
    Active = 'active'        # The job is running or ready to run (dependency check is passed).
    Suspended = 'suspended'  # The job is paused due to batch size constraint or exception/error.
    Completed = 'completed'  # An end state. Job is finished successfully.
    Failed = 'failed'        # An end state. Job has failed due to too many errors or consecutive errors.
    Expired = 'expired'      # An end state. Job has not finished before the maximum hours for it to run. Newer inputs may be available for a job at a more recent time.

    @staticmethod
    def is_end_state(status):
        return status in [ JobStatus.Completed, JobStatus.Failed, JobStatus.Expired ]


class JobInfo(TypedDict):
    PartitionKey: str  # jobType_offsetVersion, e.g. LoadList_1001
    RowKey: str        # jobId_offsetRevision_PartitionKey
    revision: int      # default 0, increment it to rerun a job with same inputs.
    inputs: str        # dictionary in pickle
    states: str        # dictionary in pickle
    status: str        # current job status
    create_time: datetime
    update_time: datetime


class JobRun(TypedDict):
    PartitionKey: str   # Reference to JobInfo RowKey
    RowKey: str         # endTime_PartitionKey
    is_error: bool
    message: str
    end_status: str     # the ending job status after this run
    start_time: datetime
    end_time: datetime


class JobData(object):
    def __init__(self, conn_str: str, temp_dir: str=TEMP_DIR):
        self.info_store = TableStore(conn_str, "JobInfo")
        self.run_store = TableStore(conn_str, "JobRun")
        self.blob_store = BlobStore(conn_str)
        self.temp_dir = temp_dir

    def create_if_not_exist(self):
        self.info_store.create_if_not_exist()
        self.run_store.create_if_not_exist()

    def upsert_info(self, data: JobInfo):
        if self.info_store.upsert_entity(data):
            return data
    
    def get_info(self, job_id: str):
        id_parts = job_id.split('_')
        assert(len(id_parts) == 4)
        partition_key = '_'.join(id_parts[2:])
        return self.info_store.get_entity(partition_key, job_id)
    
    def list_infos(self, job_name: str):
        return self.info_store.query_entities(job_name)
    
    def list_runs(self, job_id: str):
        return self.run_store.query_entities(job_id)

    def expire_job(self, job_info: JobInfo, current_time: datetime):
        job_info['status'] = JobStatus.Expired
        job_info['update_time'] = current_time
        return self.upsert_info(job_info)

    def fail_job(self, job_info: JobInfo, current_time: datetime):
        job_info['status'] = JobStatus.Failed
        job_info['update_time'] = current_time
        return self.upsert_info(job_info)
        
    def complete_run(self, success: bool, job_info: JobInfo, message: str, start_time: datetime):
        self.insert_run(job_info['RowKey'], start_time, job_info['update_time'], message, job_info['status'], not success)
        self.upsert_info(job_info)
        
    def insert_run(self, job_id: str, start_time: datetime, end_time: datetime, message: str, end_status: str, is_error: bool = False):
        job_run: JobRun = {
            "PartitionKey": job_id,
            "RowKey": end_time.strftime('%Y%m%d%H%M%S%f') + '_' + job_id,
            "is_error": is_error,
            "message": message,
            "end_status": end_status,
            "start_time": start_time,
            "end_time": end_time
        }
        if self.run_store.insert_entity(job_run):
            return job_run

    '''
    Get the number of consecutive failures and total number of failures for a job.
    '''
    def summarize_failures(self, job_info: JobInfo) -> (int, int):
        all_runs = self.run_store.query_entities(job_info['RowKey'])
        consecutive_failure_count = 0
        for run in sorted(all_runs, key=lambda x: x['start_time'], reverse=True):
            if run['is_error']:
                consecutive_failure_count += 1
            else:
                break
        return consecutive_failure_count, sum(1 for run in all_runs if run['is_error'])
    
    def get_temp_file_path(self, container_name: str, blob_name: str) -> str:
        dir_path = '{0}/{1}'.format(self.temp_dir, container_name)
        os.makedirs(dir_path, exist_ok=True)
        return dir_path + '/' + blob_name + '.tmp'
    
    def upload_file(self, container_name: str, blob_name: str, create_file_func: Callable[[str], bool]) -> bool:
        file_path = self.get_temp_file_path(container_name, blob_name)
        if create_file_func(file_path):
            return self.blob_store.upload(container_name, blob_name, file_path)
        return False
    
    def download_file(self, container_name: str, blob_name: str) -> str:
        file_path = self.get_temp_file_path(container_name, blob_name)
        if self.blob_store.download(container_name, blob_name, file_path):
            return file_path
        
    def lease_job(self, job_type: str, lease_duration: int = 15):
        return self.blob_store.lease_blob('BatchJobAdmin', job_type, lease_duration)

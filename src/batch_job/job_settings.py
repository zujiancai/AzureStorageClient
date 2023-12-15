from datetime import datetime
import pickle
from typing import Type

from batch_job.base_job import BaseJob, BaseJobInputs, BaseJobStates
from batch_job.job_data import JobInfo, JobStatus
from batch_job.job_schedule import JobSchedule


class JobSettings(object):
    def __init__(self, 
                 job_schedule: JobSchedule,
                 max_failures: int,
                 max_consecutive_failures: int,
                 expire_hours: int,
                 batch_size: int,
                 process_interval_in_seconds: float,
                 job_class: Type[BaseJob],
                 job_type: str,
                 job_version: int,
                 require_lock: bool):
        self.job_schedule = job_schedule
        self.max_failures = max_failures
        self.max_consecutive_failures = max_consecutive_failures
        self.expire_hours = expire_hours
        self.batch_size = batch_size
        self.process_interval_in_seconds = process_interval_in_seconds
        self.job_class = job_class
        self.job_type = job_type
        self.job_version = job_version
        self.require_lock = require_lock

    def create_info(self, revision: int, run_date: datetime) -> JobInfo:
        if not run_date:
            run_date = datetime.utcnow()
        partition_key, row_key, run_date = self.job_class.create_keys(self.job_type, self.job_version, run_date, revision)
        inputs = pickle.dumps(BaseJobInputs(run_date=run_date, batch_size=self.batch_size, process_interval=self.process_interval_in_seconds))
        states = pickle.dumps(BaseJobStates(last_processed='', processed=0, skipped=0))
        return JobInfo(
            PartitionKey=partition_key,
            RowKey=row_key,
            revision=revision,
            inputs=inputs,
            states=states,
            status=JobStatus.Pending,
            create_time=datetime.utcnow(),
            update_time=datetime.utcnow())


class JobSettingsFactory(object):
    '''
    Default settings: max_failures = 20, max_consecutive_failures = 5, expire_hours = 24, batch_size = 1000, process_interval_in_seconds = 0
    '''
    def create_default(self, job_schedule: JobSchedule, job_class: Type[BaseJob], job_type: str, job_version: int):
        return JobSettings(job_schedule, 20, 5, 24, 1000, 0, job_class, job_type, job_version, False)
    
    def create(self, friendly_job_name: str):
        return self.create_default(JobSchedule(), BaseJob, friendly_job_name, 1)

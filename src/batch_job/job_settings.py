from datetime import datetime
from importlib import import_module
import pickle
import sys
from typing import Type

from batch_job.base_job import BaseJob, BaseJobInputs, BaseJobStates
from batch_job.job_data import JobInfo, JobStatus
from batch_job.job_schedule import JobSchedule, schedule_from_crontab


def cached_import(module_path, class_name):
    # Check whether module is loaded and fully initialized.
    if not (
        (module := sys.modules.get(module_path))
        and (spec := getattr(module, "__spec__", None))
        and getattr(spec, "_initializing", False) is False
    ):
        module = import_module(module_path)
    return getattr(module, class_name)


def import_string(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit(".", 1)
    except ValueError as err:
        raise ImportError("%s doesn't look like a module path" % dotted_path) from err

    try:
        return cached_import(module_path, class_name)
    except AttributeError as err:
        raise ImportError('Module "%s" does not define a "%s" attribute/class' % (module_path, class_name)) from err


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
    

def convert_settings(raw_settings: dict) -> JobSettings:
    '''
    Convert dict settings to JobSettings object. 
    - These settings are required: job_class, job_type
    - For other settings, if they are missing, use default values: job_schedule = None (no constraint), max_failures = 20, max_consecutive_failures = 5, 
        expire_hours = 24, batch_size = 1000, process_interval_in_seconds = 0
    '''
    job_schedule = schedule_from_crontab(raw_settings.get('job_schedule', None))
    max_failures = int(raw_settings.get('max_failures', 20))
    max_consecutive_failures = int(raw_settings.get('max_consecutive_failures', 5))
    expire_hours = int(raw_settings.get('expire_hours', 24))
    batch_size = int(raw_settings.get('batch_size', 1000))
    process_interval_in_seconds = float(raw_settings.get('process_interval_in_seconds', 0))
    job_class = import_string(raw_settings.get('job_class'))
    job_type = str(raw_settings.get('job_type'))
    job_version = int(raw_settings.get('job_version', 1))
    require_lock = bool(raw_settings.get('require_lock', False))
    return JobSettings(job_schedule, max_failures, max_consecutive_failures, expire_hours, batch_size, process_interval_in_seconds, job_class, job_type, job_version, require_lock)


class JobSettingsFactory(object):
    def __init__(self, raw_settings: dict) -> None:
        self.all_settings = raw_settings
    
    def create(self, friendly_job_name: str):
        if friendly_job_name in self.all_settings:
            return convert_settings(self.all_settings[friendly_job_name])
        return convert_settings({ 'job_class': 'batch_job.base_job.BaseJob', 'job_type': friendly_job_name })

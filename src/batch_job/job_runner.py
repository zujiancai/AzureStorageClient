from datetime import datetime, timedelta, timezone

from batch_job.job_data import JobData, JobStatus
from batch_job.job_settings import JobSettingsFactory, JobSettings


class JobRunner(object):
    def __init__(self, settings_factory: JobSettingsFactory, job_data: JobData):
        self.settings_factory = settings_factory
        self.job_data = job_data

    def run(self, friendly_job_name: str, revision: int = 0, run_date_override: datetime = None):
        # Resolve the friendly job name to JobSettings
        settings = self.settings_factory.create(friendly_job_name)
        self.run_success = []
        self.run_with_error = []
        self.set_failed = []
        self.set_expired = []
        
        run_date = datetime.now(timezone.utc) if not run_date_override else run_date_override

        if settings.require_lock:
            lease = self.job_data.lease_job(settings.job_type)
            if lease:
                try:
                    self.internal_run(settings, revision, run_date)
                finally:
                    lease.release()
        else:
            self.internal_run(settings, revision, run_date)

    def internal_run(self, settings: JobSettings, revision: int, run_date: datetime):
        # Get all existing job infos for the given job settings
        current_time = datetime.now(timezone.utc)
        all_infos = self.job_data.list_infos(settings.get_job_partition())

        # Check if any existing active, pending, or suspended job to resume, to fail or to expire.
        job_to_run = None
        new_job_id = settings.get_job_id(run_date, revision)
        for info in all_infos:
            if new_job_id == info['RowKey']:
                new_job_id = None # Set None to notify the new job id has been created.
            if not JobStatus.is_end_state(info['status']):
                # check and set failure
                consecutive_failure_count, total_failure_count = self.job_data.summarize_failures(info)
                if consecutive_failure_count >= settings.max_consecutive_failures or total_failure_count >= settings.max_failures:
                    self.job_data.fail_job(info, current_time)
                    self.set_failed.append(info['RowKey'])
                # check and set expiration
                elif current_time > info['create_time'] + timedelta(hours = settings.expire_hours):
                    self.job_data.expire_job(info, current_time)
                    self.set_expired.append(info['RowKey'])
                # find the resumable jobs and only run the first one
                elif not job_to_run:
                    job_to_run = settings.job_class(self.job_data, info)

        # If no existing to resume and the new job id has not been created, check the job schedule to see if a new job should be created.
        if not job_to_run and new_job_id:
            if settings.job_schedule.check(current_time):
                job_to_run = settings.job_class(self.job_data, settings.create_info(revision, run_date))

        # At most one job will be executed. After the job is executed, update job info and job run.
        if job_to_run:
            if job_to_run.run():
                self.run_success.append(job_to_run.job_info['RowKey'])
            else:
                self.run_with_error.append(job_to_run.job_info['RowKey'])

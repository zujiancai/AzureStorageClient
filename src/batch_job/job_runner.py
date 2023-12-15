from datetime import datetime, timedelta

from batch_job.job_data import JobData, JobStatus
from batch_job.job_settings import JobSettingsFactory, JobSettings


class JobRunner(object):
    def __init__(self, settings_factory: JobSettingsFactory, job_data: JobData):
        self.settings_factory = settings_factory
        self.job_data = job_data

    def run(self, friendly_job_name: str):
        # Resolve the friendly job name to JobSettings
        settings = self.settings_factory.create(friendly_job_name)
        self.run_success = []
        self.run_with_error = []
        self.set_failed = []
        self.set_expired = []

        if settings.require_lock:
            lease = self.job_data.lease_job(settings.job_type)
            if lease:
                try:
                    self.internal_run(settings)
                finally:
                    lease.release()
        else:
            self.internal_run(settings)

    def internal_run(self, settings: JobSettings):
        # Get all existing job infos for the given job settings
        current_time = datetime.utcnow()
        revision = 0
        job_type_id, job_id, run_date = settings.job_class.create_keys(settings.job_type, settings.job_version, current_time, revision)
        all_infos = self.job_data.list_infos(job_type_id)

        # Check if any existing active, pending, or suspended job to resume, to fail or to expire.
        job_to_run = None
        latest_job = None
        for info in all_infos:
            latest_job = info if not latest_job or info['RowKey'] > latest_job['RowKey'] else latest_job
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

        # If no existing to resume, check the job schedule to see if a new job should be created.
        if not job_to_run:
            # Check if the job is due, and has not been created yet, if so, create a new job.
            if settings.job_schedule.check(current_time) and (not latest_job or latest_job['RowKey'] < job_id):
                job_to_run = settings.job_class(self.job_data, settings.create_info(revision, current_time))

        # At most one job will be executed. After the job is executed, update job info and job run.
        if job_to_run:
            if job_to_run.run():
                self.run_success.append(job_to_run.job_info['RowKey'])
            else:
                self.run_with_error.append(job_to_run.job_info['RowKey'])

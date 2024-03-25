from datetime import datetime, timezone
import pickle
import time
from typing_extensions import TypedDict

from batch_job import VERSION_OFFSET, REVISION_OFFSET
from batch_job.job_data import JobInfo, JobData, JobStatus


class BaseJobInputs(TypedDict): 
    run_date: datetime
    batch_size: int
    process_interval: float


class BaseJobStates(TypedDict):
    last_processed: str
    processed: int
    skipped: int


class BaseJob(object):
    def __init__(self, job_data: JobData, job_info: JobInfo) -> None:
        self.job_inputs: BaseJobInputs = pickle.loads(job_info['inputs'])
        self.job_states: BaseJobStates = pickle.loads(job_info['states'])
        self.message = ''
        self.job_info = job_info
        self.job_data = job_data

    def get_type(self) -> str:
        return self.__class__.__name__
    
    def list_expected(self, run_date: datetime) -> list[tuple[str, str]]:
        return []
    
    def list_not_expected(self, run_date: datetime) -> list[tuple[str, str]]:
        return []

    def check_dependencies(self, run_date: datetime) -> bool:
        if not JobStatus.is_end_state(self.job_info['status']):
            # Check required data
            for expected_data_id in self.list_expected(run_date):
                if not self.job_data.file_exists(*expected_data_id):
                    self.message = 'Job {0} expects data {1}/{2} but it does not exist.'.format(self.get_type(), *expected_data_id)
                    return False
            # Check unexpected data
            for not_expected_data_id in self.list_not_expected(run_date):
                if self.job_data.file_exists(*not_expected_data_id):
                    self.message = 'Job {0} does not expect data {1}/{2} but it exists.'.format(self.get_type(), *not_expected_data_id)
                    return False
            self.job_info['status'] = JobStatus.Active
            return True
        return False

    def load_items(self, last_processed: str) -> tuple[bool, list]:
        '''
        Optional for subclass to override. It is used to populate the list to loop through.
        - Return[0]: a flag of true if all data has been loaded, false otherwise (use last_processed in states to load more in next iteration).
        - Return[1]: a list of items to loop through. If it is overrided to return a non-empty list, process_item must be overrided as well.
        '''
        return True, []

    def process_item(self, work_item) -> bool:
        '''
        Optional for subclass to override. Logic to process one item in the list. Return True if the item is processed successfully, false if it is skipped.
         - If the item could not be processed and need to be retried later, raise an exception to exit current iteration.
         - The default behavior is to throw a not implemented error.
        '''
        raise NotImplementedError('process_one is not implemented.')

    def post_loop(self, run_date: datetime):
        '''
        Optional for subclass to override. It is for post-loop handling, e.g. saving final result to blob.
        '''
        pass
    
    def run(self):
        try:
            self.start_time = datetime.now(timezone.utc)
            return self.internal_run()
        except Exception as err:
            self.job_info['status'] = JobStatus.Suspended
            self.message = 'Job failed with error: ' + str(err)[0:200]
            return self.save_results(False)

    def internal_run(self):
        if not self.check_dependencies(self.job_inputs['run_date']):
            return True # If job is skipped due to dependencies or in not runnable status, return as success.

        all_loaded, work_items = self.load_items(self.job_states['last_processed'])
        
        item_count = 0
        for work_item in work_items:
            if self.process_item(work_item):
                self.job_states['processed'] += 1
            else:
                self.job_states['skipped'] += 1

            item_count += 1
            self.job_states['last_processed'] = str(work_item)

            if item_count >= self.job_inputs['batch_size']:
                self.message = 'Job {0} is suspended for reaching batch size {1} after handling {2} with ending item {3}.'.format(
                    self.get_type(), self.job_inputs['batch_size'], item_count, self.job_states['last_processed'])
                break

            if self.job_inputs['process_interval'] > 0:
                time.sleep(self.job_inputs['process_interval'])

        self.post_loop(self.job_inputs['run_date'])

        if not self.message: # if no message, we infer that all items in the list are handled.
            if all_loaded:
                self.job_info['status'] = JobStatus.Completed
                self.message = 'Job {0} completed after handling {1} with ending item {2}.'.format(self.get_type(), item_count, self.job_states['last_processed'])
            else:
                self.job_info['status'] = JobStatus.Suspended
                self.message = 'Job {0} is suspended for more data to load.'.format(self.get_type())

        return self.save_results(True)
    
    def save_results(self, success: bool) -> tuple[bool, str]:
        # self.job_info['inputs'] = pickle.dumps(self.job_inputs) # inputs should not change
        self.job_info['states'] = pickle.dumps(self.job_states)
        self.job_info['update_time'] = datetime.now(timezone.utc)

        self.job_data.complete_run(success, self.job_info, self.message, self.start_time)
        return success

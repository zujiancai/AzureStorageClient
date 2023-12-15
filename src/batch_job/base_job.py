from datetime import datetime
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

    @staticmethod
    def create_keys(job_type: str, job_version: int, run_date: datetime, revision: int) -> (str, str, datetime):
        partition_key = '{0}_{1}'.format(job_type, job_version + VERSION_OFFSET)
        row_key = '{0}_{1}_{2}'.format(run_date.strftime('%Y%m%d'), revision + REVISION_OFFSET, partition_key)
        return partition_key, row_key, run_date.replace(hour=0, minute=0, second=0, microsecond=0)

    def get_type(self) -> str:
        return self.__class__.__name__
    
    def get_required_jobs(self) -> dict[str, str]:
        return {}
    
    def get_required_data(self) -> dict[str, bool]:
        return {}

    def check_dependencies(self) -> bool:
        if self.job_info['status'] != JobStatus.Pending:
            return True
        # Check required jobs
        for required_job_id, expected_status in self.get_required_jobs().items():
            required_job = self.job_data.get_info(required_job_id)
            if not required_job:
                self.message = 'Job {0} depends on job {1} to be {2} but it does not exist.'.format(self.get_type(), required_job_id, expected_status)
                return False
            elif required_job['status'] != expected_status:
                self.message = 'Job {0} depends on job {1} to be {2} but it is {3}.'.format(self.get_type(), required_job_id, expected_status, required_job['status'])
                return False
        # Check required data
        for required_data, expect_to_exist in self.get_required_data().items():
            # required_data = None #store.load_df(required_data_id)
            # if not type(required_data) is pd.DataFrame:
            #     return False, 'Job {0} is skipped as required data {1} is missing.'.format(self.get_type(), required_data_id)
            pass
        self.job_info['status'] = JobStatus.Active
        return True

    def load_items(self, last_processed: str) -> (bool, list):
        '''
        Used to populate the list to loop through. 
        - Return[0]: a flag of true if all data has been loaded, false otherwise (use last_processed in states to load more in next iteration).
        - Return[1]: a list of items to loop through.
        '''
        return True, []

    def process_item(self, work_item) -> bool:
        '''
        Logic to process one item in the list. Return True if the item is processed successfully, false if it is skipped.
         - if the item could not be processed and need to be retried later, raise an exception to exit current iteration.
        '''
        raise NotImplementedError('process_one is not implemented.')

    def post_loop(self):
        '''
        Post-loop handling.
        '''
        pass
    
    def run(self):
        try:
            self.start_time = datetime.utcnow()
            return self.internal_run()
        except Exception as err:
            self.job_info['status'] = JobStatus.Suspended
            self.message = 'Job failed with error: ' + str(err)[0:200]
            return self.save_results(False)

    def internal_run(self):
        if not self.check_dependencies():
            return self.save_results(True)

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

        self.post_loop()

        if not self.message: # if no message, we infer that all items in the list are handled.
            if all_loaded:
                self.job_info['status'] = JobStatus.Completed
                self.message = 'Job {0} completed after handling {1} with ending item {2}.'.format(self.get_type(), item_count, self.job_states['last_processed'])
            else:
                self.job_info['status'] = JobStatus.Suspended
                self.message = 'Job {0} is suspended for more data to load.'.format(self.get_type())

        return self.save_results(True)
    
    def save_results(self, success: bool) -> (bool, str):
        self.job_info['inputs'] = pickle.dumps(self.job_inputs)
        self.job_info['states'] = pickle.dumps(self.job_states)
        self.job_info['update_time'] = datetime.utcnow()

        self.job_data.complete_run(success, self.job_info, self.message, self.start_time)
        return success

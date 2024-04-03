import unittest
from datetime import datetime, timedelta, timezone
import pickle
import time

from batch_job.job_data import JobStatus
from batch_job.job_runner import JobRunner
from batch_job.job_settings import JobSettingsFactory, BaseJob, JobSchedule
from tests.mock_data import MockJobData


class TesterJob(BaseJob):
    def __init__(self, job_data, job_info):
        super().__init__(job_data, job_info)

    def load_items(self, last_processed: str) -> bool:
        if not last_processed:
            return False, [1, 2, 3]
        else:
            last_int = int(last_processed)
            items = range(last_int+1, last_int+4)
            return (items[-1] >= 9), items
    
    def process_item(self, item) -> bool:
        if item < 0:
            return False # Skip negative items
        if not 'result' in self.job_states:
            self.job_states['result'] = item
        else:    
            self.job_states['result'] += item
        return True
    
    def post_loop(self, run_date: datetime):
        if not 'result' in self.job_states or self.job_states['result'] > 45:
            raise Exception('Invalid result')


class TestJobRunner(unittest.TestCase):
    def setUp(self):
        test_settings = {
            'BaseJob1': {
                'job_class': 'batch_job.job_settings.BaseJob',
                'job_type': 'BaseJob1'
            },
            'TestJob1': {
                'job_class': 'tests.test_job_runner.TesterJob',
                'job_type': 'TestJob1',
                'max_failures': 3,
                'max_consecutive_failures': 2
            }
        }
        self.job_runner = JobRunner(JobSettingsFactory(test_settings), MockJobData('connection_string'))

    def validate_info(self, job_id: str, property_names: list[str], expected_values: list, compare_properties: list [tuple[str, str]] = [],
                      states_property_names: list[str] = [], states_expected_values: list = []):
        info = self.job_runner.job_data.get_info(job_id)
        self.assertIsNotNone(info)
        for i in range(len(property_names)):
            self.assertEqual(info[property_names[i]], expected_values[i])
        for prop1, prop2 in compare_properties:
            self.assertTrue(info[prop1] < info[prop2])
        if len(states_property_names) > 0:
            states = pickle.loads(info['states'])
            for i in range(len(states_property_names)):
                self.assertEqual(states[states_property_names[i]], states_expected_values[i])
        return info

    def validate_run(self, job_id: str, expected_count: int, property_names: list[str], expected_values: list, compare_properties: list [tuple[str, str]] = []):
        runs = self.job_runner.job_data.list_runs(job_id)
        self.assertEqual(len(runs), expected_count)
        if expected_count > 0:
            for i in range(len(property_names)):
                self.assertEqual(runs[-1][property_names[i]], expected_values[i])
            for prop1, prop2 in compare_properties:
                self.assertTrue(runs[-1][prop1] < runs[-1][prop2])
        return runs

    def test_run_existing_base_job(self):
        # Create a job info
        job_type = 'BaseJob1'
        revision = 2
        current_time = datetime.now(timezone.utc)
        settings = self.job_runner.settings_factory.create(job_type)
        info = settings.create_info(revision, current_time)
        info['status'] = JobStatus.Suspended
        self.job_runner.job_data.upsert_info(info)

        # Run the job
        self.job_runner.run(job_type)

        # Check job and run
        job_id = info['RowKey']
        info = self.validate_info(job_id, ['status', 'revision', 'create_time'], [JobStatus.Completed, revision, current_time])
        self.validate_run(job_id, 1, ['end_status', 'end_time'], [JobStatus.Completed, info['update_time']])

    def test_run_new_base_job(self):
        # Run the job as new
        job_type = 'BaseJob1'
        revision = 8
        self.job_runner.run(job_type, revision)
        settings = self.job_runner.settings_factory.create(job_type)

        # Check job and run
        job_id = settings.get_job_id(datetime.now(timezone.utc), revision)
        self.assertTrue(job_id in self.job_runner.run_success)
        info = self.validate_info(job_id, ['status', 'revision'], [JobStatus.Completed, revision])
        self.validate_run(job_id, 1, ['end_status', 'end_time'], [JobStatus.Completed, info['update_time']])

    def test_run_new_base_job_for_given_date(self):
        # Run the job as new
        job_type = 'BaseJob1'
        revision = 3
        run_date = datetime(2022, 2, 22, tzinfo=timezone.utc)
        self.job_runner.run(job_type, revision, run_date)
        settings = self.job_runner.settings_factory.create(job_type)

        # Check job and run
        job_id = settings.get_job_id(run_date, revision)
        self.assertTrue(job_id in self.job_runner.run_success)
        info = self.validate_info(job_id, ['status', 'revision'], [JobStatus.Completed, revision])
        self.validate_run(job_id, 1, ['end_status', 'end_time'], [JobStatus.Completed, info['update_time']])

    def test_run_new_tester_job_and_resume_to_completion(self):
        # Run the job as new
        job_type = 'TestJob1'
        self.job_runner.run(job_type)
        settings = self.job_runner.settings_factory.create(job_type)

        job_id = settings.get_job_id(datetime.now(timezone.utc), 0)
        info1 = self.validate_info(job_id, ['status'], [JobStatus.Suspended], [], ['last_processed', 'result', 'processed'], ['3', 6, 3])
        self.validate_run(job_id, 1, ['end_status', 'end_time'], [JobStatus.Suspended, info1['update_time']])
        time.sleep(0.2)

        # Second run
        self.job_runner.run(job_type)
        info2 = self.validate_info(job_id, ['status'], [JobStatus.Suspended], [('create_time', 'update_time')], ['last_processed', 'result', 'processed'], ['6', 21, 6])
        self.validate_run(job_id, 2, ['end_status', 'end_time'], [JobStatus.Suspended, info2['update_time']])
        time.sleep(0.2)

        # Third run and complete
        self.job_runner.run(job_type)
        info3 = self.validate_info(job_id, ['status'], [JobStatus.Completed], [('create_time', 'update_time')], ['last_processed', 'result', 'processed'], ['9', 45, 9])
        self.validate_run(job_id, 3, ['end_status', 'end_time'], [JobStatus.Completed, info3['update_time']])

    def test_run_existing_tester_job_with_error(self):
        # Create a job info
        job_type = 'TestJob1'
        current_time = datetime.now(timezone.utc)
        revision = 1
        settings = self.job_runner.settings_factory.create(job_type)
        info = settings.create_info(revision, current_time)
        info['status'] = JobStatus.Suspended
        states = pickle.loads(info['states'])
        states['last_processed'] = '100'
        info['states'] = pickle.dumps(states)
        self.job_runner.job_data.upsert_info(info)

        # Run the job
        self.job_runner.run(job_type)

        # Check job and run
        job_id = info['RowKey']
        self.validate_info(job_id, ['status', 'revision'], [JobStatus.Suspended, revision], [], ['last_processed', 'result', 'processed'], ['103', 306, 3])
        self.validate_run(job_id, 1, ['end_status', 'is_error', 'message'], [JobStatus.Suspended, True, 'Job failed with error: Invalid result'])

    def test_existing_tester_job_fail_with_max_consecutive_failures(self):
        # Create a job info
        job_type = 'TestJob1'
        current_time = datetime.now(timezone.utc)
        revision = 2
        settings = self.job_runner.settings_factory.create(job_type)
        info = settings.create_info(revision, current_time)
        self.job_runner.job_data.upsert_info(info)
        # Maximum consecutive failures are 2
        self.job_runner.job_data.insert_run(info['RowKey'], current_time - timedelta(hours=1), current_time - timedelta(minutes=55), 'fail1', JobStatus.Suspended, True)
        self.job_runner.job_data.insert_run(info['RowKey'], current_time - timedelta(minutes=50), current_time - timedelta(minutes=45), 'fail2', JobStatus.Suspended, True)

        # Run the job
        self.job_runner.run(job_type)

        # Check job and run
        job_id = info['RowKey']
        self.validate_info(job_id, ['status', 'revision'], [JobStatus.Failed, revision])
        self.validate_run(job_id, 2, ['end_status', 'is_error', 'message'], [JobStatus.Suspended, True, 'fail2'])

    def test_existing_tester_job_fail_with_max_failures(self):
        # Create a job info
        job_type = 'TestJob1'
        current_time = datetime.now(timezone.utc)
        revision = 3
        settings = self.job_runner.settings_factory.create(job_type)
        info = settings.create_info(revision, current_time)
        info['status'] = JobStatus.Suspended
        states = pickle.loads(info['states'])
        states['last_processed'] = '80'
        info['states'] = pickle.dumps(states)
        self.job_runner.job_data.upsert_info(info)
        # Maximum failures are 3, current only 2
        self.job_runner.job_data.insert_run(info['RowKey'], current_time - timedelta(hours=1), current_time - timedelta(minutes=55), 'fail1', JobStatus.Suspended, True)
        self.job_runner.job_data.insert_run(info['RowKey'], current_time - timedelta(minutes=50), current_time - timedelta(minutes=45), 'fail2', JobStatus.Suspended, True)
        self.job_runner.job_data.insert_run(info['RowKey'], current_time - timedelta(minutes=40), current_time - timedelta(minutes=35), 'good1', JobStatus.Suspended, False)

        # Run the job
        self.job_runner.run(job_type)

        # Check job and run
        job_id = info['RowKey']
        self.assertTrue(job_id in self.job_runner.run_with_error)
        self.validate_info(job_id, ['status', 'revision'], [JobStatus.Suspended, revision], [], ['last_processed', 'result', 'processed'], ['83', 246, 3])
        self.validate_run(job_id, 4, ['end_status', 'is_error', 'message'], [JobStatus.Suspended, True, 'Job failed with error: Invalid result'])

        # Run the job again and fail as maximum failures are reached
        self.job_runner.run(job_type)
        self.assertTrue(job_id in self.job_runner.set_failed)
        self.validate_info(job_id, ['status'], [JobStatus.Failed])
    
    def test_existing_tester_job_expire(self):
        # Create a job info
        job_type = 'TestJob1'
        current_time = datetime.now(timezone.utc)
        revision = 4
        settings = self.job_runner.settings_factory.create(job_type)
        info = settings.create_info(revision, current_time)
        info['create_time'] = current_time - timedelta(hours=25)
        self.job_runner.job_data.upsert_info(info)

        # Run the job
        self.job_runner.run(job_type)

        # Check job and run
        job_id = info['RowKey']
        self.assertTrue(job_id in self.job_runner.set_expired)
        self.validate_info(job_id, ['status', 'revision'], [JobStatus.Expired, revision])
        self.validate_run(job_id, 0, [], [])

    def test_existing_tester_job_for_current_date_ended(self):
        # Create a job info
        job_type = 'TestJob1'
        current_time = datetime.now(timezone.utc)
        revision = 5
        settings = self.job_runner.settings_factory.create(job_type)
        info = settings.create_info(revision, current_time)
        job_id = info['RowKey']
        for existing_status in [JobStatus.Completed, JobStatus.Failed, JobStatus.Expired]:
            info['status'] = existing_status
            self.job_runner.job_data.upsert_info(info)

            # Run the job
            self.job_runner.run(job_type)

            # No job is run as current date is ended
            self.assertFalse(job_id in self.job_runner.run_success)
            self.assertFalse(job_id in self.job_runner.run_with_error)
            self.assertFalse(job_id in self.job_runner.set_failed)
            self.assertFalse(job_id in self.job_runner.set_expired)

    def test_existing_tester_job_fail_with_skips(self):
        # Create a job info
        job_type = 'TestJob1'
        current_time = datetime.now(timezone.utc)
        revision = 6
        settings = self.job_runner.settings_factory.create(job_type)
        info = settings.create_info(revision, current_time)
        info['status'] = JobStatus.Suspended
        states = pickle.loads(info['states'])
        states['last_processed'] = '-3'
        info['states'] = pickle.dumps(states)
        self.job_runner.job_data.upsert_info(info)

        # Run the job
        self.job_runner.run(job_type)

        # Check job and run
        job_id = info['RowKey']
        self.validate_info(job_id, ['status', 'revision'], [JobStatus.Suspended, revision], [], ['last_processed', 'result', 'processed', 'skipped'], ['0', 0, 1, 2])
        self.validate_run(job_id, 1, ['end_status', 'is_error'], [JobStatus.Suspended, False])
    
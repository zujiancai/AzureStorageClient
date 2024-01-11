import unittest
from datetime import datetime, timedelta
import pickle
from unittest.mock import MagicMock

from batch_job.base_job import BaseJob, JobStatus, JobInfo
from tests.mock_data import MockJobData


class TestBaseJob(unittest.TestCase):
    def setUp(self):
        self.job_data = MockJobData('connection_string')
        self.job_info = JobInfo(
            PartitionKey='testjob_1001',
            RowKey='20220101_1000_testjob_1001',
            revision=0,
            inputs=pickle.dumps({'run_date': datetime(2022, 1, 1, 12, 30), 'batch_size': 1000, 'process_interval': 0}),
            states=pickle.dumps({"last_processed": "", "processed": 0, "skipped": 0}),
            status=JobStatus.Pending,
            create_time=datetime.utcnow() - timedelta(hours=1),
            update_time=datetime.utcnow())
        self.job = BaseJob(self.job_data, self.job_info)

    def test_create_keys(self):
        job_type = "testjob"
        job_version = 1
        run_date = datetime(2022, 1, 1, 12, 30)
        revision = 0
        partition_key, row_key, date = BaseJob.create_keys(job_type, job_version, run_date, revision)
        expected_partition_key = "testjob_1000001"
        expected_row_key = "20220101_1000000_testjob_1000001"
        expected_date = datetime(2022, 1, 1, 0, 0, 0)
        self.assertEqual(partition_key, expected_partition_key)
        self.assertEqual(row_key, expected_row_key)
        self.assertEqual(date, expected_date)

    def test_get_type(self):
        self.assertEqual(self.job.get_type(), "BaseJob")

    def test_check_dependencies_missing_required_job(self):
        self.job_info['status'] = JobStatus.Pending
        self.job.get_required_jobs = MagicMock(return_value={"20230101_1006_TestJob_1002": JobStatus.Active})
        self.assertFalse(self.job.check_dependencies())
        self.assertEqual(self.job.message, "Job BaseJob depends on job 20230101_1006_TestJob_1002 to be active but it does not exist.")
        self.assertEqual(self.job_info['status'], JobStatus.Pending)

    def test_check_dependencies_incorrect_required_job_status(self):
        self.job_info['status'] = JobStatus.Pending
        self.job.get_required_jobs = MagicMock(return_value={"20230101_1006_TestJob_1002": JobStatus.Completed})
        self.job_data.get_info = MagicMock(return_value={"status": JobStatus.Failed})
        self.assertFalse(self.job.check_dependencies())
        self.assertEqual(self.job.message, "Job BaseJob depends on job 20230101_1006_TestJob_1002 to be completed but it is failed.")
        self.assertEqual(self.job_info['status'], JobStatus.Pending)

    def test_internal_run_dependencies_not_met(self):
        self.job.check_dependencies = MagicMock(return_value=False)
        self.job.save_results = MagicMock(return_value=(True, ""))
        self.assertTrue(self.job.internal_run())
        self.job.check_dependencies.assert_called_once()
        self.job.save_results.assert_called_once_with(True)

    def test_internal_run_dependencies_met(self):
        self.job.check_dependencies = MagicMock(return_value=True)
        self.job.load_items = MagicMock(return_value=(True, ['item1']))
        self.job.process_item = MagicMock(return_value=True)
        self.job.post_loop = MagicMock()
        self.job.save_results = MagicMock(return_value=(True, ""))
        self.assertTrue(self.job.internal_run())
        self.job.check_dependencies.assert_called_once()
        self.job.load_items.assert_called_once_with("")
        self.job.process_item.assert_called_once()
        self.job.post_loop.assert_called_once()
        self.job.save_results.assert_called_once_with(True)

    def test_run_success(self):
        success = self.job.run()
        self.assertTrue(success)
        info = self.job_data.get_info(self.job_info['RowKey'])
        self.assertIsNotNone(info)
        self.assertEqual(info['status'], JobStatus.Completed)
        runs = self.job_data.list_runs(self.job_info['RowKey'])
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]['end_status'], JobStatus.Completed)

    def test_run_failure(self):
        self.job.load_items = MagicMock(return_value=(True, ['item1']))
        success = self.job.run()
        self.assertFalse(success)
        info = self.job_data.get_info(self.job_info['RowKey'])
        self.assertIsNotNone(info)
        self.assertEqual(info['status'], JobStatus.Suspended)
        runs = self.job_data.list_runs(self.job_info['RowKey'])
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]['is_error'], True)

import unittest
from datetime import datetime, timedelta, timezone
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
            create_time=datetime.now(timezone.utc) - timedelta(hours=1),
            update_time=datetime.now(timezone.utc))
        self.job = BaseJob(self.job_data, self.job_info)

    def test_get_type(self):
        self.assertEqual(self.job.get_type(), "BaseJob")

    def test_check_dependencies_success(self):
        self.job_info['status'] = JobStatus.Suspended
        self.job.list_expected = MagicMock(return_value=[('test_container1', 'test_blob1'), ('test_container2', 'test_blob2')])
        self.job.list_not_expected = MagicMock(return_value=[('test_container1', 'test_blob3')])
        self.assertTrue(self.job.check_dependencies(datetime.now(timezone.utc)))
        self.assertEqual(self.job_info['status'], JobStatus.Active)

    def test_check_dependencies_missing_expected_data(self):
        self.job_info['status'] = JobStatus.Pending
        self.job.list_expected = MagicMock(return_value=[('test_container1', 'test_blob3')])
        self.assertFalse(self.job.check_dependencies(datetime.now(timezone.utc)))
        self.assertEqual(self.job.message, 'Job BaseJob expects data test_container1/test_blob3 but it does not exist.')
        self.assertEqual(self.job_info['status'], JobStatus.Pending)

    def test_check_dependencies_having_unexpected_data(self):
        self.job_info['status'] = JobStatus.Suspended
        self.job.list_not_expected = MagicMock(return_value=[('test_container2', 'test_blob2')])
        self.assertFalse(self.job.check_dependencies(datetime.now(timezone.utc)))
        self.assertEqual(self.job.message, "Job BaseJob does not expect data test_container2/test_blob2 but it exists.")
        self.assertEqual(self.job_info['status'], JobStatus.Suspended)

    def test_internal_run_dependencies_not_met(self):
        self.job.check_dependencies = MagicMock(return_value=False)
        self.job.save_results = MagicMock(return_value=(True, ""))
        self.assertTrue(self.job.internal_run())
        self.job.check_dependencies.assert_called_once()

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

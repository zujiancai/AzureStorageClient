from datetime import datetime, time
import unittest

from batch_job.job_schedule import check_cron, JobSchedule


class TestJobSchedule(unittest.TestCase):
    def test_check_cron(self):
        # Test case where expression is "*"
        self.assertTrue(check_cron("*", 5))

        # Test case where expression is a single value
        self.assertTrue(check_cron("10", 10))
        self.assertFalse(check_cron("10", 5))

        # Test case where expression is a range
        self.assertTrue(check_cron("1-5", 3))
        self.assertFalse(check_cron("1-5", 7))

        # Test case where expression has a step value
        self.assertTrue(check_cron("*/2", 4))
        self.assertFalse(check_cron("*/2", 5))

        # Test case where expression has multiple segments
        self.assertTrue(check_cron("1,3,5", 3))
        self.assertFalse(check_cron("1,3,5", 2))
        self.assertFalse(check_cron("1,3-5,9", 7))
        self.assertTrue(check_cron("1,4-6,8", 5))

    def test_schedule_check(self):
        # Test case where not constraints are set
        job_schedule = JobSchedule()
        self.assertTrue(job_schedule.check(datetime(2023, 1, 1, 0, 0)))

        # Test case where all conditions are met
        job_schedule = JobSchedule().for_months("*").for_days("*/3").for_weekdays("1,3-6").after(8, 59, 59)
        self.assertTrue(job_schedule.check(datetime(2022, 1, 3, 9, 0)))

        # Test case where in_months condition is not met
        job_schedule = JobSchedule().for_months("1,2,3").for_days("1-5").for_weekdays("1-3").after(8, 0, 30)
        self.assertFalse(job_schedule.check(datetime(2022, 4, 3, 9, 0)))

        # Test case where on_days condition is not met
        job_schedule = JobSchedule().for_days("1-5").for_weekdays("1,2,3").after(8, 0, 30)
        self.assertFalse(job_schedule.check(datetime(2022, 1, 6, 9, 0)))

        # Test case where on_weekdays condition is not met
        job_schedule = JobSchedule().for_weekdays("*/3").after(8, 0)
        self.assertFalse(job_schedule.check(datetime(2022, 1, 3, 9, 0)))

        # Test case where after_time condition is not met
        job_schedule = JobSchedule().after(8, 0)
        self.assertFalse(job_schedule.check(datetime(2022, 1, 3, 7, 0)))
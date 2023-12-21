from datetime import datetime, time
import unittest

from batch_job.job_schedule import check_cron, JobSchedule, schedule_from_crontab


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

    def test_schedule_from_crontab(self):
        # Test case where expression is None
        job_schedule = schedule_from_crontab(None)
        self.assertTrue(job_schedule.check(datetime(2023, 1, 1, 0, 0)))

        # Test case where expression is empty
        job_schedule = schedule_from_crontab('')
        self.assertTrue(job_schedule.check(datetime(2023, 1, 1, 0, 0)))

        # Test case where expression is a single value
        job_schedule = schedule_from_crontab("10 10 10 10 *")
        self.assertTrue(job_schedule.check(datetime(2023, 10, 10, 10, 10)))
        self.assertFalse(job_schedule.check(datetime(2023, 10, 10, 10, 9)))

        # Test case where expression is a range
        job_schedule = schedule_from_crontab("15 2 1-5 1-5 1-5")
        self.assertTrue(job_schedule.check(datetime(2023, 3, 3, 3, 3))) # 3/3/2023 is a Friday
        self.assertFalse(job_schedule.check(datetime(2023, 3, 4, 3, 3))) # 3/4/2023 is a Saturday

        # Test case where expression has a step value
        job_schedule = schedule_from_crontab("2 2 */2 */2 *")
        self.assertTrue(job_schedule.check(datetime(2023, 6, 4, 3, 3)))
        self.assertFalse(job_schedule.check(datetime(2023, 6, 3, 3, 3)))

        # Test case where expression has multiple segments
        job_schedule = schedule_from_crontab("35 12 1,3,5 1,3,5 1,3,5")
        self.assertTrue(job_schedule.check(datetime(2023, 3, 3, 13, 35)))
        self.assertFalse(job_schedule.check(datetime(2023, 7, 3, 13, 35)))

        # Test case where expression has wrong hours
        with self.assertRaises(ValueError):
            schedule_from_crontab("35 25 1,3,5 1,3,5 1,3,5")

        # Test case where expression does not have enough segments
        with self.assertRaises(AssertionError):
            schedule_from_crontab("35 12 1,3,5 1,3,5")

        # Test case where expression does not support range and multiple segments in hours and minutes
        with self.assertRaises(ValueError):
            schedule_from_crontab("35-40,45 12 1,3,5 1,3,5 1,3,5")

        with self.assertRaises(ValueError):
            schedule_from_crontab("35 12,13 1,3,5 1,3,5 1,3,5")

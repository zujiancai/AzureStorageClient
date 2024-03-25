from datetime import datetime, time, timezone


def check_cron(expression: str, number: int):
    # Handle the case where the expression is "*"
    if expression == "*":
        return True

    # Split the expression by comma to handle multiple values
    segments = expression.split(',')
    for segment in segments:
        # Check if the segment contains a range (e.g., "1-3")
        if '-' in segment:
            start, end = map(int, segment.split('-'))
            if start <= number <= end:
                return True
        elif '/' in segment:
            # Check if the segment contains a step value (e.g., "*/2")
            step = int(segment.split('/')[1])
            if number % step == 0:
                return True
        else:
            # Handle single values (e.g., "5")
            if int(segment) == number:
                return True
    return False


class JobSchedule(object):
    def __init__(self, in_months = None, on_days = None, on_weekdays = None, after_time = None):
        self.in_months = in_months
        self.on_days = on_days
        self.on_weekdays = on_weekdays
        self.after_time = after_time

    def check(self, base_time: datetime = None):
        if not base_time:
            base_time = datetime.now(timezone.utc)
        if self.in_months and not check_cron(self.in_months, base_time.month):
            return False
        if self.on_days and not check_cron(self.on_days, base_time.day):
            return False
        if self.on_weekdays and not check_cron(self.on_weekdays, base_time.isoweekday()):
            return False
        if self.after_time and base_time.time() < self.after_time:
            return False
        return True
    
    def for_months(self, months: str):
        self.in_months = months
        return self
    
    def for_days(self, days: str):
        self.on_days = days
        return self
    
    def for_weekdays(self, weekdays: str):
        self.on_weekdays = weekdays
        return self
    
    def after(self, hour, minute, second = 0):
        '''
        The schedule semantics is to ensure the job will be executed after the specified time of the day, thus only exact hour/minute/second is supported.
        '''
        self.after_time = time(hour, minute, second)
        return self


def schedule_from_crontab(expression: str):
    if not expression:
        return JobSchedule()
    segments = expression.split(' ')
    assert(len(segments) == 5)
    return JobSchedule().for_months(segments[3]).for_days(segments[2]).for_weekdays(segments[4]).after(int(segments[1]), int(segments[0]))

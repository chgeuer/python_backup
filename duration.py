#!/usr/bin/env python2.7

import time
import datetime
import re

class ScheduleParser:
    @staticmethod
    def __from_atom(time):
        """
            >>> ScheduleParser._ScheduleParser__from_atom('7d')
            datetime.timedelta(7)
        """
        num = int(time[:-1])
        unit = time[-1:]
        return {
            "d": lambda d: datetime.timedelta(days=d),
            "h": lambda h: datetime.timedelta(hours=h),
            "m": lambda m: datetime.timedelta(minutes=m),
            "s": lambda s: datetime.timedelta(seconds=s)
        }[unit](num)

    @staticmethod
    def parse_timedelta(time_val):
        """
            >>> ScheduleParser.parse_timedelta('7d')
            datetime.timedelta(7)
            >>> ScheduleParser.parse_timedelta('7d 20s')
            datetime.timedelta(7, 20)
            >>> ScheduleParser.parse_timedelta('1d 1h 1m 1s')
            datetime.timedelta(1, 3661)
            >>> ScheduleParser.parse_timedelta('1d 23h 59m 59s')
            datetime.timedelta(1, 86399)
            >>> ScheduleParser.parse_timedelta('1d23h 59m 59s')
            datetime.timedelta(1, 86399)
            >>> ScheduleParser.parse_timedelta('1d 23h 59m 60s')
            datetime.timedelta(2)
        """
        no_spaces = time_val.replace(" ", "")
        atoms = re.findall(r"(\d+[dhms])", no_spaces)
        durations = map(lambda time: ScheduleParser.__from_atom(time), atoms)
        return reduce(lambda x, y: x + y, durations)

class BusinessHours:
    standard_prefix="db.backup.window"

    @staticmethod
    def __sample_data():
        return (
            "db.backup.window.1:111111 111000 000000 011111;"
            "db.backup.window.2:111111 111000 000000 011111;"
            "db.backup.window.3:111111 111000 000000 011111;"
            "db.backup.window.4:111111 111000 000000 011111;"
            "db.backup.window.5:111111 111000 000000 011111;"
            "db.backup.window.6:111111 111111 111111 111111;"
            "db.backup.window.7:111111 111111 111111 111111"
            )

    @staticmethod
    def parse_tag_str(tags_value, prefix=standard_prefix):
        """
            >>> BusinessHours.parse_tag_str(BusinessHours._BusinessHours__sample_data(), 'db.backup.window').tags['db.backup.window.1']
            '111111 111000 000000 011111'
        """
        tags = dict(kvp.split(":", 1) for kvp in (tags_value.split(";")))
        return BusinessHours(tags=tags, prefix=prefix)

    @staticmethod
    def parse_day(day_values):
        """
            >>> BusinessHours.parse_day('111111 111000 000000 011111')
            [True, True, True, True, True, True, True, True, True, False, False, False, False, False, False, False, False, False, False, True, True, True, True, True]
        """
        hour_strs = re.findall(r"([01])", day_values)
        durations = map(lambda x: {"1":True, "0":False}[x], hour_strs)
        return durations

    def __init__(self, tags, prefix=standard_prefix):
        """
            >>> sample_data = BusinessHours._BusinessHours__sample_data()
            >>> BusinessHours.parse_tag_str(sample_data).hours[1]
            [True, True, True, True, True, True, True, True, True, False, False, False, False, False, False, False, False, False, False, True, True, True, True, True]
        """
        self.tags = tags
        self.prefix = prefix
        self.hours = dict()
        for day in range(1, 8):
            x = tags["{prefix}.{day}".format(prefix=prefix, day=day)]
            self.hours[day] = BusinessHours.parse_day(x)
    
    def is_backup_allowed_dh(self, day, hour):
        """
            >>> sample_data = BusinessHours._BusinessHours__sample_data()
            >>> sample_hours = BusinessHours.parse_tag_str(sample_data)
            >>> sample_hours.is_backup_allowed_dh(day=1, hour=4)
            True
            >>> sample_hours.is_backup_allowed_dh(day=1, hour=11)
            False
            >>> sample_hours.is_backup_allowed_dh(day=7, hour=11)
            True
        """
        return self.hours[day][hour]
    
    def is_backup_allowed_time(self, time):
        """
            >>> sample_data = BusinessHours._BusinessHours__sample_data()
            >>> sample_hours = BusinessHours.parse_tag_str(sample_data)
            >>> some_tuesday_evening = Timing.parse("20180605_215959")
            >>> sample_hours.is_backup_allowed_time(some_tuesday_evening)
            True
            >>> some_tuesday_noon = Timing.parse("20180605_115500")
            >>> sample_hours.is_backup_allowed_time(some_tuesday_noon)
            False
            >>> some_sunday_noon = Timing.parse("20180610_115500")
            >>> sample_hours.is_backup_allowed_time(some_sunday_noon)
            True
        """
        # time.struct_time.tm_wday is range [0, 6], Monday is 0
        return self.is_backup_allowed_dh(day=1 + time.tm_wday, hour=time.tm_hour)

    def is_backup_allowed_now_localtime(self):
        return self.is_backup_allowed_time(time=time.localtime())

class Timing:
    time_format="%Y%m%d_%H%M%S"

    @staticmethod
    def now():
        return Timing.datetime_to_timestr(time.gmtime())

    @staticmethod
    def datetime_to_timestr(t):
        return time.strftime(Timing.time_format, t)

    @staticmethod 
    def parse(time_str):
        """
            >>> Timing.parse("20180605_215959")
            time.struct_time(tm_year=2018, tm_mon=6, tm_mday=5, tm_hour=21, tm_min=59, tm_sec=59, tm_wday=1, tm_yday=156, tm_isdst=-1)
        """
        return time.strptime(time_str, Timing.time_format)

    @staticmethod
    def timestr_to_datetime(time_str):
        t = Timing.parse(time_str)
        return datetime.datetime(
            year=t.tm_year, month=t.tm_mon, day=t.tm_mday,
            hour=t.tm_hour, minute=t.tm_min, second=t.tm_sec)

    @staticmethod
    def time_diff_in_seconds(timestr_1, timestr_2):
        diff=Timing.timestr_to_datetime(timestr_2) - Timing.timestr_to_datetime(timestr_1)
        return int(diff.total_seconds())

if __name__ == "__main__":
    import doctest
    doctest.testmod()

# coding=utf-8

import re

from .timing import Timing
from .scheduleparser import ScheduleParser
from .backupexception import BackupException

class BusinessHours(object):
    """
    Process business hour statements, such as determine wheter a certain
    point in time is within or outside business hours.
    """

    default_schedule = "bkp_db_schedule"

    def __init__(self, tags, schedule=default_schedule):
        """
        Given a dictionary of all the instance metadata tags,
        extracts the tag containing the schedule information
        and parses the values.
        """

        # Get the schedule tag and remove space
        schedule_tag = tags[schedule].replace(' ', '')

        # Parse days from tag
        self.tags = dict(d.split(':', 1) for d in schedule_tag.split(','))

        # Parse hours
        self.hours = dict()
        weekdays = ['mo', 'tu', 'we', 'th', 'fr', 'sa', 'su']

        for day in range(0, 7):
            if not self.tags.has_key(weekdays[day]):
                raise BackupException("Missing schedule for {}".format(weekdays[day]))
            self.hours[day+1] = BusinessHours.parse_day(self.tags[weekdays[day]])

        # Also retrieve min/max retention values from tag
        if not self.tags.has_key('min'):
            raise BackupException("Missing value for min in schedule {schedule}".format(
                schedule=schedule))
        self.min = ScheduleParser.parse_timedelta(self.tags['min'])

        if not self.tags.has_key('max') and schedule != "bkp_log_schedule":
            raise BackupException("Missing value for max in schedule {schedule}".format(
                schedule=schedule))
        self.max = ScheduleParser.parse_timedelta(self.tags['max'])

    @staticmethod
    def parse_tag_str(tags_value, schedule=default_schedule):
        try:
            tags = dict(kvp.split(":", 1) for kvp in tags_value.split(";"))
            return BusinessHours(tags=tags, schedule=schedule)
        except Exception as ex:
            raise BackupException("Error parsing business hours '{}': {}".format(
                tags_value, ex.message))

    @staticmethod
    def parse_day(day_values):
        try:
            hour_strs = re.findall(r"([01])", day_values)
            return [{"1":True, "0":False}[x] for x in hour_strs]
        except Exception as e:
            raise BackupException("Error parsing business hours '{}': {}".format(
                day_values, e.message))

    def is_backup_allowed_dh(self, day, hour):
        return self.hours[day][hour]

    def is_backup_allowed_time(self, time):
        # time.struct_time.tm_wday is range [0, 6], Monday is 0
        parsed_time = Timing.parse(time)
        return self.is_backup_allowed_dh(day=1 + parsed_time.tm_wday, hour=parsed_time.tm_hour)

    def is_backup_allowed_now_localtime(self):
        return self.is_backup_allowed_time(time=Timing.now_localtime())

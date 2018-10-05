# coding=utf-8

import time
import datetime
import logging

class Timing:
    time_format = "%Y%m%d_%H%M%S"

    @staticmethod
    def now_localtime(): 
        return time.strftime(Timing.time_format, time.localtime())

    @staticmethod
    def parse(time_str):
        return time.strptime(time_str, Timing.time_format)

    @staticmethod
    def time_diff(str1, str2):
        t1 = Timing.parse(str1)
        dt1 = datetime.datetime(year=t1.tm_year, month=t1.tm_mon, day=t1.tm_mday, hour=t1.tm_hour, minute=t1.tm_min, second=t1.tm_sec)
        t2 = Timing.parse(str2)
        dt2 = datetime.datetime(year=t2.tm_year, month=t2.tm_mon, day=t2.tm_mday, hour=t2.tm_hour, minute=t2.tm_min, second=t2.tm_sec)
        return dt2 - dt1

    @staticmethod
    def sort(times, selector=lambda x: x):
        return sorted(times, cmp=lambda a, b: Timing.time_diff_in_seconds(selector(b), selector(a)))

    @staticmethod
    def time_diff_in_seconds(timestr_1, timestr_2):
        return int(Timing.time_diff(timestr_1, timestr_2).total_seconds())

    @staticmethod
    def files_needed_for_recovery(times, restore_point,
                                  select_end_date=lambda x: x["end_date"],
                                  select_is_full=lambda x: x["is_full"]):
        create_tuple = lambda x: (select_end_date(x), select_is_full(x))
        end_date_from_tuple = lambda x: x[0]
        index_of_files_to_download = set()
        for x in Timing.sort(list(set(map(create_tuple, times))), end_date_from_tuple):
            x_end_date = end_date_from_tuple(x)
            x_is_full = x[1]
            x_is_before = Timing.time_diff_in_seconds(restore_point, x_end_date) <= 0
            if x_is_full and x_is_before:
                index_of_files_to_download = set()
            index_of_files_to_download.add(x)
            if not x_is_before:
                break

        files_to_download = []
        for x in Timing.sort(times, select_end_date):
            if create_tuple(x) in index_of_files_to_download:
                files_to_download.append(x)

        result = Timing.sort(files_to_download, select_end_date)
        logging.debug("Files which must be fetched for {}: {}".format(restore_point, str(result)))
        return result

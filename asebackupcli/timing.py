# coding=utf-8
# pylint: disable=c0301

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

""" Timing module."""

import time
import datetime
import logging

class Timing(object):
    """Timing class."""
    time_format = "%Y%m%d_%H%M%S"

    @staticmethod
    def now_localtime():
        """Return formatted localtime."""
        return time.strftime(Timing.time_format, time.localtime())

    @staticmethod
    def parse(time_str):
        """Parse time string."""
        return time.strptime(time_str, Timing.time_format)

    @staticmethod
    def time_diff(str1, str2):
        """Calculate time difference."""
        t1 = Timing.parse(str1)
        dt1 = datetime.datetime(year=t1.tm_year, month=t1.tm_mon, day=t1.tm_mday,
                                hour=t1.tm_hour, minute=t1.tm_min, second=t1.tm_sec)
        t2 = Timing.parse(str2)
        dt2 = datetime.datetime(year=t2.tm_year, month=t2.tm_mon, day=t2.tm_mday,
                                hour=t2.tm_hour, minute=t2.tm_min, second=t2.tm_sec)
        return dt2 - dt1

    @staticmethod
    def sort(times, selector=lambda x: x):
        """Sort by time."""
        return sorted(times, cmp=lambda a, b: Timing.time_diff_in_seconds(selector(b), selector(a)))

    @staticmethod
    def time_diff_in_seconds(timestr_1, timestr_2):
        return int(Timing.time_diff(timestr_1, timestr_2).total_seconds())

    @staticmethod
    def files_needed_for_recovery(times, restore_point,
                                  select_end_date=lambda a: a["end_date"],
                                  select_is_full=lambda f: f["is_full"]):
        """Compute which files must be fetched for a restore"""
        create_tuple = lambda a: (select_end_date(a), select_is_full(a))
        by_end_date = lambda (end_date, _is_full): end_date

        unique_set = set(map(create_tuple, times))
        sorted_set = Timing.sort(unique_set, by_end_date)

        index_of_files_to_download = set()
        for (end_date, is_full) in sorted_set:
            is_before = Timing.time_diff_in_seconds(restore_point, end_date) <= 0

            #
            # Each time we encounter a full backup which could serve
            # as start point for restore (because it is a full backup which finished before
            # our restore point), empty the collection of items
            #
            # In case we hit the last relevant TRAN backup item (is_before == true),
            # it has been added to index_of_files_to_download, so we can exit our search loop.
            #
            if is_full and is_before:
                index_of_files_to_download = set()

            #if not is_full or is_before:


                index_of_files_to_download.add((end_date, is_full))

            elif is_full and not is_before:
                break

            elif not is_full and is_before:
                index_of_files_to_download.add((end_date, is_full))

            elif not is_full and not is_before:
                index_of_files_to_download.add((end_date, is_full))
                break

        files_to_download = []
        for stripe in Timing.sort(times, select_end_date):
            if create_tuple(stripe) in index_of_files_to_download:
                files_to_download.append(stripe)

        result = Timing.sort(files_to_download, select_end_date)
        logging.debug("Files which must be fetched for %s: %s", restore_point, str(result))
        return result

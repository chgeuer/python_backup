#!/usr/bin/env python2.7

import time
import datetime
import inspect

def time_format():
    return "%Y%m%d_%H%M%S"

def previous():
    return "19720321_120000"

def now():
    return time.strftime(time_format(), time.gmtime())

def timestr_to_datetime(time_str):
    t = time.strptime(time_str, time_format())
    return datetime.datetime(
        year=t.tm_year, month=t.tm_mon, day=t.tm_mday, 
        hour=t.tm_hour, minute=t.tm_min, second=t.tm_sec)

def time_diff_in_seconds(timestr_1, timestr_2):
    return int((timestr_to_datetime(timestr_2) - timestr_to_datetime(timestr_1)).total_seconds())

def main():
    print "Seconds {}".format(time_diff_in_seconds(previous(), now()))
    print datetime.timedelta(seconds=time_diff_in_seconds(previous(), now()))

if __name__ == '__main__':
    main()

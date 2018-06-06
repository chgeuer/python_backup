#!/usr/bin/env python2.7

import sys
import os
import platform
import subprocess
import re
import glob
import argparse
import time
import calendar
import datetime
import json
import hashlib
import base64
import requests
import logging
import unittest
import socket

import pid
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob.models import ContentSettings
from azure.common import AzureMissingResourceHttpError

class ScheduleParser:
    @staticmethod
    def __from_atom(time):
        """
            >>> ScheduleParser._ScheduleParser__from_atom('7d')
            datetime.timedelta(7)
            >>> ScheduleParser._ScheduleParser__from_atom('2w')
            datetime.timedelta(14)
        """
        num = int(time[:-1])
        unit = time[-1:]
        return {
            "w": lambda w: datetime.timedelta(days=7*w),
            "d": lambda d: datetime.timedelta(days=d),
            "h": lambda h: datetime.timedelta(hours=h),
            "m": lambda m: datetime.timedelta(minutes=m),
            "s": lambda s: datetime.timedelta(seconds=s)
        }[unit](num)

    @staticmethod
    def parse_timedelta(time_val):
        """
            >>> ScheduleParser.parse_timedelta('1w 3d 2s')
            datetime.timedelta(10, 2)
            >>> ScheduleParser.parse_timedelta('7d')
            datetime.timedelta(7)
            >>> ScheduleParser.parse_timedelta('7d 20s')
            datetime.timedelta(7, 20)
            >>> ScheduleParser.parse_timedelta('1d 1h 1m 1s')
            datetime.timedelta(1, 3661)
            >>> ScheduleParser.parse_timedelta('1d 23h 59m 59s')
            datetime.timedelta(1, 86399)
            >>> ScheduleParser.parse_timedelta('1d 23h 59m 59s')
            datetime.timedelta(1, 86399)
            >>> ScheduleParser.parse_timedelta('1d 23h 59m 60s')
            datetime.timedelta(2)
        """
        no_spaces = time_val.replace(" ", "")
        atoms = re.findall(r"(\d+[wdhms])", no_spaces)
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
    def now_localtime():
        return Timing.datetime_to_timestr(time.localtime())

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
        """
            >>> Timing.time_diff_in_seconds("20180106_120000", "20180106_120010")
            10
            >>> Timing.time_diff_in_seconds("20180106_110000", "20180106_120010")
            3610
        """
        diff=Timing.timestr_to_datetime(timestr_2) - Timing.timestr_to_datetime(timestr_1)
        return int(diff.total_seconds())

class Naming:
    @staticmethod
    def backup_type_str(is_full):
        """
            >>> Naming.backup_type_str(is_full=True)
            'full'
            >>> Naming.backup_type_str(is_full=False)
            'tran'
        """
        return ({True:"full", False:"tran"})[is_full]

    @staticmethod
    def construct_filename(dbname, is_full, timestamp, stripe_index, stripe_count):
        """
            >>> Naming.construct_filename(dbname="test1db", is_full=True, timestamp=Timing.parse("20180601_112429"), stripe_index=2, stripe_count=101)
            'test1db_full_20180601_112429_S002-101.cdmp'
            >>> Naming.construct_filename(dbname="test1db", is_full=False, timestamp=Timing.parse("20180601_112429"), stripe_index=2, stripe_count=101)
            'test1db_tran_20180601_112429_S002-101.cdmp'
        """
        return "{dbname}_{type}_{ts}_S{idx:03d}-{cnt:03d}.cdmp".format(
            dbname=dbname, 
            type=Naming.backup_type_str(is_full), 
            ts=Timing.datetime_to_timestr(timestamp),
            idx=int(stripe_index), 
            cnt=int(stripe_count))

class AzureVMInstanceMetadata:
    @staticmethod
    def request_metadata(api_version="2017-12-01"):
        url="http://169.254.169.254/metadata/instance?api-version={v}".format(v=api_version)
        response = requests.get(url=url, headers={"Metadata": "true"})
        return response.json()

    @staticmethod
    def create_instance():
        """
            >>> json_meta = '{ "compute": { "subscriptionId": "724467b5-bee4-484b-bf13-d6a5505d2b51", "resourceGroupName": "backuptest", "name": "somevm", "tags":"db.backup.interval.min:24h;db.backup.interval.max:3d;log.backup.interval.min:600s;log.backup.interval.max:30m;db.backup.window.1:111111 111000 000000 011111;db.backup.window.2:111111 111000 000000 011111;db.backup.window.3:111111 111000 000000 011111;db.backup.window.4:111111 111000 000000 011111;db.backup.window.5:111111 111000 000000 011111;db.backup.window.6:111111 111111 111111 111111;db.backup.window.7:111111 111111 111111 111111" } }'
            >>> meta = AzureVMInstanceMetadata(lambda: (json.JSONDecoder()).decode(json_meta))
            >>> meta.vm_name
            'somevm'
        """
        christian_demo_machine = socket.gethostname() == "erlang" # TODO remove the local machine check here...
        if christian_demo_machine:
            return AzureVMInstanceMetadata(lambda: (json.JSONDecoder()).decode(AzureVMInstanceMetadata.__get_test_data("meta.json")))
        else:
            return AzureVMInstanceMetadata(lambda: AzureVMInstanceMetadata.request_metadata())

    @staticmethod
    def __get_test_data(filename):
        with open(filename, mode='rt') as file:
            content = file.read()
        return content

    def __init__(self, req):
        self.req = req
        first_json = self.req()
        self.subscription_id = str(first_json["compute"]["subscriptionId"])
        self.resource_group_name = str(first_json["compute"]["resourceGroupName"])
        self.vm_name = str(first_json["compute"]["name"])

    def get_tags(self):
        tags_value = str(self.req()['compute']['tags'])
        return dict(kvp.split(":", 1) for kvp in (tags_value.split(";")))

class BackupConfigurationFile:
    @staticmethod
    def read_key_value_file(filename):
        """
            >>> values = BackupConfigurationFile.read_key_value_file(filename="config.txt")
            >>> values["sap.CID"]
            'ABC'
        """
        with open(filename, mode='rt') as file:
            lines = file.readlines()
            # skip comments and empty lines
            content_lines = filter(lambda line: not re.match(r"^\s*#|^\s*$", line), lines) 
            content = dict(line.split(":", 1) for line in content_lines)
            return dict(map(lambda x: (x[0].strip(), x[1].strip()), content.iteritems()))

    def __init__(self, filename):
        self.filename = filename
    
    def get_value(self, key):
        values = BackupConfigurationFile.read_key_value_file(filename=self.filename)
        return values[key]

class BackupConfiguration:
    def __init__(self, config_filename):
        """
            >>> cfg = BackupConfiguration(config_filename="config.txt")
            >>> cfg.get_value("sap.CID")
            'ABC'
            >>> cfg.get_full_backup_interval_min()
            datetime.timedelta(1)
            >>> some_tuesday_evening = Timing.parse("20180605_215959")
            >>> cfg.get_business_hours().is_backup_allowed_time(some_tuesday_evening)
            True
            >>> cfg.get_storage_client() != None
            True
        """
        self.cfg_file = BackupConfigurationFile(filename=config_filename)
        self.instance_metadata = AzureVMInstanceMetadata.create_instance()
        self.data = {
            "sap.CID": lambda: self.cfg_file.get_value("sap.CID"),
            "sap.SID": lambda: self.cfg_file.get_value("sap.SID"),
            "azure.storage.account_name": lambda: self.cfg_file.get_value("azure.storage.account_name"),
            "azure.storage.account_key": lambda: self.cfg_file.get_value("azure.storage.account_key"),
            "azure.storage.container_name": lambda: self.cfg_file.get_value("azure.storage.container_name"),
            "db.backup.interval.min": lambda: ScheduleParser.parse_timedelta(self.instance_metadata.get_tags()["db.backup.interval.min"]),
            "db.backup.interval.max": lambda: ScheduleParser.parse_timedelta(self.instance_metadata.get_tags()["db.backup.interval.max"]),
            "log.backup.interval.min": lambda: ScheduleParser.parse_timedelta(self.instance_metadata.get_tags()["log.backup.interval.min"]),
            "log.backup.interval.max": lambda: ScheduleParser.parse_timedelta(self.instance_metadata.get_tags()["log.backup.interval.max"]),
            "backup.businesshours": lambda: BusinessHours(self.instance_metadata.get_tags())
        }

    def get_value(self, key):
        return self.data[key]()
    def get_full_backup_interval_min(self):
        return self.get_value("db.backup.interval.min")
    def get_full_backup_interval_max(self):
        return self.get_value("db.backup.interval.max")
    def get_tran_backup_interval_min(self):
        return self.get_value("log.backup.interval.min")
    def get_tran_backup_interval_max(self):
        return self.get_value("log.backup.interval.max")
    def get_business_hours(self):
        return self.get_value("backup.businesshours")
    def get_storage_client(self):
        block_blob_service = BlockBlobService(
            account_name=self.get_value("azure.storage.account_name"), 
            account_key=self.get_value("azure.storage.account_key"))
        _created = block_blob_service.create_container(container_name=self.get_value("azure.storage.container_name"))
        return block_blob_service

if __name__ == "__main__":
    import doctest
    doctest.testmod()

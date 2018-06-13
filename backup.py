#!/usr/bin/env python2.7

from __future__ import print_function
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
from itertools import groupby

import pid
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob.models import ContentSettings
from azure.common import AzureMissingResourceHttpError

def printe(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

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
            Parses a time delta.

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
            >>> ScheduleParser.parse_timedelta('1d 23h 59m 60s')
            datetime.timedelta(2)
        """
        no_spaces = time_val.replace(" ", "")
        atoms = re.findall(r"(\d+[wdhms])", no_spaces)
        durations = map(lambda time: ScheduleParser.__from_atom(time), atoms)
        return reduce(lambda x, y: x + y, durations)

class BusinessHours:
    standard_prefix="db_backup_window"

    @staticmethod
    def __sample_data():
        return (
            "db_backup_window_1:111111 111000 000000 011111;"
            "db_backup_window_2:111111 111000 000000 011111;"
            "db_backup_window_3:111111 111000 000000 011111;"
            "db_backup_window_4:111111 111000 000000 011111;"
            "db_backup_window_5:111111 111000 000000 011111;"
            "db_backup_window_6:111111 111111 111111 111111;"
            "db_backup_window_7:111111 111111 111111 111111"
            )

    @staticmethod
    def parse_tag_str(tags_value, prefix=standard_prefix):
        """
            >>> BusinessHours.parse_tag_str(BusinessHours._BusinessHours__sample_data(), 'db_backup_window').tags['db_backup_window_1']
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
            x = tags["{prefix}_{day}".format(prefix=prefix, day=day)]
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
        return self.is_backup_allowed_time(time=Timing.now_localtime())

class Timing:
    time_format="%Y%m%d_%H%M%S"

    @staticmethod
    def now_localtime(): return time.localtime()

    @staticmethod
    def now_localtime_string(): return Timing.datetime_to_timestr(time.localtime())

    @staticmethod
    def datetime_to_timestr(t): return time.strftime(Timing.time_format, t)

    @staticmethod 
    def parse(time_str):
        """
            >>> Timing.parse("20180605_215959")
            time.struct_time(tm_year=2018, tm_mon=6, tm_mday=5, tm_hour=21, tm_min=59, tm_sec=59, tm_wday=1, tm_yday=156, tm_isdst=-1)
        """
        return time.strptime(time_str, Timing.time_format)

    @staticmethod
    def time_diff(time_1_str, time_2_str):
        """
            >>> Timing.time_diff("20180106_120000", "20180106_120010")
            datetime.timedelta(0, 10)
            >>> Timing.time_diff("20180106_110000", "20180106_120010")
            datetime.timedelta(0, 3610)
        """
        t1 = Timing.parse(time_1_str)
        dt1 = datetime.datetime(year=t1.tm_year, month=t1.tm_mon, day=t1.tm_mday, hour=t1.tm_hour, minute=t1.tm_min, second=t1.tm_sec)
        t2 = Timing.parse(time_2_str)
        dt2 = datetime.datetime(year=t2.tm_year, month=t2.tm_mon, day=t2.tm_mday, hour=t2.tm_hour, minute=t2.tm_min, second=t2.tm_sec)
        return dt2 - dt1

    @staticmethod
    def sort(times):
        """
            >>> Timing.sort(['20180110_120000', '20180105_120000', '20180101_120000'])
            ['20180101_120000', '20180105_120000', '20180110_120000']
            >>> Timing.sort(['20180105_120000', '20180110_120000', '20180105_120000', '20180101_120000'])
            ['20180101_120000', '20180105_120000', '20180105_120000', '20180110_120000']
        """
        return sorted(times, cmp=lambda a, b: Timing.time_diff_in_seconds(b, a))

    @staticmethod
    def time_diff_in_seconds(timestr_1, timestr_2):
        """
            >>> Timing.time_diff_in_seconds("20180106_120000", "20180106_120010")
            10
            >>> Timing.time_diff_in_seconds("20180106_110000", "20180106_120010")
            3610
        """
        return int(Timing.time_diff(timestr_1, timestr_2).total_seconds())

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
    def type_str_is_full(type_str):
        """
            >>> Naming.type_str_is_full('full')
            True
            >>> Naming.type_str_is_full('tran')
            False
        """
        return ({"full":True, "tran":False})[type_str]

    @staticmethod
    def construct_filename(dbname, is_full, start_timestamp, stripe_index, stripe_count):
        """
            >>> Naming.construct_filename(dbname="test1db", is_full=True, start_timestamp=Timing.parse("20180601_112429"), stripe_index=2, stripe_count=101)
            'test1db_full_20180601_112429_S002-101.cdmp'
            >>> Naming.construct_filename(dbname="test1db", is_full=False, start_timestamp=Timing.parse("20180601_112429"), stripe_index=2, stripe_count=101)
            'test1db_tran_20180601_112429_S002-101.cdmp'
        """
        return "{dbname}_{type}_{start_timestamp}_S{idx:03d}-{cnt:03d}.cdmp".format(
            dbname=dbname, 
            type=Naming.backup_type_str(is_full), 
            start_timestamp=Timing.datetime_to_timestr(start_timestamp),
            idx=int(stripe_index), 
            cnt=int(stripe_count))

    @staticmethod
    def local_filesystem_name(directory, dbname, is_full, start_timestamp, stripe_index, stripe_count):
        file_name = Naming.construct_filename(
            dbname, is_full, start_timestamp, stripe_index, stripe_count)
        return os.path.join(directory, file_name)

    @staticmethod
    def construct_blobname_prefix(dbname, is_full):
        """
            >>> Naming.construct_blobname_prefix(dbname="test1db", is_full=True)
            'test1db_full_'
        """
        return "{dbname}_{type}_".format(dbname=dbname, type=Naming.backup_type_str(is_full))

    @staticmethod
    def construct_blobname(dbname, is_full, start_timestamp, end_timestamp, stripe_index, stripe_count):
        """
            >>> Naming.construct_blobname(dbname="test1db", is_full=True, start_timestamp=Timing.parse("20180601_112429"), end_timestamp=Timing.parse("20180601_131234"), stripe_index=2, stripe_count=101)
            'test1db_full_20180601_112429--20180601_131234_S002-101.cdmp'
        """
        return "{dbname}_{type}_{start}--{end}_S{idx:03d}-{cnt:03d}.cdmp".format(
            dbname=dbname, 
            type=Naming.backup_type_str(is_full), 
            start=Timing.datetime_to_timestr(start_timestamp),
            end=Timing.datetime_to_timestr(end_timestamp),
            idx=int(stripe_index), 
            cnt=int(stripe_count))

    @staticmethod
    def parse_filename(filename):
        """
            >>> Naming.parse_filename('test1db_full_20180601_112429_S002-101.cdmp')
            ('test1db', True, '20180601_112429', 2, 101)
            >>> Naming.parse_filename('test1db_tran_20180601_112429_S02-08.cdmp')
            ('test1db', False, '20180601_112429', 2, 8)
        """
        m=re.search(r'(?P<dbname>\S+?)_(?P<type>full|tran)_(?P<start>\d{8}_\d{6})_S(?P<idx>\d+)-(?P<cnt>\d+)\.cdmp', filename)
        return (m.group('dbname'), Naming.type_str_is_full(m.group('type')), m.group('start'), int(m.group('idx')), int(m.group('cnt')))

    @staticmethod
    def parse_blobname(filename):
        """
            >>> Naming.parse_blobname('test1db_full_20180601_112429--20180601_131234_S002-101.cdmp')
            ('test1db', True, '20180601_112429', '20180601_131234', 2, 101)
            >>> Naming.parse_blobname('test1db_tran_20180601_112429--20180601_131234_S2-008.cdmp')
            ('test1db', False, '20180601_112429', '20180601_131234', 2, 8)
        """
        m=re.search(r'(?P<dbname>\S+?)_(?P<type>full|tran)_(?P<start>\d{8}_\d{6})--(?P<end>\d{8}_\d{6})_S(?P<idx>\d+)-(?P<cnt>\d+)\.cdmp', filename)

        (dbname, is_full, start_date, end_date, stripe_index, stripe_count) = (m.group('dbname'), Naming.type_str_is_full(m.group('type')), m.group('start'), m.group('end'), int(m.group('idx')), int(m.group('cnt')))

        return dbname, is_full, start_date, end_date, stripe_index, stripe_count

    @staticmethod
    def blobname_to_filename(blobname):
        parts = Naming.parse_blobname(blobname)
        return Naming.construct_filename(
                    dbname=parts[0],
                    is_full=parts[1],
                    start_timestamp=Timing.parse(parts[2]),
                    # skip parts[3] which is end-timestamp
                    stripe_index=parts[4],
                    stripe_count=parts[5])

class BackupBlobName:
    def __init__(self, blobname):
        self.blobname = blobname
        parts = Naming.parse_blobname(self.blobname)
        self.dbname = parts[0]
        self.is_full = parts[1]
        self.start_timestamp = parts[2]
        self.end_timestamp = parts[3]
        self.stripe_index = parts[4]
        self.stripe_count = parts[5]

class DevelopmentSettings:
    @staticmethod
    def is_christians_developer_box():
        return socket.gethostname() == "erlang"

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
        if DevelopmentSettings.is_christians_developer_box():
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

    def json(self):
        return self.req()

    def get_tags(self):
        tags_value = str(self.json()['compute']['tags'])
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
            >>> cfg.get_db_backup_interval_min()
            datetime.timedelta(1)
            >>> some_tuesday_evening = Timing.parse("20180605_215959")
            >>> cfg.get_business_hours().is_backup_allowed_time(some_tuesday_evening)
            True
        """
        self.cfg_file = BackupConfigurationFile(filename=config_filename)
        self.instance_metadata = AzureVMInstanceMetadata.create_instance()
        self._block_blob_service = None

        #
        # This dict contains function callbacks (lambdas) to return the value based on the current value
        #
        self.data = {
            "sap.CID": lambda: self.cfg_file.get_value("sap.CID"),
            "sap.SID": lambda: self.cfg_file.get_value("sap.SID"),

            "vm_name": lambda: self.instance_metadata.vm_name,
            "subscription_id": lambda: self.instance_metadata.subscription_id,
            "resource_group_name": lambda: self.instance_metadata.resource_group_name,

            "azure.storage.account_name": lambda: self.cfg_file.get_value("azure.storage.account_name"),
            "azure.storage.account_key": lambda: self.cfg_file.get_value("azure.storage.account_key"),
            "azure.storage.container_name": lambda: self.cfg_file.get_value("azure.storage.container_name"),

            "db_backup_interval_min": lambda: ScheduleParser.parse_timedelta(self.instance_metadata.get_tags()["db_backup_interval_min"]),
            "db_backup_interval_max": lambda: ScheduleParser.parse_timedelta(self.instance_metadata.get_tags()["db_backup_interval_max"]),
            "log_backup_interval_min": lambda: ScheduleParser.parse_timedelta(self.instance_metadata.get_tags()["log_backup_interval_min"]),
            "log_backup_interval_max": lambda: ScheduleParser.parse_timedelta(self.instance_metadata.get_tags()["log_backup_interval_max"]),
            "backup.businesshours": lambda: BusinessHours(self.instance_metadata.get_tags())
        }

    def get_value(self, key): return self.data[key]()
    def get_vm_name(self): return self.get_value("vm_name")
    def get_subscription_id(self): return self.get_value("subscription_id")
    def get_resource_group_name(self): return self.get_value("resource_group_name")
    def get_CID(self): return self.get_value("sap.CID")
    def get_SID(self): return self.get_value("sap.SID")
    def get_db_backup_interval_min(self): return self.get_value("db_backup_interval_min")
    def get_db_backup_interval_max(self): return self.get_value("db_backup_interval_max")
    def get_log_backup_interval_min(self): return self.get_value("log_backup_interval_min")
    def get_log_backup_interval_max(self): return self.get_value("log_backup_interval_max")
    def get_business_hours(self): return self.get_value("backup.businesshours")
    def get_databases_to_skip(self): return [ "dbccdb" ]

    def __get_azure_storage_account_name(self): return self.get_value("azure.storage.account_name")
    def __get_azure_storage_account_key(self): return self.get_value("azure.storage.account_key")

    @property 
    def azure_storage_container_name(self): return self.get_value("azure.storage.container_name")

    @property
    def storage_client(self):
        if not self._block_blob_service:
            self._block_blob_service = BlockBlobService(
                account_name=self.__get_azure_storage_account_name(), 
                account_key=self.__get_azure_storage_account_key())
            _created = self._block_blob_service.create_container(container_name=self.azure_storage_container_name)
        return self._block_blob_service

    def get_standard_local_directory(self):
        return "/tmp"

class DatabaseConnector:
    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration

    def get_database_password(self, sid):
        sid = self.backup_configuration.get_SID()
        executable = "/sybase/{sid}/dba/bin/dbsp".format(sid=sid)
        arg = "***REMOVED***"
        stdout, _stderr = DatabaseConnector.call_process(command_line=[executable, arg], stdin="")
        return str(stdout).strip()

    @staticmethod
    def sql_statement_stripe_count(dbname, is_full):
        return "\n".join([
            "set nocount on",
            "go",
            "declare @dbname varchar(30),",
            "        @dumptype varchar(12),",
            "        @stripes int,",
            "        @data_free numeric (10,2),",
            "        @data_size numeric (10,2),",
            "        @log_free numeric (10,2),",
            "        @log_size numeric (10,2),",
            "        @max_stripe_size_in_GB int",
            "",
            "select @dbname = '{dbname}'".format(dbname=dbname),
            "select @stripes = 0",
            "select @max_stripe_size_in_GB = 10",
            "select",
            "    @data_free = convert(numeric(10,2),sum(curunreservedpgs(dbid, lstart, unreservedpgs)) * (@@maxpagesize / 1024. / 1024)),",
            "    @data_size = convert(numeric(10,2),sum(u.size * (@@maxpagesize / 1024. / 1024.))),",
            "    @log_free = convert(numeric(10,2),lct_admin('logsegment_freepages', u.dbid) * (@@maxpagesize / 1024. /1024. ))",
            "    from master..sysusages u, master..sysdevices d",
            "    where d.vdevno = u.vdevno",
            "        and d.status &2 = 2",
            "        and u.segmap <> 4",
            "        and u.segmap < 5",
            "        and db_name(u.dbid) = @dbname",
            "    group by u.dbid",
            "select @log_size =  sum(us.size * (@@maxpagesize / 1024. / 1024.))",
            "    from master..sysdatabases db, master..sysusages us",
            "    where db.dbid = us.dbid",
            "        and us.segmap = 4",
            "        and db_name(db.dbid) = @dbname",
            "    group by db.dbid",
            "select @data_free = isnull (@data_free, 0),",
            "       @data_size = isnull (@data_size ,0),",
            "       @log_free  = isnull (@log_free, 0),",
            "       @log_size  = isnull (@log_size, 0)"
        ]
        +
        {
            True: [
                "select @stripes = convert (int, ((@data_size - @data_free + @log_size - @log_free) / 1024 + @max_stripe_size_in_GB ) / @max_stripe_size_in_GB)",
                "if(( @stripes < 2 ) and ( @data_size - @data_free + @log_size - @log_free > 1024 ))"
                ],
            False: [
                "select @stripes = convert (int, ((@log_size - @log_free) / 1024 + @max_stripe_size_in_GB ) / @max_stripe_size_in_GB)",
                "if(( @stripes < 2 ) and ( @log_size - @log_free > 1024 ))"
            ]
        }[is_full]
        +
        [
            "begin",
            "    select @stripes = 2",
            "end",
            "",
            "if @stripes > 8",
            "begin",
            "    select @stripes = 8",
            "end",
            "",
            "select @stripes",
            "go"
        ])

    def determine_database_backup_stripe_count(self, dbname, is_full):
        (stdout, _stderr) = DatabaseConnector.call_process(
            command_line=self.isql(),
            stdin=DatabaseConnector.sql_statement_stripe_count(dbname=dbname, is_full=is_full))
        return int(stdout)

    @staticmethod
    def sql_statement_list_databases(is_full):
        return "\n".join(
            [
                "set nocount on",
                "go",
                "select name, status, status2 into #dbname",
                "    from master..sysdatabases",
                "    where dbid <> 2 and status3 & 256 = 0"
            ]
            +
            {
                False:[
                    "delete from #dbname where status2 & 16 = 16 or status2 & 32 = 32 or status & 8 = 8",
                    "delete from #dbname where tran_dumpable_status(name) <> 0"
                ],
                True: []
            }[is_full]
            +
            [
                "declare @inputstrg varchar(1000)",
                "declare @delim_pos int",
                "select @inputstrg = ''",
                "if char_length(@inputstrg) > 1",
                "begin",
                "    create table #selected_dbs(sequence int identity, dbname varchar(50))",
                "    while char_length(@inputstrg) > 0",
                "    begin",
                "        select @delim_pos = charindex(',', @inputstrg)",
                "        if @delim_pos = 0",
                "        begin",
                "            select @delim_pos = char_length(@inputstrg) + 1",
                "        end",
                "        insert into #selected_dbs(dbname) select substring(@inputstrg, 1, @delim_pos - 1)",
                "        select @inputstrg = substring(@inputstrg, @delim_pos + 1, char_length(@inputstrg))",
                "    end",
                "    delete from #dbname where name not in (select dbname from #selected_dbs)",
                "end",
                "select name from #dbname order by 1",
                "go"
            ])

    def list_databases(self, is_full):
        # print("Listing databases CID={cid} SID={sid}".format(
        #     cid=self.backup_configuration.get_CID(),
        #     sid=self.backup_configuration.get_SID()))
        # print("Calling {}".format(" ".join(self.isql())))
        # print(DatabaseConnector.sql_statement_list_databases(is_full=is_full))

        (stdout, _stderr) = DatabaseConnector.call_process(
            command_line=self.isql(),
            stdin=DatabaseConnector.sql_statement_list_databases(is_full=is_full))

        return filter(
            lambda e: e != "", 
            map(
                lambda s: s.strip(), 
                stdout.split("\n")))

    @staticmethod
    def sql_statement_create_backup(dbname, is_full, start_timestamp, stripe_count, output_dir):
        """
            >>> print(DatabaseConnector.sql_statement_create_backup(output_dir="/tmp", dbname="AZU", is_full=True, start_timestamp=Timing.parse("20180629_124500"), stripe_count=1))
            use master
            go
            sp_dboption AZU, 'trunc log on chkpt', 'false'
            go
            dump database AZU to '/tmp/AZU_full_20180629_124500_S001-001.cdmp'
            with compression = '101'
            go

            >>> print(DatabaseConnector.sql_statement_create_backup(output_dir="/tmp", dbname="AZU", is_full=True, start_timestamp=Timing.parse("20180629_124500"), stripe_count=4))
            use master
            go
            sp_dboption AZU, 'trunc log on chkpt', 'false'
            go
            dump database AZU to '/tmp/AZU_full_20180629_124500_S001-004.cdmp'
                stripe on '/tmp/AZU_full_20180629_124500_S002-004.cdmp'
                stripe on '/tmp/AZU_full_20180629_124500_S003-004.cdmp'
                stripe on '/tmp/AZU_full_20180629_124500_S004-004.cdmp'
            with compression = '101'
            go

            >>> print(DatabaseConnector.sql_statement_create_backup(output_dir="/tmp", dbname="AZU", is_full=False, start_timestamp=Timing.parse("20180629_124500"), stripe_count=1))
            use master
            go
            dump transaction AZU to '/tmp/AZU_tran_20180629_124500_S001-001.cdmp'
            with compression = '101'
            go

            >>> print(DatabaseConnector.sql_statement_create_backup(output_dir="/tmp", dbname="AZU", is_full=False, start_timestamp=Timing.parse("20180629_124500"), stripe_count=4))
            use master
            go
            dump transaction AZU to '/tmp/AZU_tran_20180629_124500_S001-004.cdmp'
                stripe on '/tmp/AZU_tran_20180629_124500_S002-004.cdmp'
                stripe on '/tmp/AZU_tran_20180629_124500_S003-004.cdmp'
                stripe on '/tmp/AZU_tran_20180629_124500_S004-004.cdmp'
            with compression = '101'
            go
        """

        files = map(lambda stripe_index: 
            Naming.local_filesystem_name(
                directory=output_dir, 
                dbname=dbname, 
                is_full=is_full, 
                start_timestamp=start_timestamp, 
                stripe_index=stripe_index, 
                stripe_count=stripe_count), range(1, stripe_count + 1))

        return "\n".join(
            [
                "use master",
                "go"
            ]
            +
            {
                False: [],
                True: [
                    "sp_dboption {dbname}, 'trunc log on chkpt', 'false'".format(dbname=dbname),
                    "go"
                ]
            }[is_full]
            +
            [
                "dump {type} {dbname} to {file_names}".format(
                    type={True:"database", False:"transaction"}[is_full],
                    dbname=dbname,
                    file_names="\n    stripe on ".join(
                        map(lambda fn: "'{fn}'".format(fn=fn), files)
                    )
                ),
                "with compression = '101'",
                "go"
            ]
        )

    def create_backup(self, dbname, is_full, start_timestamp, stripe_count, output_dir):
        sql = DatabaseConnector.sql_statement_create_backup(
                dbname=dbname, is_full=is_full, 
                start_timestamp=start_timestamp, 
                stripe_count=stripe_count,
                output_dir=output_dir)

        return DatabaseConnector.call_process(command_line=self.isql(), stdin=sql)

    @staticmethod
    def isql_path():
        return "/opt/sap/OCS-16_0/bin/isql"

    @staticmethod
    def create_isql_commandline(server_name, username, password):
        supress_header = "-b"
        return [
            DatabaseConnector.isql_path(),
            "-S", server_name,
            "-U", username,
            "-P", password,
            "-w", "999",
            supress_header
        ]

    def isql(self):
        return DatabaseConnector.create_isql_commandline(
            server_name=self.backup_configuration.get_CID(),
            username="sapsa",
            password=self.get_database_password(
                sid=self.backup_configuration.get_SID()))

    @staticmethod
    def ddlgen_path():
        return "/opt/sap/ASE-16_0/bin/ddlgen"

    @staticmethod
    def create_ddlgen_commandline(sid, dbname, username, password):
        # ddlgen -Usapsa -S${SID} -D${DB} -P${SAPSA_PWD} -F% -TDBD -N%
        # ddlgen -Usapsa -S${SID} -D${DB} -P${SAPSA_PWD} -F%
        return [
            DatabaseConnector.ddlgen_path(),
            "-U{}".format(username),
            "-S{}".format(sid),
            "-D{}".format(dbname),
            "-P{}".format(password)
        ]

    def ddlgen(self, dbname):
        return DatabaseConnector.create_ddlgen_commandline(
            sid=self.backup_configuration.get_SID(),
            dbname=dbname,
            username="sapsa",
            password=self.get_database_password(sid=self.backup_configuration.get_SID()))

    @staticmethod
    def get_ase_environment():
        ase_env = os.environ.copy()

        p=lambda path: os.path.join("/opt/sap", path)
        val=lambda name: ase_env.get(name, "")

        jre7=p("shared/SAPJRE-7_1_049_64BIT")
        jre8=p("shared/SAPJRE-8_1_029_64BIT")

        ase_env["SAP_JRE7"]=jre7
        ase_env["SAP_JRE7_64"]=jre7
        ase_env["SYBASE_JRE_RTDS"]=jre7
        ase_env["SAP_JRE8"]=jre8
        ase_env["SAP_JRE8_64"]=jre8
        ase_env["COCKPIT_JAVA_HOME"]=jre8
        ase_env["SYBASE"]=p("")
        ase_env["SYBROOT"]=p("")
        ase_env["SYBASE_OCS"]="OCS-16_0"
        ase_env["SYBASE_ASE"]="ASE-16_0"
        ase_env["SYBASE_WS"]="WS-16_0"

        ase_env["INCLUDE"] = os.pathsep.join([
            p("OCS-16_0/include"),
            val("INCLUDE")
        ])

        ase_env["LIB"] = os.pathsep.join([
            p("OCS-16_0/lib"),
            val("LIB")
        ])

        ase_env["LD_LIBRARY_PATH"] = os.pathsep.join([
            p("ASE-16_0/lib"),
            p("OCS-16_0/lib"),
            p("OCS-16_0/lib3p"),
            p("OCS-16_0/lib3p64"),
            p("DataAccess/ODBC/lib"),
            p("DataAccess64/ODBC/lib"),
            val("LD_LIBRARY_PATH")
        ])

        ase_env["PATH"] = os.pathsep.join([
            p("ASE-16_0/bin"),
            p("ASE-16_0/install"),
            p("ASE-16_0/jobscheduler/bin"),
            p("OCS-16_0/bin"),
            p("COCKPIT-4/bin"),
            val("PATH")
         ])

        if ase_env.has_key("LANG"):
            del(ase_env["LANG"])

        return ase_env

    @staticmethod
    def call_process(command_line, stdin):
        p = subprocess.Popen(
            command_line,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=DatabaseConnector.get_ase_environment()
        )
        stdout, stderr = p.communicate(stdin)
        return (stdout, stderr)

class BackupAgent:
    """
        The concrete backup business logic
    """
    def __init__(self, config_filename):
        self.backup_configuration = BackupConfiguration(config_filename)

    def existing_backups_for_db(self, dbname, is_full):
        existing_blobs_dict = dict()
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                prefix=Naming.construct_blobname_prefix(dbname=dbname, is_full=is_full), 
                marker=marker)
            for blob in results:
                blob_name=blob.name
                parts = Naming.parse_blobname(blob_name)
                end_time_of_existing_blob = parts[3]
                if not existing_blobs_dict.has_key(end_time_of_existing_blob):
                    existing_blobs_dict[end_time_of_existing_blob] = []
                existing_blobs_dict[end_time_of_existing_blob].append(blob_name)

            if results.next_marker:
                marker = results.next_marker
            else:
                break
        return existing_blobs_dict

    def existing_backups(self, databases=[]):
        existing_blobs_dict = dict()
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                marker=marker)

            for blob in results:
                blob_name=blob.name
                parts = Naming.parse_blobname(blob_name)
                end_time_of_existing_blob = parts[3]
                if len(databases) == 0 or parts[0] in databases:
                    if not existing_blobs_dict.has_key(end_time_of_existing_blob):
                        existing_blobs_dict[end_time_of_existing_blob] = []
                    existing_blobs_dict[end_time_of_existing_blob].append(blob_name)

            if results.next_marker:
                marker = results.next_marker
            else:
                break
        return existing_blobs_dict

    def latest_backup_timestamp(self, dbname, is_full):
        existing_blobs_dict = self.existing_backups_for_db(dbname=dbname, is_full=is_full)
        if len(existing_blobs_dict.keys()) == 0:
            return "19000101_000000"
        return sorted(existing_blobs_dict.keys(), cmp=Timing.sort)[-1:][0]

    def full_backup(self, force=False, skip_upload=False, output_dir=None, databases=None):
        database_connector = DatabaseConnector(self.backup_configuration)
        if databases != None:
            databases_to_backup = databases.split(",")
        else:
            databases_to_backup = database_connector.list_databases(is_full=True)

        skip_dbs = self.backup_configuration.get_databases_to_skip()
        databases_to_backup = filter(lambda db: not (db in skip_dbs), databases_to_backup)

        if output_dir == None:
            output_dir = self.backup_configuration.get_standard_local_directory()

        for dbname in databases_to_backup:
            self.full_backup_single_db(
                dbname=dbname, 
                force=force, 
                skip_upload=skip_upload, 
                output_dir=output_dir)

    @staticmethod
    def should_run_full_backup(now_time, force, latest_full_backup_timestamp, business_hours, db_backup_interval_min, db_backup_interval_max):
        """
            Determine whether a backup should be executed. 

            >>> business_hours=BusinessHours.parse_tag_str(BusinessHours._BusinessHours__sample_data())
            >>> db_backup_interval_min=ScheduleParser.parse_timedelta("24h")
            >>> db_backup_interval_max=ScheduleParser.parse_timedelta("3d")
            >>> five_day_backup =                     "20180601_010000"
            >>> two_day_backup =                      "20180604_010000"
            >>> same_day_backup =                     "20180606_010000"
            >>> during_business_hours  = Timing.parse("20180606_150000")
            >>> outside_business_hours = Timing.parse("20180606_220000")
            >>> 
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=True, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=True, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=True, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # respecting business hours, and not needed anyway
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=False, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            False
            >>> # respecting business hours
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=False, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            False
            >>> # a really old backup, so we ignore business hours
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=False, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # outside_business_hours, but same_day_backup, so no backup
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=False, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            False
            >>> # outside_business_hours and need to backup
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=False, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # a really old backup
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=False, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=True, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=True, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=True, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
        """
        allowed_by_business = business_hours.is_backup_allowed_time(now_time)
        age_of_latest_backup_in_storage = Timing.time_diff(latest_full_backup_timestamp, Timing.datetime_to_timestr(now_time))
        min_interval_allows_backup = age_of_latest_backup_in_storage > db_backup_interval_min
        max_interval_requires_backup = age_of_latest_backup_in_storage > db_backup_interval_max
        perform_full_backup = (allowed_by_business and min_interval_allows_backup or max_interval_requires_backup or force)

        # logging.info("Full backup requested. Current time: {now}. Last backup in storage: {last}. Age of backup {age}".format(now=Timing.datetime_to_timestr(now_time), last=latest_full_backup_timestamp, age=age_of_latest_backup_in_storage))
        # logging.info("Backup requirements: min=\"{min}\" max=\"{max}\"".format(min=db_backup_interval_min,max=db_backup_interval_max))
        # logging.info("Forced by user: {force}. Backup allowed by business hours: {allowed_by_business}. min_interval_allows_backup={min_interval_allows_backup}. max_interval_requires_backup={max_interval_requires_backup}".format(force=force, allowed_by_business=allowed_by_business, min_interval_allows_backup=min_interval_allows_backup, max_interval_requires_backup=max_interval_requires_backup))
        # logging.info("Decision to backup: {perform_full_backup}.".format(perform_full_backup=perform_full_backup))

        return perform_full_backup

    def full_backup_single_db(self, dbname, force, skip_upload, output_dir):
        if not BackupAgent.should_run_full_backup(
                now_time=Timing.now_localtime(), force=force, 
                latest_full_backup_timestamp=self.latest_backup_timestamp(dbname=dbname, is_full=True),
                business_hours=self.backup_configuration.get_business_hours(), 
                db_backup_interval_min=self.backup_configuration.get_db_backup_interval_min(), 
                db_backup_interval_max=self.backup_configuration.get_db_backup_interval_max()):
            logging.info("Skipping backup of database {dbname}".format(dbname=dbname))
            print("Skipping backup of database {dbname}".format(dbname=dbname))
            return

        db_connector = DatabaseConnector(self.backup_configuration)
        stripe_count = db_connector.determine_database_backup_stripe_count(
            dbname=dbname, is_full=True)

        start_timestamp = Timing.now_localtime()
        stdout, stderr = db_connector.create_backup(
            dbname=dbname, 
            is_full=True,
            start_timestamp=start_timestamp, 
            stripe_count=stripe_count, 
            output_dir=output_dir)
        end_timestamp = Timing.now_localtime()

        logging.info(stdout)
        logging.warning(stderr)
        # print(stdout)
        # printe(stderr)

        if not skip_upload:
            for stripe_index in range(1, stripe_count + 1):
                file_name = Naming.construct_filename(
                    dbname=dbname, 
                    is_full=True, 
                    start_timestamp=start_timestamp,
                    stripe_index=stripe_index, 
                    stripe_count=stripe_count)
                blob_name = Naming.construct_blobname(
                    dbname=dbname, 
                    is_full=True, 
                    start_timestamp=start_timestamp, 
                    end_timestamp=end_timestamp, 
                    stripe_index=stripe_index, 
                    stripe_count=stripe_count)

                file_path = os.path.join(output_dir, file_name)
                print("Upload {f} to {b}".format(f=file_path, b=blob_name))
                self.backup_configuration.storage_client.create_blob_from_path(
                    container_name=self.backup_configuration.azure_storage_container_name, 
                    file_path=file_path,
                    blob_name=blob_name, 
                    validate_content=True, 
                    max_connections=4)
                os.remove(file_path)

    def transaction_backup(self, output_dir=None, databases=None, force=False):
        database_connector = DatabaseConnector(self.backup_configuration)
        if databases != None:
            databases_to_backup = databases.split(",")
        else:
            databases_to_backup = database_connector.list_databases(is_full=False)

        skip_dbs = self.backup_configuration.get_databases_to_skip()
        databases_to_backup = filter(lambda db: not (db in skip_dbs), databases_to_backup)

        if output_dir == None:
            output_dir = self.backup_configuration.get_standard_local_directory()

        for dbname in databases_to_backup:
            self.tran_backup_single_db(
                dbname=dbname, 
                output_dir=output_dir, 
                force=force)

    @staticmethod
    def should_run_tran_backup(now_time, force, latest_tran_backup_timestamp, log_backup_interval_min):
        if force:
            return True

        age_of_latest_backup_in_storage = Timing.time_diff(latest_tran_backup_timestamp, Timing.datetime_to_timestr(now_time))
        min_interval_allows_backup = age_of_latest_backup_in_storage > log_backup_interval_min
        perform_tran_backup = min_interval_allows_backup
        return perform_tran_backup 

    def tran_backup_single_db(self, dbname, output_dir, force):
        if not BackupAgent.should_run_tran_backup(
                now_time=Timing.now_localtime(), 
                force=force,
                latest_tran_backup_timestamp=self.latest_backup_timestamp(dbname=dbname, is_full=False),
                log_backup_interval_min=self.backup_configuration.get_log_backup_interval_min()):

            log_msg="Skipping backup of transactions for {dbname}. (min='{min}' latest='{latest}' now='{now}'".format(dbname=dbname,
                min=self.backup_configuration.get_log_backup_interval_min(),
                latest=self.latest_backup_timestamp(dbname=dbname, is_full=False),
                now=Timing.now_localtime_string())
            logging.info(log_msg)
            print(log_msg)
            return

        db_connector = DatabaseConnector(self.backup_configuration)
        stripe_count = db_connector.determine_database_backup_stripe_count(
            dbname=dbname, is_full=False)

        start_timestamp = Timing.now_localtime()
        stdout, stderr = db_connector.create_backup(
            dbname=dbname, 
            is_full=False,
            start_timestamp=start_timestamp, 
            stripe_count=stripe_count, 
            output_dir=output_dir)
        end_timestamp = Timing.now_localtime()

        logging.info(stdout)
        logging.warning(stderr)
        # print(stdout)
        # printe(stderr)

        for stripe_index in range(1, stripe_count + 1):
            file_name = Naming.construct_filename(
                dbname=dbname, 
                is_full=False, 
                start_timestamp=start_timestamp,
                stripe_index=stripe_index, 
                stripe_count=stripe_count)
            blob_name = Naming.construct_blobname(
                dbname=dbname, 
                is_full=False, 
                start_timestamp=start_timestamp, 
                end_timestamp=end_timestamp, 
                stripe_index=stripe_index, 
                stripe_count=stripe_count)

            file_path = os.path.join(output_dir, file_name)
            print("Upload {f} to {b}".format(f=file_path, b=blob_name))
            self.backup_configuration.storage_client.create_blob_from_path(
                container_name=self.backup_configuration.azure_storage_container_name, 
                file_path=file_path,
                blob_name=blob_name, 
                validate_content=True, 
                max_connections=4)
            os.remove(file_path)

    def list_backups(self, databases = []):
        baks_dict = self.existing_backups(databases=databases)
        for end_timestamp in baks_dict.keys():
            # http://mark-dot-net.blogspot.com/2014/03/python-equivalents-of-linq-methods.html
            stripes = baks_dict[end_timestamp]
            stripes = map(lambda blobname: {
                    "blobname":blobname,
                    "filename": Naming.blobname_to_filename(blobname),
                    "parts": Naming.parse_blobname(blobname)
                }, stripes)
            stripes = map(lambda x: {
                    "blobname": x["blobname"],
                    "filename": x["filename"],
                    "parts": x["parts"],
                    "dbname": x["parts"][0],
                    "is_full": x["parts"][1],
                    #"start": x["parts"][2],
                    "end": x["parts"][3],
                    "stripe_index": x["parts"][4],
                    #"stripe_count": x["parts"][5]
                }, stripes)

            group_by_key=lambda x: "Database \"{dbname}\" {type} finished {end}".format(
                dbname=x["dbname"], 
                type=Naming.backup_type_str(x["is_full"]), 
                end=x["end"])

            for group, values in groupby(stripes, key=group_by_key): 
                files = list(map(lambda s: s["stripe_index"], values))
                print("{backup} {files}".format(backup=group, files=files))

    def restore(self, restore_point, databases):
        database_connector = DatabaseConnector(self.backup_configuration)
        if len(databases) == 0:
            databases = database_connector.list_databases(is_full=True)
        skip_dbs = self.backup_configuration.get_databases_to_skip()
        databases = filter(lambda db: not (db in skip_dbs), databases)
        for dbname in databases:
            self.restore_single_db(dbname=dbname, restore_point=restore_point)

    def restore_single_db(self, dbname, restore_point):
        blobs = self.list_restore_blobs(dbname=dbname)
        end_dates = Timing.sort(blobs.keys())
        for end_date in end_dates:
            blobnames = blobs[end_date]
            for blobname in blobnames:
                (_dbname, is_full, start_date, end_date, stripe_index, stripe_count) = Naming.parse_blobname(blobname)
                print("blob: {} {} {} {} {} {}".format(dbname, is_full, start_date, end_date, stripe_index, stripe_count))

    def list_restore_blobs(self, dbname):
        existing_blobs_dict = dict()
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                prefix="{dbname}_".format(dbname=dbname), 
                marker=marker)
            for blob in results:
                blob_name=blob.name
                parts = Naming.parse_blobname(blob_name)
                end_time_of_existing_blob = parts[3]
                if not existing_blobs_dict.has_key(end_time_of_existing_blob):
                    existing_blobs_dict[end_time_of_existing_blob] = []
                existing_blobs_dict[end_time_of_existing_blob].append(blob_name)
            if results.next_marker:
                marker = results.next_marker
            else:
                break
        return existing_blobs_dict

    def show_configuration(self):
        if DevelopmentSettings.is_christians_developer_box():
            print("WARNING!!! This seems to be Christian's Developer Box")
        print("azure.vm_name:                      {}".format(self.backup_configuration.get_vm_name()))
        print("azure.resource_group_name:          {}".format(self.backup_configuration.get_resource_group_name()))
        print("azure.subscription_id:              {}".format(self.backup_configuration.get_subscription_id()))
        print("sap.SID:                            {}".format(self.backup_configuration.get_SID()))
        print("sap.CID:                            {}".format(self.backup_configuration.get_CID()))
        print("skipped databases:                  {}".format(self.backup_configuration.get_databases_to_skip()))
        print("db_backup_interval_min:             {}".format(self.backup_configuration.get_db_backup_interval_min()))
        print("db_backup_interval_max:             {}".format(self.backup_configuration.get_db_backup_interval_max()))
        print("log_backup_interval_min:            {}".format(self.backup_configuration.get_log_backup_interval_min()))
        print("log_backup_interval_max:            {}".format(self.backup_configuration.get_log_backup_interval_max()))
        print("azure_storage_container_name:       {}".format(self.backup_configuration.azure_storage_container_name))
        print("azure_storage_account_name:         {}".format(self.backup_configuration._BackupConfiguration__get_azure_storage_account_name()))
        print("azure_storage_account_key:          {}...".format(self.backup_configuration._BackupConfiguration__get_azure_storage_account_key()[0:10]))

class Runner:
    @staticmethod
    def configure_logging():
        logfile_name='backup.log'
        logging.basicConfig(
            filename=logfile_name,
            level=logging.DEBUG,
            format="%(asctime)-15s pid-%(process)d line-%(lineno)d %(levelname)s: \"%(message)s\""
            )
        logging.getLogger('azure.storage').setLevel(logging.FATAL)

    @staticmethod
    def arg_parser():
        parser = argparse.ArgumentParser()
        parser.add_argument("-c",  "--config", help="the path to the config file")
        parser.add_argument("-f",  "--full-backup", help="Perform full backup", action="store_true")
        parser.add_argument("-ff", "--full-backup-force", help="Perform forceful full backup (ignores business hours or age of last backup)", action="store_true")
        parser.add_argument("-s",  "--skip-upload", help="Skip uploads of backup files", action="store_true")
        parser.add_argument("-o",  "--output-dir", help="Specify target folder for backup files")
        parser.add_argument("-db", "--databases", help="Select databases to backup or restore ('--databases A,B,C')")
        parser.add_argument("-t",  "--transaction-backup", help="Perform transactions backup", action="store_true")
        parser.add_argument("-tf", "--transaction-backup-force", help="Perform forceful transactions backup (ignores age of last backup)", action="store_true")
        parser.add_argument("-r",  "--restore", help="Perform restore for date")
        parser.add_argument("-l",  "--list-backups", help="Lists all backups in Azure storage", action="store_true")
        parser.add_argument("-u",  "--unit-tests", help="Run unit tests", action="store_true")
        parser.add_argument("-x",  "--show-configuration", help="Shows the VM's configuration values", action="store_true")
        return parser

    @staticmethod
    def main():
        Runner.configure_logging()
        parser = Runner.arg_parser() 
        args = parser.parse_args()

        if args.databases:
            databases =  args.databases.split(",")
        else:
            databases = []

        if args.full_backup or args.full_backup_force:
            try:
                with pid.PidFile(pidname='backup-ase-full', piddir=".") as _p:
                    BackupAgent(args.config).full_backup(
                        force=args.full_backup_force, 
                        skip_upload=args.skip_upload,
                        output_dir=args.output_dir,
                        databases=args.databases)
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip full backup, already running")
                printe("Skipping full backup, there is a full-backup in flight currently")
        elif args.transaction_backup or args.transaction_backup_force:
            try:
                with pid.PidFile(pidname='backup-ase-tran', piddir=".") as _p:
                    BackupAgent(args.config).transaction_backup(
                        force=args.transaction_backup_force)
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip transaction log backup, already running")
        elif args.restore:
            BackupAgent(args.config).restore(restore_point=args.restore, databases=databases)
        elif args.list_backups:
            BackupAgent(args.config).list_backups(databases=databases)
        elif args.unit_tests:
            import doctest
            doctest.testmod()
            # doctest.testmod(verbose=True)
        elif args.show_configuration:
            BackupAgent(args.config).show_configuration()
        else:
            parser.print_help()

if __name__ == "__main__":
    Runner.main()

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

import pid
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob.models import ContentSettings
from azure.common import AzureMissingResourceHttpError

def eprint(*args, **kwargs):
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
        return (m.group('dbname'), Naming.type_str_is_full(m.group('type')), m.group('start'), m.group('end'), int(m.group('idx')), int(m.group('cnt')))

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
            >>> cfg.get_storage_client() != None
            True
        """
        self.cfg_file = BackupConfigurationFile(filename=config_filename)
        self.instance_metadata = AzureVMInstanceMetadata.create_instance()
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
    
    def get_storage_client(self):
        block_blob_service = BlockBlobService(
            account_name=self.get_value("azure.storage.account_name"), 
            account_key=self.get_value("azure.storage.account_key"))
        _created = block_blob_service.create_container(container_name=self.get_value("azure.storage.container_name"))
        return block_blob_service

class DatabaseConnector:
    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration

    def list_databases(self):
        return [ "A", "B", "C" ]

class BackupAgent:
    def __init__(self, config_filename):
        self.backup_configuration = BackupConfiguration(config_filename)

    def full_backup(self, force=False, skip_upload=False, output_dir=None, databases=None):
        database_connector = DatabaseConnector(self.backup_configuration)
        if databases != None:
            databases_to_backup = databases.split(",")
        else:
            databases_to_backup = database_connector.list_databases()

        for dbname in databases_to_backup:
            self.full_backup_single_db(dbname=dbname, force=force, skip_upload=skip_upload, output_dir=output_dir)

    def full_backup_single_db(self, dbname, force, skip_upload, output_dir):
        start_timestamp = Timing.now_localtime()
        allowed_by_business = self.backup_configuration.get_business_hours().is_backup_allowed_time(start_timestamp)

        print("Attempting full backup: dbname={dbname} start_timestamp={start_timestamp} allowed_by_business={allowed_by_business} force={force} skip_upload={skip_upload} output_dir=\"{output_dir}\" ".format(
            dbname=dbname, start_timestamp=start_timestamp, allowed_by_business=allowed_by_business, 
            force=force, skip_upload=skip_upload, output_dir=output_dir))

        file_name = Naming.construct_filename(dbname=dbname, is_full=True, start_timestamp=start_timestamp, stripe_index=1, stripe_count=1)

        subprocess.check_output(["./isql.py", "-f", file_name])
        end_timestamp = Timing.now_localtime()

        blob_name = Naming.construct_blobname(dbname=dbname, is_full=True, start_timestamp=start_timestamp, end_timestamp=end_timestamp, stripe_index=1, stripe_count=1)
        print("Upload {f} to {b}".format(f=file_name, b=blob_name))


    def transaction_backup(self):
        print("transaction_backup Not yet impl")
        logging.info("Run transaction log backup")

    def restore(self, restore_point):
        print("restore Not yet impl restore for point {}".format(restore_point))

class Runner:
    @staticmethod
    def configure_logging():
        logfile_name='backup.log'
        logging.basicConfig(
            filename=logfile_name,
            level=logging.INFO,
            format="%(asctime)-15s pid-%(process)d line-%(lineno)d %(levelname)s: \"%(message)s\""
            )
        logging.getLogger('azure.storage').setLevel(logging.FATAL)

    @staticmethod
    def arg_parser():
        parser = argparse.ArgumentParser()
        parser.add_argument("-c",  "--config", help="the path to the config file")
        parser.add_argument("-f",  "--full-backup", help="Perform full backup", action="store_true")
        parser.add_argument("-ff", "--full-backup-force", help="Perform forceful full backup (ignores business hour or age of last backup)", action="store_true")
        parser.add_argument("-s",  "--skip-upload", help="Skip uploads of backup files", action="store_true")
        parser.add_argument("-o",  "--output-dir", help="Specify target folder for backup files")
        parser.add_argument("-db", "--databases", help="Select databases to backup or restore")
        parser.add_argument("-t",  "--transaction-backup", help="Perform full backup", action="store_true")
        parser.add_argument("-r",  "--restore", help="Perform restore for date")
        parser.add_argument("-u",  "--unit-tests", help="Run unit tests", action="store_true")
        return parser

    @staticmethod
    def main():
        Runner.configure_logging()
        parser = Runner.arg_parser() 
        args = parser.parse_args()
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
                eprint("Skipping full backup, there is a full-backup in flight currently")
        elif args.transaction_backup:
            try:
                with pid.PidFile(pidname='backup-ase-tran', piddir=".") as _p:
                    BackupAgent(args.config).transaction_backup()
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip transaction log backup, already running")
        elif args.restore:
            BackupAgent(args.config).restore(args.restore)
        elif args.unit_tests:
            import doctest
            doctest.testmod()
            # doctest.testmod(verbose=True)
        else:
            parser.print_help()

if __name__ == "__main__":
    Runner.main()

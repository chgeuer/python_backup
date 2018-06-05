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



class TestMethods(unittest.TestCase):
    def test_time_diff_in_seconds(self):
        self.assertEqual(Timing.time_diff_in_seconds("20180106_120000", "20180106_120010"), 10)
        self.assertEqual(Timing.time_diff_in_seconds("20180106_110000", "20180106_120010"), 3610)
        self.assertEqual(
            Naming.construct_filename(
                dbname="test1db", is_full=True, 
                timestamp=Timing.parse("20180601_112429"), 
                stripe_index=2, stripe_count=101), 
            "test1db_full_20180601_112429_S002-101.cdmp")
        self.assertEqual(
            Naming.construct_filename(
                dbname="test1db", is_full=True, 
                timestamp=Timing.parse("20180601_112429"), 
                stripe_index=2, stripe_count=3), 
            "test1db_full_20180601_112429_S02-03.cdmp")

    def test_when_to_run_a_full_backup(self):
        test_data = [
            [ False, { 'now': "20180604_005900", 'at': "01:00:00", 'age': 23*3600 } ], # One minute before official schedule do not run a full backup
            [ False, { 'now': "20180604_235959", 'at': "00:00:00", 'age': 23*3600 } ], # One minute before official schedule do not run a full backup
            [ False, { 'now': "20180604_010200", 'at': "01:00:00", 'age': 39 } ],      #  Two minutes after full backup schedule, skip, because the successful backup is 39 seconds old
            [ True,  { 'now': "20180604_010200", 'at': "01:00:00", 'age': 23*3600 } ], #  Two minutes after full backup schedule, run, because the successful backup is 23 hours old
            [ True,  { 'now': "20180604_000000", 'at': "00:00:00", 'age': 23*3600 } ], #  Two minutes after full backup schedule, run, because the successful backup is 23 hours old
            [ True,  { 'now': "20180604_000001", 'at': "00:00:00", 'age': 2 } ],       #  Two minutes after full backup schedule, run, because the successful backup is from before scheduled time
        ]
        for d in test_data:
            print("Try now={now} at={at} age={age}".format(now=Timing.parse(d[1]['now']), at=d[1]['at'], age=d[1]['age']))
            self.assertEqual(d[0], Timing.should_run_regular_backup_now(
                fullbackupat_str=d[1]['at'],
                age_of_last_backup_in_seconds=d[1]['age'],
                now=Timing.parse(d[1]['now'])
            ))

class StorageConfiguration:
    account_name = None
    account_key = None
    block_blob_service = None
    container_name = None
    def __init__(self, filename):
        with open(filename, mode='rt') as file:
            content = file.read()
        j = (json.JSONDecoder()).decode(content)
        self.account_name=j['account_name']
        self.account_key=j['account_key']
        self.container_name=j['container_name']
        self.block_blob_service = BlockBlobService(
            account_name=self.account_name, 
            account_key=self.account_key)
        _created = self.block_blob_service.create_container(container_name=self.container_name)

class AzureVMInstanceMetadata:
    @staticmethod
    def request_metadata(api_version="2017-12-01"):
        url="http://169.254.169.254/metadata/instance?api-version={v}".format(v=api_version)
        response = requests.get(url=url, headers={"Metadata": "true"})
        return response.json()

    @staticmethod
    def create_instance():
        # TODO remove the local machine check here... 
        if socket.gethostname() == "erlang":
            with open("meta.json", mode='rt') as file:
                return AzureVMInstanceMetadata(lambda: (json.JSONDecoder()).decode(file.read()))
        else:
            return AzureVMInstanceMetadata(lambda: AzureVMInstanceMetadata.request_metadata())

    def __init__(self, req):
        self.config = req()
        self.subscription_id = self.config["compute"]["subscriptionId"]
        self.resource_group_name = self.config["compute"]["resourceGroupName"]
        self.vm_name = self.config["compute"]["name"]
        self.tags_value = self.config['compute']['tags']
        self.tags = dict(kvp.split(":", 1) for kvp in (self.tags_value.split(";")))
        self.fullbackupat = self.tags["fullbackupat"]
        self.maximum_age_still_respect_business_hours = int(self.tags["maximum_age_still_respect_business_hours"])

class Naming:
    @staticmethod
    def backup_type_str(is_full):
        return ({True:"full", False:"tran"})[is_full]

    @staticmethod
    def construct_filename(dbname, is_full, timestamp, stripe_index, stripe_count):
        return "{dbname}_{type}_{ts}_S{idx:03d}-{cnt:03d}.cdmp".format(
            dbname=dbname, 
            type=Naming.backup_type_str(is_full), 
            ts=Timing.datetime_to_timestr(timestamp),
            idx=int(stripe_index), 
            cnt=int(stripe_count))

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

class BackupTimestampBlob:
    def __init__(self, storage_cfg, instance_metadata, is_full):
        self.storage_cfg=storage_cfg
        self.instance_metadata=instance_metadata
        self.is_full=is_full
        self.blob_name="{subscription_id}-{resource_group_name}-{vm_name}-{type}.json".format(
            subscription_id=self.instance_metadata.subscription_id,
            resource_group_name=self.instance_metadata.resource_group_name,
            vm_name=self.instance_metadata.vm_name,
            type=Naming.backup_type_str(self.is_full)
        )

    def age_of_last_backup_in_seconds(self):
        return Timing.time_diff_in_seconds(self.read(), Timing.now())

    def full_backup_required(self):
        age_of_last_backup_in_seconds = self.age_of_last_backup_in_seconds()

        now=time.gmtime()
        now_epoch=calendar.timegm(now)

        sched = time.strptime(self.instance_metadata.fullbackupat, "%H:%M:%S")
        planned_today=datetime.datetime(
            now.tm_year, now.tm_mon, now.tm_mday, 
            sched.tm_hour, sched.tm_min, sched.tm_sec).utctimetuple()
        planned_today_epoch=calendar.timegm(planned_today)

        if age_of_last_backup_in_seconds > self.instance_metadata.maximum_age_still_respect_business_hours:
            return True

        if (now_epoch < planned_today_epoch):
            return False

        return age_of_last_backup_in_seconds > now_epoch - planned_today_epoch

    def write(self):
        self.storage_cfg.block_blob_service.create_blob_from_text(
            container_name=self.storage_cfg.container_name, 
            blob_name=self.blob_name,
            encoding="utf-8",
            content_settings=ContentSettings(content_type="application/json"),
            text=(json.JSONEncoder()).encode({ 
                "backup_type": Naming.backup_type_str(self.is_full), 
                "utc_time": Timing.now()
            })
        )

    def read(self):
        try:
            blob=self.storage_cfg.block_blob_service.get_blob_to_text(
                container_name=self.storage_cfg.container_name, 
                blob_name=self.blob_name,
                encoding="utf-8"
            )
            return (json.JSONDecoder()).decode(blob.content)["utc_time"]
        except AzureMissingResourceHttpError:
            return "19000101_000000"

class BackupAgent:
    def __init__(self, filename):
        self.storage_cfg = StorageConfiguration(filename)
        self.instance_metadata = AzureVMInstanceMetadata.create_instance()

    def backup(self):
        timestamp_file_full = BackupTimestampBlob(
            storage_cfg=self.storage_cfg, 
            instance_metadata=self.instance_metadata, 
            is_full=True)

        full_backup_required=timestamp_file_full.full_backup_required()

        full_backup_was_already_running=False
        if full_backup_required:
            try:
                with pid.PidFile(pidname='backup-ase-full', piddir=".") as _p:
                    logging.info("Run full backup")
                    self.do_full_backup(timestamp_file_full)
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip full backup, already running")
                full_backup_was_already_running=True

        if full_backup_was_already_running or not full_backup_required:
            try:
                with pid.PidFile(pidname='backup-ase-tran', piddir=".") as _p:
                    logging.info("Run transaction log backup")
                    self.do_transaction_backup()
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip transaction log backup, already running")

    def do_full_backup(self, timestamp_file_full):
        logging.info("Last full backup : {age_in_seconds} secs ago".format(
            age_in_seconds=timestamp_file_full.age_of_last_backup_in_seconds()))

        subprocess.check_output(["./isql.py", "-f"])
        source = "."
        pattern = "*.cdmp"
        # TODO only for full files
        for filename in glob.glob1(dirname=source, pattern=pattern):
            file_path = os.path.join(source, filename)
            exists = self.storage_cfg.block_blob_service.exists(
                container_name=self.storage_cfg.container_name, 
                blob_name=filename)
            if not exists:
                logging.info("Upload {}".format(filename))
                self.storage_cfg.block_blob_service.create_blob_from_path(
                    container_name=self.storage_cfg.container_name, 
                    blob_name=filename, file_path=file_path,
                    validate_content=True, max_connections=4)
            os.remove(file_path)
        timestamp_file_full.write() # make sure we use proper timestamp info

    def do_transaction_backup(self):
        timestamp_file_tran = BackupTimestampBlob(
            storage_cfg=self.storage_cfg, 
            instance_metadata=self.instance_metadata, 
            is_full=False)

        timestamp_file_tran.write()

    def restore(self, restore_point):
        print "Perform restore for restore point \"{}\"".format(restore_point)

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
        parser.add_argument("-c", "--config", help="the JSON config file")
        parser.add_argument("-b", "--backup", help="Perform backup", action="store_true")
        parser.add_argument("-r", "--restore", help="Perform restore for date")
        parser.add_argument("-t", "--tests", help="Run tests", action="store_true")
        return parser

    @staticmethod
    def main():
        Runner.configure_logging()
        parser = Runner.arg_parser() 
        args = parser.parse_args()
        if args.backup:
            BackupAgent(args.config).backup()
        elif args.restore:
            BackupAgent(args.config).restore(args.restore)
        elif args.tests:
            suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
            unittest.TextTestRunner(verbosity=2).run(suite)
        else:
            parser.print_help()

if __name__ == '__main__':
    Runner.main()

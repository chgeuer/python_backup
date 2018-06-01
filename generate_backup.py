#!/usr/bin/env python2.7 

# S01-10 or S001-115

# Who decides the filename test1db_full_20180529_120301_S01-01.cdmp (which date is correct)
#   is generated by the Python s cript, and passed within the SQL statement
# How to enumerate the databases to backup? How does isql output look like
# Should databases to be skipped be configured in JSON?
# When backup calls isql, how to determine which files to upload? When there are already files, which ones to ignore?
# It was mentioned that in some cases, backup should terminate without doing anything? 
#   - Full backup is running
#     - Full Backup starts --> terminate the 2nd instance
#     - Transaction Backup starts --> allowed to run
#   - Transaction backup is running
#     - Full Backup starts --> allowed to run
#     - Transaction Backup starts --> terminate the 2nd instance
# - keep local files

import sys
import os
import platform
import subprocess
import re
import glob
import argparse
import time
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
                timestamp=time.strptime("20180601_112429", Timing.time_format()), 
                stripe_index=2, stripe_count=101), 
            "test1db_full_20180601_112429_S002-101.cdmp")
        self.assertEqual(
            Naming.construct_filename(
                dbname="test1db", is_full=True, 
                timestamp=time.strptime("20180601_112429", Timing.time_format()), 
                stripe_index=2, stripe_count=3), 
            "test1db_full_20180601_112429_S02-03.cdmp")

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
        logging.info("Running in subscription {}".format(self.subscription_id))
        self.resource_group_name = self.config["compute"]["resourceGroupName"]
        logging.info("Running in resource group {}".format(self.resource_group_name))
        self.vm_name = self.config["compute"]["name"]
        logging.info("Running in VM {}".format(self.vm_name))
        self.tags_value = self.config['compute']['tags']
        self.tags = dict(kvp.split(":", 1) for kvp in (self.tags_value.split(";")))
        self.backupschedule = int(self.tags["backupschedule"])
        logging.info("Backup schedule {}".format(self.backupschedule))
        self.backuptime = self.tags["backuptime"]
        logging.info("Backup time {}".format(self.backuptime))

class Naming:
    @staticmethod
    def backup_type_str(is_full):
        return ({True:"full", False:"tran"})[is_full]

    @staticmethod
    def construct_filename(dbname, is_full, timestamp, stripe_index, stripe_count):
        format_str = (
            {
                True:  "{name}_{type}_{ts}_S{idx:02d}-{cnt:02d}.cdmp", 
                False: "{name}_{type}_{ts}_S{idx:03d}-{cnt:03d}.cdmp"
            }
        )[stripe_count < 100]

        return format_str.format(
            name=dbname, 
            type=Naming.backup_type_str(is_full), 
            ts=Timing.datetime_to_timestr(timestamp),
            idx=int(stripe_index), 
            cnt=int(stripe_count))

class Timing:
    @staticmethod
    def time_format():
        return "%Y%m%d_%H%M%S"

    @staticmethod
    def now():
        return Timing.datetime_to_timestr(time.gmtime())

    @staticmethod
    def datetime_to_timestr(t):
        return time.strftime(Timing.time_format(), t)

    @staticmethod
    def timestr_to_datetime(time_str):
        t = time.strptime(time_str, Timing.time_format())
        return datetime.datetime(
            year=t.tm_year, month=t.tm_mon, day=t.tm_mday, 
            hour=t.tm_hour, minute=t.tm_min, second=t.tm_sec)

    @staticmethod
    def time_diff_in_seconds(timestr_1, timestr_2):
        return int((Timing.timestr_to_datetime(timestr_2) - Timing.timestr_to_datetime(timestr_1)).total_seconds())

class BackupTimestampBlob:
    storage_cfg = None
    instance_metadata = None
    is_full = None
    blob_name = None
    
    def __init__(self, storage_cfg, instance_metadata, is_full):
        self.storage_cfg = storage_cfg
        self.instance_metadata = instance_metadata
        self.is_full = is_full
        self.blob_name="{subscription_id}-{resource_group_name}-{vm_name}-{type}.json".format(
            subscription_id=self.instance_metadata.subscription_id,
            resource_group_name=self.instance_metadata.resource_group_name,
            vm_name=self.instance_metadata.vm_name,
            type=Naming.backup_type_str(self.is_full)
        )

    def age_of_last_backup_in_seconds(self):
        return Timing.time_diff_in_seconds(self.read(), Timing.now())

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

    def main_backup_full(self):
        timestamp_file_full = BackupTimestampBlob(
            storage_cfg=self.storage_cfg, 
            instance_metadata=self.instance_metadata, 
            is_full=True)

        timestamp_file_tran = BackupTimestampBlob(
            storage_cfg=self.storage_cfg, 
            instance_metadata=self.instance_metadata, 
            is_full=False)

        logging.info("Last full backup : {age_in_seconds} secs ago".format(age_in_seconds=timestamp_file_full.age_of_last_backup_in_seconds()))
        logging.info("Last transaction backup : {age_in_seconds} secs ago".format(age_in_seconds=timestamp_file_tran.age_of_last_backup_in_seconds()))

        subprocess.check_output(["./isql.py", "-f"])
        source = "."
        pattern = "*.cdmp"
        for filename in glob.glob1(dirname=source, pattern=pattern):
            file_path = os.path.join(source, filename)
            exists = self.storage_cfg.block_blob_service.exists(
                container_name=self.storage_cfg.container_name, 
                blob_name=filename)
            if not exists:
                print("Upload {}".format(filename))
                self.storage_cfg.block_blob_service.create_blob_from_path(
                    container_name=self.storage_cfg.container_name, 
                    blob_name=filename, file_path=file_path,
                    validate_content=True, max_connections=4)
            os.remove(file_path)
        timestamp_file_full.write()

    def main_backup_transactions(self):
        print("Perform transactional backup {fn}".format(fn="..."))

    def main_restore(self, restore_point):
        print "Perform restore for restore point \"{}\"".format(restore_point)

def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config-file", 
                        help="the JSON config file")
    parser.add_argument("-f", "--backup-full", 
                        help="Perform full backup",
                        action="store_true")
    parser.add_argument("-t", "--backup-transactions", 
                        help="Perform transaction backup",
                        action="store_true")
    parser.add_argument("--tests", 
                        help="Perform unit tests",
                        action="store_true")
    parser.add_argument("-r", "--restore", 
                        help="Perform restore for date")
    return parser

def main():
    parser = arg_parser() 
    args = parser.parse_args()
    if args.backup_full:
        backup_agent = BackupAgent(args.config_file)
        backup_agent.main_backup_full()
    elif args.backup_transactions:
        backup_agent = BackupAgent(args.config_file)
        try:
            with pid.PidFile(pidname='txbackup') as _p:
                backup_agent.main_backup_transactions()
        except pid.PidFileAlreadyLockedError:
            print("Skip full backup, already running")
    elif args.restore:
        backup_agent = BackupAgent(args.config_file)
        backup_agent.main_restore(args.restore)
    elif args.tests:
        suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
        unittest.TextTestRunner(verbosity=2).run(suite)
    else:
        parser.print_help()

if __name__ == '__main__':
    logging.basicConfig(filename='backup.log',level=logging.INFO)
    main()

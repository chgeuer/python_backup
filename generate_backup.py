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

import pid
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob.models import ContentSettings
from azure.common import AzureMissingResourceHttpError

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

    def __init__(self, api_version="2017-12-01"):
        self.config = AzureVMInstanceMetadata.request_metadata(api_version=api_version)
        self.subscription_id = self.config["compute"]["subscriptionId"]
        logging.warning("Running in subscription {}".format(self.subscription_id))
        self.resource_group_name = self.config["compute"]["resourceGroupName"]
        self.vm_name = self.config["compute"]["name"]
        self.tags_value = self.config['compute']['tags']
        self.tags = dict(kvp.split(":", 1) for kvp in (self.tags_value.split(";")))
        self.backupschedule = int(self.tags["backupschedule"])
        logging.warning("Backup schedule {}".format(self.backupschedule))
        self.backuptime = self.tags["backuptime"]

class BackupTimestamp:
    storage_cfg = None
    instance_metadata = None
    is_full = None
    
    def __init__(self, storage_cfg, instance_metadata, is_full):
        self.storage_cfg = storage_cfg
        self.instance_metadata = instance_metadata
        self.is_full = is_full

    def age_of_last_backup_in_seconds(self):
        return Naming.time_diff_in_seconds(self.read(), Naming.now())

    def blob_name(self):
        return "{subscription_id}-{resource_group_name}-{vm_name}-{type}.json".format(
            subscription_id=self.instance_metadata.subscription_id,
            resource_group_name=self.instance_metadata.resource_group_name,
            vm_name=self.instance_metadata.vm_name,
            type=Naming.backup_type_str(self.is_full)
        )

    def write(self):
        self.storage_cfg.block_blob_service.create_blob_from_text(
            container_name=self.storage_cfg.container_name, 
            blob_name=self.blob_name()
            encoding="utf-8",
            content_settings=ContentSettings(content_type="application/json"),
            text=(json.JSONEncoder()).encode({ 
                "backup_type": Naming.backup_type_str(self.is_full), 
                "utc_time": Naming.now()
            })
        )

    def read(self):
        try:
            blob=self.storage_cfg.block_blob_service.get_blob_to_text(
                container_name=self.storage_cfg.container_name, 
                blob_name=self.blob_name()
                encoding="utf-8"
            )
            return (json.JSONDecoder()).decode(blob.content)["utc_time"]
        except AzureMissingResourceHttpError:
            return "19000101_000000"

class Naming:
    @staticmethod
    def time_format():
        return "%Y%m%d_%H%M%S"

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

        return format_str.format(name=dbname, 
            type={True:"full", False:"tran"}[is_full], 
            ts=Naming.datetime_to_timestr(timestamp),
            idx=int(stripe_index), cnt=int(stripe_count))

    @staticmethod
    def now():
        return Naming.datetime_to_timestr(time.gmtime())

    @staticmethod
    def datetime_to_timestr(t):
        return time.strftime(Naming.time_format(), t)

    @staticmethod
    def timestr_to_datetime(time_str):
        t = time.strptime(time_str, Naming.time_format())
        return datetime.datetime(
            year=t.tm_year, month=t.tm_mon, day=t.tm_mday, 
            hour=t.tm_hour, minute=t.tm_min, second=t.tm_sec)

    @staticmethod
    def time_diff_in_seconds(timestr_1, timestr_2):
        return int((Naming.timestr_to_datetime(timestr_2) - Naming.timestr_to_datetime(timestr_1)).total_seconds())

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
    parser.add_argument("-r", "--restore", 
                        help="Perform restore for date")
    return parser

def main():
    parser = arg_parser() 
    args = parser.parse_args()
    if args.backup_full:
        main_backup_full(args.config_file)
    elif args.backup_transactions:
        try:
            with pid.PidFile(pidname='txbackup') as _p:
                main_backup_transactions()
        except pid.PidFileAlreadyLockedError:
            print("Skip full backup, already running")
    elif args.restore:
        main_restore(args.restore)
    else:
        parser.print_help()

def main_backup_full(filename):
    storage_cfg = StorageConfiguration(filename)
    instance_metadata = AzureVMInstanceMetadata()
    timestamp_file = BackupTimestamp(
        storage_cfg=storage_cfg, 
        instance_metadata=instance_metadata, 
        is_full=True)

    print("Last backup : {age_in_seconds} secs ago".format(
        age_in_seconds=timestamp_file.age_of_last_backup_in_seconds()))

    subprocess.check_output(["./isql.py", "-f"])
    source = "."
    pattern = "*.cdmp"
    for filename in glob.glob1(dirname=source, pattern=pattern):
        file_path = os.path.join(source, filename)
        exists = storage_cfg.block_blob_service.exists(
            container_name=storage_cfg.container_name, 
            blob_name=filename)
        if not exists:
            print("Upload {}".format(filename))
            storage_cfg.block_blob_service.create_blob_from_path(
                container_name=storage_cfg.container_name, 
                blob_name=filename, file_path=file_path,
                validate_content=True, max_connections=4)
        os.remove(file_path)
    timestamp_file.write()

def main_backup_transactions():
    print("Perform transactional backup {fn}".format(fn="..."))

def main_restore(restore_point):
    print "Perform restore for restore point \"{}\"".format(restore_point)

if __name__ == '__main__':
    main()

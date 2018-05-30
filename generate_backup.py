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

import pid
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob.models import ContentSettings
from azure.common import AzureMissingResourceHttpError

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

def instance_metadata(api_version="2017-12-01"):
    url="http://169.254.169.254/metadata/instance?api-version={api_version}".format(api_version=api_version)
    return requests.get(url=url, headers={"Metadata": "true"}).json()

def vm_tags():
    try:
        tags = instance_metadata()['compute']['tags']
        return dict(kvp.split(":", 1) for kvp in (tags.split(";")))
    except (requests.exceptions.ConnectionError, ValueError, KeyError):
        return {"backupschedule": "15", "backuptime": "01:10:00"}

def backupschedule():
    return int(vm_tags()["backupschedule"])

def account_credentials_from_file(filename):
    with open(filename, mode='rt') as file:
        content = file.read()
    j = (json.JSONDecoder()).decode(content)
    account_name=j['account_name']
    account_key=j['account_key']
    container_name=j['container_name']
    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
    _created = block_blob_service.create_container(container_name=container_name)
    return (block_blob_service, container_name)

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

def construct_filename(dbname, is_full, timestamp, stripe_index, stripe_count):
    format_str = (
        {
            True:  "{name}_{type}_{ts}_S{idx:02d}-{cnt:02d}.cdmp", 
            False: "{name}_{type}_{ts}_S{idx:03d}-{cnt:03d}.cdmp"
        }
    )[stripe_count < 100]

    return format_str.format(name=dbname, 
        type={True:"full", False:"tran"}[is_full], 
        ts=datetime_to_timestr(timestamp),
        idx=int(stripe_index), cnt=int(stripe_count))

def timestamp_blob_name(is_full):
    meta = instance_metadata()
    subscription_id=meta["compute"]["subscriptionId"]
    resource_group_name=meta["compute"]["resourceGroupName"]
    vm_name=meta["compute"]["name"]
    blob_name="{subscription_id}-{resource_group_name}-{vm_name}-{type}.json".format(
        subscription_id=subscription_id,
        resource_group_name=resource_group_name,
        vm_name=vm_name,
        type=backup_type_str(is_full)
    )
    return blob_name

def backup_type_str(is_full):
    return ({True:"full", False:"tran"})[is_full]

def time_format():
    return "%Y%m%d_%H%M%S"

def now():
    return datetime_to_timestr(time.gmtime())

def datetime_to_timestr(t):
    return time.strftime(time_format(), t)

def timestr_to_datetime(time_str):
    t = time.strptime(time_str, time_format())
    return datetime.datetime(
        year=t.tm_year, month=t.tm_mon, day=t.tm_mday, 
        hour=t.tm_hour, minute=t.tm_min, second=t.tm_sec)

def time_diff_in_seconds(timestr_1, timestr_2):
    return int((timestr_to_datetime(timestr_2) - timestr_to_datetime(timestr_1)).total_seconds())

def store_backup_timestamp(block_blob_service, container_name, is_full):
    block_blob_service.create_blob_from_text(
        container_name=container_name, 
        blob_name=timestamp_blob_name(is_full=is_full), 
        encoding="utf-8",
        content_settings=ContentSettings(content_type="application/json"),
        text=(json.JSONEncoder()).encode({ 
            "backup_type": backup_type_str(is_full), 
            "utc_time": now()
        })
    )

def get_backup_timestamp(block_blob_service, container_name, is_full):
    try:
        blob=block_blob_service.get_blob_to_text(
            container_name=container_name, 
            blob_name=timestamp_blob_name(is_full=is_full), 
            encoding="utf-8"
        )
        return (json.JSONDecoder()).decode(blob.content)["utc_time"]
    except AzureMissingResourceHttpError:
        return "19000101_000000"

def age_of_last_backup_in_seconds(block_blob_service, container_name, is_full):
    last_backup = get_backup_timestamp(block_blob_service, container_name, is_full)
    return time_diff_in_seconds(last_backup, now())

def main_backup_full(filename):
    block_blob_service, container_name = account_credentials_from_file(filename)

    age_in_seconds = age_of_last_backup_in_seconds(
        block_blob_service=block_blob_service, 
        container_name=container_name, is_full=True)
    print("Last backup : {age_in_seconds} secs ago".format(age_in_seconds=age_in_seconds))

    subprocess.check_output(["./isql.py", "-f"])
    source = "."
    pattern = "*.cdmp"
    for filename in glob.glob1(dirname=source, pattern=pattern):
        file_path = os.path.join(source, filename)
        exists = block_blob_service.exists(container_name=container_name, blob_name=filename)
        if not exists:
            print("Upload {}".format(filename))
            block_blob_service.create_blob_from_path(
                container_name=container_name, blob_name=filename, file_path=file_path,
                validate_content=True, max_connections=4)
        os.remove(file_path)
    # Store backup timestamp in storage
    store_backup_timestamp(
        block_blob_service=block_blob_service, 
        container_name=container_name, 
        is_full=True)

def main_backup_transactions():
    print("Perform transactional backup {fn}".format(fn="..."))

def main_restore(restore_point):
    print "Perform restore for restore point \"{}\"".format(restore_point)

if __name__ == '__main__':
    print "Schedule {}".format(backupschedule())
    main()

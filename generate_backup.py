#!/usr/bin/env python2.7

# Who decides the filename test1db_tx_20180529_120301_S0-1.cdmp (which date is correct)
# How to enumerate the databases to backup? How does isql output look like
# Should databases to be skipped be configured in JSON?
# When backup calls isql, how to determine which files to upload? When there are already files, which ones to ignore?
# It was mentioned that in some cases, backup should terminate without doing anything? 
#   - Full backup is running
#     - Full Backup starts --> terminate
#     - Transaction Backup starts --> 
#   - Transaction backup is running
#     - Full Backup starts --> 
#     - Transaction Backup starts --> 

import sys
import re
import os
import glob
import hashlib
import base64
import argparse
import subprocess
import platform
import pid
import time
import json
from azure.storage.blob import BlockBlobService, PublicAccess

def main():
    parser = arg_parser() 
    args = parser.parse_args()
    if args.backup_full:
        main_backup_full(args.config_file)
    elif args.backup_transactions:
        try:
            with pid.PidFile(pidname='txbackup', piddir='.') as _p:
                main_backup_transactions()
        except pid.PidFileAlreadyLockedError:
            print("Skip full backup, already running")
    elif args.restore:
        main_restore(args.restore)
    else:
        parser.print_help()

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

def name():
    dbname = "test1db"
    type = "full"
    timestamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    stripe_index = 1
    stripe_count = 15
    filename = "{}_{}_{}_S{}-{}.cdmp".format(dbname, type, timestamp, str(stripe_index), str(stripe_count))
    return filename

def main_backup_full(filename):
    block_blob_service, container_name = account_credentials_from_file(filename)
    subprocess.check_output(["./isql.py", "-t"])
    source = "."
    pattern = "*.cdmp"
    for filename in glob.glob1(dirname=source, pattern=pattern):
        # test1db_tx_20180529_113854_S0-10.cdmp
        if not re.search(r'a', filename):
            print("Skipping {}".format(filename))
            continue
        file_path = os.path.join(source, filename)
        exists = block_blob_service.exists(container_name=container_name, blob_name=filename)
        if not exists:
            print("Upload {}".format(filename))
            block_blob_service.create_blob_from_path(
                container_name=container_name, blob_name=filename, file_path=file_path,
                validate_content=True, max_connections=4)
        os.remove(file_path)

def main_backup_transactions():
    print("Perform transactional backup")

def main_restore(restore_point):
    print "Perform restore for restore point \"{}\"".format(restore_point)

if __name__ == '__main__':
    main()

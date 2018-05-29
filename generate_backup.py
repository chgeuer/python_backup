#!/usr/bin/env python2.7

import sys
import os
import glob
import hashlib
import base64
import argparse
import platform
import pid
import time
import json
from azure.storage.blob import BlockBlobService, PublicAccess

#print(os.path.abspath(sys.argv[0]))
#txt = "Nobody inspects the spammish repetition"
#print(hashlib.md5(txt.encode('utf-8')).hexdigest())
#print(base64.standard_b64encode(hashlib.md5(txt.encode('utf-8')).digest()).decode("utf-8") )

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
    return (j['account_name'], j['account_key'])

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
    account_name, account_key = account_credentials_from_file(filename)
    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

    container_name = "foo"
    source = "."
    pattern = "*.bin"

    _created_container = block_blob_service.create_container(container_name=container_name)
    for filename in glob.glob1(dirname=source, pattern=pattern):
        file = os.path.join(source, filename)
        exists = block_blob_service.exists(container_name=container_name, blob_name=filename)
        if not exists:
            print("Upload {}".format(filename))
            block_blob_service.create_blob_from_path(
                container_name=container_name, blob_name=filename, file_path=file,
                validate_content=True, max_connections=4)

def main_backup_transactions():
    print "Perform transactional backup"

def main_restore(restore_point):
    print "Perform restore for restore point \"{}\"".format(restore_point)

if __name__ == '__main__':
    main()

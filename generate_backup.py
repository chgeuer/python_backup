#!/usr/bin/env python

import sys
import os
import glob
import subprocess
import hashlib
import base64
import argparse
import platform
import pid
import time
import json

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

def az_exec(args):
    try:
        content = subprocess.check_output(args)
        return (json.JSONDecoder()).decode(content)
    except subprocess.CalledProcessError as e:
        print "Execution resulted in {}".format(e.returncode)
        return None 

def list_container(container_name, filename):
    account_name, account_key = account_credentials_from_file(filename)
    return az_exec(['az', 'storage', 'blob', 'list', 
        '--account-name', account_name, 
        '--account-key', account_key, 
        '--container-name', container_name, 
        '--output', 'json'])

def main_backup_full(filename):
    account_name, account_key = account_credentials_from_file(filename)

    container_name = "foo"
    # source = "/mnt/c/Users/chgeuer/Videos"
    source = "/mnt/c/Users/chgeuer/Desktop/Python Quick Start for Linux System Administrators/3"
    pattern = "*.py"

    for blob in list_container(container_name, filename):
        print("{} Size {}".format(blob['name'], blob['properties']['contentLength']))

    created = az_exec(['az', 'storage', 'container', 'create', 
        '--account-name', account_name, 
        '--account-key', account_key, 
        '--name', container_name, 
        '--output', 'json'])['created']
    if (created):
        print("Created container {}".format(container_name))
    else:
        print("container {} existed".format(container_name))

    for filename in glob.glob1(dirname=source, pattern=pattern):
        file = os.path.join(source, filename)
        exists = az_exec(['az', 'storage', 'blob', 'exists', 
            '--account-name', account_name, 
            '--account-key', account_key, 
            '--container-name', container_name, 
            '--name', filename,
            '--output', 'json'])['exists']
        if not exists:
            print("Upload {}".format(filename))
            j = az_exec(['az', 'storage', 'blob', 'upload', 
                '--account-name', account_name, 
                '--account-key', account_key, 
                '--max-connections', '2', 
                '--type', 'block', 
                '--file', file,
                '--container-name', container_name, 
                '--name', filename,
                '--no-progress', 
                '--output', 'json'])
            print(j)

def main_backup_transactions():
    print "Perform transactional backup"
    print(os.listdir("."))
    time.sleep(20)

def main_restore(restore_point):
    print "Perform restore for restore point \"{}\"".format(restore_point)
    with open('storage.json', mode='rt') as file:
        content = file.read()
    j = (json.JSONDecoder()).decode(content)
    print(j)

if __name__ == '__main__':
    main()

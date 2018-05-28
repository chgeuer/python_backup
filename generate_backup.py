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
            with pid.PidFile(pidname='txbackup', piddir='.') as p:
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

def main_backup_full(filename="config.json"):
    account_name, account_key = account_credentials_from_file(filename)
    print "Use " + account_name + " " + account_key

    container_name = "foo"
    # source = "/mnt/c/Users/chgeuer/Videos"
    source = "/mnt/c/Users/chgeuer/Desktop/Python Quick Start for Linux System Administrators/3"
    pattern = "*.py"
    print(os.listdir(source))

    content = subprocess.check_output(['az', 'storage', 'container', 'create', 
                                       '--account-name', account_name, 
                                       '--account-key', account_key, 
                                       '--name', container_name, 
                                       '--output', 'json'])
    created = (json.JSONDecoder()).decode(content)['created']
    if (created):
        print("Created container {}".format(container_name))
    else:
        print("container {} existed".format(container_name))

    for filename in glob.glob1(dirname=source, pattern=pattern):
        file = os.path.join(source, filename)
        print("Upload {}".format(filename))
        content = subprocess.check_output(['az', 'storage', 'blob', 'upload', 
                                        '--account-name', account_name, 
                                        '--account-key', account_key, 
                                        '--max-connections', '2', 
                                        '--type', 'block', 
                                        '--file', file,
                                        '--container-name', container_name, 
                                        '--name', filename,
                                        '--no-progress', 
                                        '--output', 'json'])
        j = (json.JSONDecoder()).decode(content)
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

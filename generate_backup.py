#! /usr/bin/env python

import sys
import os
import hashlib
import base64
import argparse
import platform
import pid
import time

#print(os.path.abspath(sys.argv[0]))
#txt = "Nobody inspects the spammish repetition"
#print(hashlib.md5(txt.encode('utf-8')).hexdigest())
#print(base64.standard_b64encode(hashlib.md5(txt.encode('utf-8')).digest()).decode("utf-8") )

def main():
    parser = arg_parser() 
    args = parser.parse_args()
    if args.backup_full:
        main_backup_full()
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

def arg_parser():
    # https://docs.python.org/2/howto/argparse.html
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--backup-full", 
                        help="Perform full backup",
                        action="store_true")
    parser.add_argument("-t", "--backup-transactions", 
                        help="Perform transaction backup",
                        action="store_true")
    parser.add_argument("-r", "--restore", 
                        help="Perform restore for date")
    return parser

def main_backup_full():
    print(os.listdir("."))
    time.sleep(20)

def main_backup_transactions():
    print "Perform transactional backup"

def main_restore(restore_point):
    print "Perform restore for restore point \"{}\"".format(restore_point)

main()

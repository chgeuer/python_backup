#!/usr/bin/env python2.7

import time
import argparse
import subprocess

def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--backup-full", help="Perform full backup")
    parser.add_argument("-t", "--backup-transactions", help="Perform transaction backup")
    return parser

def create(name, mb):
    time.sleep(2)
    return subprocess.check_output(["/bin/dd", "if=/dev/urandom", "of={}".format(name), "count=1024", "bs={}".format(int(1024*mb))])

def main():
    parser = arg_parser() 
    args = parser.parse_args()
    if args.backup_full:
        create(args.backup_full, 1)
    elif args.backup_transactions:
        create(args.backup_transactions, 1)
    else:
        parser.print_help()
        exit()

if __name__ == '__main__':
    main()

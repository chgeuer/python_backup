#!/usr/bin/env python2.7

import sys
import time
import math
import os
import argparse
import subprocess

def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--backup-full", 
                        help="Perform full backup",
                        action="store_true")
    parser.add_argument("-t", "--backup-transactions", 
                        help="Perform transaction backup",
                        action="store_true")
    return parser

def create(name, mb):
    # return subprocess.check_output(["/usr/bin/openssl", "rand", "-out", name, str(int(mb * math.pow(2, 20)))])
    # simulate backup by writing junk into a file
    return subprocess.check_output(["/bin/dd", "if=/dev/urandom", "of={}".format(name), "count=1024", "bs={}".format(int(1024*mb))])

def get_all_databases():
    return [ "" ]

def main():
    parser = arg_parser() 
    args = parser.parse_args()
    if args.backup_full:
        bak_type = "full"
        dbname = "test1db"
        timestamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
        stripe_count = 3
        for stripe_index in range(0, stripe_count): 
            filename = "{name}_{type}_{ts}_S{idx:03d}-{cnt:03d}.cdmp".format(
                name=dbname, type=bak_type, ts=timestamp, idx=int(stripe_index), cnt=int(stripe_count))
            create(filename, 128)
    elif args.backup_transactions:
        bak_type = "tx"
        dbname = "test1db"
        timestamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
        stripe_count = 1
        for stripe_index in range(0, stripe_count): 
            filename = "{name}_{type}_{ts}_S{idx:03d}-{cnt:03d}.cdmp".format(
                name=dbname, type=bak_type, ts=timestamp, idx=int(stripe_index), cnt=int(stripe_count))
            create(filename, 2)
    else:
        parser.print_help()
        exit()

if __name__ == '__main__':
    main()

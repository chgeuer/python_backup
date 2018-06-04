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

def time_format():
    return "%Y%m%d_%H%M%S"

def datetime_to_timestr(t):
    return time.strftime(time_format(), t)

def main():
    parser = arg_parser() 
    args = parser.parse_args()
    timestamp=time.gmtime()
    if args.backup_full:
        dbname = "test1db"
        stripe_count = 1
        for stripe_index in range(0, stripe_count): 
            filename = construct_filename(
                dbname=dbname, 
                is_full=True, 
                timestamp=timestamp, 
                stripe_index=stripe_index, 
                stripe_count=stripe_count)
            create(filename, 2)
    elif args.backup_transactions:
        dbname = "test1db"
        stripe_count = 1
        for stripe_index in range(0, stripe_count): 
            filename = construct_filename(
                dbname=dbname, 
                is_full=False, 
                timestamp=timestamp, 
                stripe_index=stripe_index, 
                stripe_count=stripe_count)
            create(filename, 2)
    else:
        parser.print_help()
        exit()

if __name__ == '__main__':
    main()

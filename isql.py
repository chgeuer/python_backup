#!/usr/bin/env python2.7

import sys
import time
import argparse
import subprocess

def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-U", help="<username>")
    parser.add_argument("-P", help="<password>")
    parser.add_argument("-S", help="<SID>")
    parser.add_argument("-w", help="<width>")
    parser.add_argument("-b", help="<suppress_headers>")
    parser.add_argument("-f", "--filename")
    return parser

def create(name):
    with open(name, mode='wt') as file:
        for line in sys.stdin:
            file.write(line)

    time.sleep(2)
    # return subprocess.check_output(["/bin/dd", "if=/dev/urandom", "of={}".format(name), "count=1024", "bs={}".format(int(1024*mb))])

def main():
    parser = arg_parser() 
    args = parser.parse_args()

    create(args.filename)

if __name__ == '__main__':
    main()

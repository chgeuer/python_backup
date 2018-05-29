#!/usr/bin/env python2.7

import sys
import math
import os
import subprocess

def create(name, mb):
    return subprocess.check_output(["/usr/bin/openssl", "rand", "-out", name, str(int(mb * math.pow(2, 20)))])

def main():
    print create("1.bin", 1)
    print create("2.bin", 1)
    print create("3.bin", 128)
    print create("4.bin", 128)


if __name__ == '__main__':
    main()

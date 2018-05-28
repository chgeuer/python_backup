#!/usr/bin/env python

import sys
import math
import os
import subprocess

def main():
    mb = int(10 * math.pow(2, 20))
    content = subprocess.check_output(["/usr/bin/openssl", "rand", "-out", "1.bin", str(mb)])
    print content

if __name__ == '__main__':
    main()

# coding=utf-8

from __future__ import print_function
import sys

def printe(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

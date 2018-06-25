#!/usr/bin/env python2.7
# coding=utf-8

from .funcmodule import printe
from .runner import Runner

def main():
    try:
        Runner.main()
        exit(0)
    except Exception as e:
        printe(e.message)
        exit(-1)

if __name__ == '__main__':
    main()

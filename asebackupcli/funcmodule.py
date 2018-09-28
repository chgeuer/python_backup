# coding=utf-8

from __future__ import print_function
import sys
import logging

def printe(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def out(message):
    logging.info(message)
    print(message)

def log_stdout_stderr(stdout, stderr):
    if stdout:
        for line in stdout.split("\n"):
            logging.info(line)
    if stderr:
        for line in stderr.split("\n"):
            logging.warning(line)


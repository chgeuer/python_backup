# coding=utf-8
# pylint: disable=c0301

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

from __future__ import print_function
import sys
import logging

def printe(*args, **kwargs):
    """Print to STDERR."""
    print(*args, file=sys.stderr, **kwargs)

def out(message):
    """Print to STDOUT."""
    logging.info(message)
    print(message)

def log_stdout_stderr(stdout, stderr):
    if stdout:
        for line in stdout.split("\n"):
            logging.info(line)
    if stderr:
        for line in stderr.split("\n"):
            logging.warning(line)

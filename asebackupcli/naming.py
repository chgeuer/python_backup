# coding=utf-8
# pylint: disable=c0301

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""Naming module"""

import os
import re

class Naming(object):
    """Naming utilities"""

    @staticmethod
    def backup_type_str(is_full):
        return ({True:"full", False:"tran"})[is_full]

    @staticmethod
    def type_str_is_full(type_str):
        return ({"full":True, "tran":False})[type_str]

    @staticmethod
    def construct_filename(dbname, is_full, start_timestamp, stripe_index, stripe_count):
        """Constructs a filename."""
        return "{dbname}_{type}_{start_timestamp}_S{idx:03d}-{cnt:03d}.cdmp".format(
            dbname=dbname, type=Naming.backup_type_str(is_full),
            start_timestamp=start_timestamp,
            idx=int(stripe_index), cnt=int(stripe_count))

    @staticmethod
    def local_filesystem_name(directory, dbname, is_full, start_timestamp, stripe_index, stripe_count):
        """Creates a file path in the local file system"""
        file_name = Naming.construct_filename(
            dbname, is_full, start_timestamp, stripe_index, stripe_count)
        return os.path.join(directory, file_name)

    @staticmethod
    def pipe_name(output_dir, dbname, is_full, stripe_index, stripe_count):
        """Creates a named pipe path in the local file system"""
        return os.path.join(output_dir, "backup_{}_{}_{:03d}_{:03d}.cdmp_pipe".format(
            dbname, Naming.backup_type_str(is_full), stripe_index, stripe_count))

    @staticmethod
    def pipe_names(dbname, is_full, stripe_count, output_dir):
        """Create named pipe names."""
        return [
            Naming.pipe_name(
                output_dir=output_dir,
                dbname=dbname,
                is_full=is_full,
                stripe_index=stripe_index,
                stripe_count=stripe_count)
            for stripe_index in range(1, stripe_count + 1)
        ]

    @staticmethod
    def construct_blobname_prefix(dbname, is_full):
        return "{dbname}_{type}_".format(
            dbname=dbname,
            type=Naming.backup_type_str(is_full))

    @staticmethod
    def construct_blobname(dbname, is_full, start_timestamp, end_timestamp, stripe_index, stripe_count):
        """Creates a blob name"""
        return "{dbname}_{type}_{start}--{end}_S{idx:03d}-{cnt:03d}.cdmp".format(
            dbname=dbname,
            type=Naming.backup_type_str(is_full),
            start=start_timestamp,
            end=end_timestamp,
            idx=int(stripe_index),
            cnt=int(stripe_count))

    @staticmethod
    def construct_ddlgen_name(dbname, start_timestamp):
        """Constructs a ddlgen filename."""
        return "{dbname}_ddlgen_{start}.sql".format(
            dbname=dbname, start=start_timestamp)

    @staticmethod
    def parse_filename(filename):
        """Parses a filename."""
        match = re.search(r'(?P<dbname>\S+?)_(?P<type>full|tran)_(?P<start>\d{8}_\d{6})_S(?P<idx>\d+)-(?P<cnt>\d+)\.cdmp', filename)
        if match is None:
            return None

        return match.group('dbname'), Naming.type_str_is_full(match.group('type')), match.group('start'), int(match.group('idx')), int(match.group('cnt'))

    @staticmethod
    def parse_blobname(filename):
        """Parses a blob name."""
        match = re.search(r'(?P<dbname>\S+?)_(?P<type>full|tran)_(?P<start>\d{8}_\d{6})--(?P<end>\d{8}_\d{6})_S(?P<idx>\d+)-(?P<cnt>\d+)\.cdmp', filename)
        if match is None:
            return None
        return (match.group('dbname'), Naming.type_str_is_full(match.group('type')), match.group('start'), match.group('end'), int(match.group('idx')), int(match.group('cnt')))

    @staticmethod
    def parse_ase_generated_filename(filename):
        """Returns the filename parts of ASE-generated TRAN dumps."""
        match = re.search(r'(?P<dbname>.+?)_trans_(?P<start>\d{8}_\d{6})_S(?P<idx>\d+)-(?P<cnt>\d+)\.cdmp', filename)
        if match is None:
            return None
        return (match.group('dbname'), match.group('start'), int(match.group('idx')), int(match.group('cnt')))

    @staticmethod
    def blobname_to_filename(blobname):
        parts = Naming.parse_blobname(blobname)
        return Naming.construct_filename(
            dbname=parts[0],
            is_full=parts[1],
            start_timestamp=parts[2],
            # skip parts[3] which is end-timestamp
            stripe_index=parts[4],
            stripe_count=parts[5])

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""Unit tests for Naming."""
import unittest
from asebackupcli.naming import Naming

class TestNaming(unittest.TestCase):
    """Unit tests for class Naming."""

    def test_backup_type_str(self):
        """Test Naming.backup_type_str"""
        self.assertEqual(
            Naming.backup_type_str(is_full=True),
            'full')
        self.assertEqual(
            Naming.backup_type_str(is_full=False),
            'tran')

    def test_type_str_is_full(self):
        """Test Naming.type_str_is_full"""
        self.assertEqual(
            Naming.type_str_is_full('full'),
            True)
        self.assertEqual(
            Naming.type_str_is_full('tran'),
            False)

    def test_construct_filename(self):
        """Test Naming.construct_filename"""
        self.assertEqual(
            Naming.construct_filename(dbname="test1db", is_full=True,
                                      start_timestamp="20180601_112429",
                                      stripe_index=2, stripe_count=101),
            'test1db_full_20180601_112429_S002-101.cdmp')
        self.assertEqual(
            Naming.construct_filename(dbname="test1db", is_full=False,
                                      start_timestamp="20180601_112429",
                                      stripe_index=2, stripe_count=101),
            'test1db_tran_20180601_112429_S002-101.cdmp')

    def test_construct_blobname_prefix(self):
        """Test Naming.construct_blobname_prefix"""
        self.assertEqual(
            Naming.construct_blobname_prefix(dbname="test1db", is_full=True),
            'test1db_full_')

    def test_construct_blobname(self):
        """Test Naming.construct_blobname"""
        self.assertEqual(
            Naming.construct_blobname(
                dbname="test1db",
                is_full=True,
                start_timestamp="20180601_112429",
                end_timestamp="20180601_131234",
                stripe_index=2,
                stripe_count=101),
            'test1db_full_20180601_112429--20180601_131234_S002-101.cdmp')

    def test_parse_filename(self):
        """Test Naming.parse_filename"""
        self.assertEqual(
            Naming.parse_filename('test1db_full_20180601_112429_S002-101.cdmp'),
            ('test1db', True, '20180601_112429', 2, 101))
        self.assertEqual(
            Naming.parse_filename('test1db_tran_20180601_112429_S02-08.cdmp'),
            ('test1db', False, '20180601_112429', 2, 8))
        self.assertEqual(
            Naming.parse_filename('bad_input') is None,
            True)

    def test_parse_blobname(self):
        """Test Naming.parse_blobname"""
        self.assertEqual(
            Naming.parse_blobname('test1db_full_20180601_112429--20180601_131234_S002-101.cdmp'),
            ('test1db', True, '20180601_112429', '20180601_131234', 2, 101)
        )
        self.assertEqual(
            Naming.parse_blobname('test1db_tran_20180601_112429--20180601_131234_S2-008.cdmp'),
            ('test1db', False, '20180601_112429', '20180601_131234', 2, 8)
        )
        self.assertEqual(
            Naming.parse_filename('bad_input'),
            None
        )

    def test_pipe_names(self):
        """Test Naming.pipe_names"""
        self.assertEqual(
            Naming.pipe_names(dbname='AZU', is_full=True, stripe_count=3, output_dir='/tmp'),
            [
                '/tmp/backup_AZU_full_001_003.cdmp_pipe',
                '/tmp/backup_AZU_full_002_003.cdmp_pipe',
                '/tmp/backup_AZU_full_003_003.cdmp_pipe'
            ]
        )

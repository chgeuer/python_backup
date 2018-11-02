# coding=utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""Unit tests for DatabaseConnector."""
import unittest
from asebackupcli.databaseconnector import DatabaseConnector

class TestDatabaseConnector(unittest.TestCase):
    """Unit tests for class DatabaseConnector."""

    def test___sql_statement_create_backup_for_filenames(self):
        """Test for DatabaseConnector.sql_statement_create_backup_for_filenames"""
        self.assertEqual(
            DatabaseConnector.sql_statement_create_backup_for_filenames(
                dbname="AZU",
                is_full=True,
                files=[
                    "/tmp/pipe0",
                    "/tmp/pipe1"
                ]),
            '''use master
go
dump database AZU to '/tmp/pipe0'
    stripe on '/tmp/pipe1'
with compression = '101'

if @@error = 0
begin
  print 'ASE_AZURE_BACKUP_SUCCESS'
end

go
'''
        )

    def test___sql_statement_create_backup_1(self):
        """Test for DatabaseConnector.sql_statement_create_backup_for_filenames"""
        self.assertEqual(
            DatabaseConnector.sql_statement_create_backup(
                output_dir="/tmp",
                dbname="AZU",
                is_full=True,
                start_timestamp="20180629_124500",
                stripe_count=1),
            '''use master
go
dump database AZU to '/tmp/AZU_full_20180629_124500_S001-001.cdmp'
with compression = '101'

if @@error = 0
begin
  print 'ASE_AZURE_BACKUP_SUCCESS'
end

go
'''
        )

    def test___sql_statement_create_backup_2(self):
        """Test for DatabaseConnector.sql_statement_create_backup_for_filenames"""
        self.assertEqual(
            DatabaseConnector.sql_statement_create_backup(
                output_dir="/tmp",
                dbname="AZU",
                is_full=True,
                start_timestamp="20180629_124500",
                stripe_count=4),
            '''use master
go
dump database AZU to '/tmp/AZU_full_20180629_124500_S001-004.cdmp'
    stripe on '/tmp/AZU_full_20180629_124500_S002-004.cdmp'
    stripe on '/tmp/AZU_full_20180629_124500_S003-004.cdmp'
    stripe on '/tmp/AZU_full_20180629_124500_S004-004.cdmp'
with compression = '101'

if @@error = 0
begin
  print 'ASE_AZURE_BACKUP_SUCCESS'
end

go
'''
        )

    def test___sql_statement_create_backup_3(self):
        """Test for DatabaseConnector.sql_statement_create_backup_for_filenames"""
        self.assertEqual(
            DatabaseConnector.sql_statement_create_backup(
                output_dir="/tmp",
                dbname="AZU",
                is_full=False,
                start_timestamp="20180629_124500",
                stripe_count=1),
            '''use master
go
dump transaction AZU to '/tmp/AZU_tran_20180629_124500_S001-001.cdmp'
with compression = '101'

if @@error = 0
begin
  print 'ASE_AZURE_BACKUP_SUCCESS'
end

go
'''
        )

    def test___sql_statement_create_backup_4(self):
        """Test for DatabaseConnector.sql_statement_create_backup_for_filenames"""
        self.assertEqual(
            DatabaseConnector.sql_statement_create_backup(
                output_dir="/tmp",
                dbname="AZU",
                is_full=False,
                start_timestamp="20180629_124500",
                stripe_count=4),
            '''use master
go
dump transaction AZU to '/tmp/AZU_tran_20180629_124500_S001-004.cdmp'
    stripe on '/tmp/AZU_tran_20180629_124500_S002-004.cdmp'
    stripe on '/tmp/AZU_tran_20180629_124500_S003-004.cdmp'
    stripe on '/tmp/AZU_tran_20180629_124500_S004-004.cdmp'
with compression = '101'

if @@error = 0
begin
  print 'ASE_AZURE_BACKUP_SUCCESS'
end

go
'''
        )

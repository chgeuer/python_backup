# coding=utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""Unit tests for BackupAgent."""
import unittest
from asebackupcli.backupagent import BackupAgent
from asebackupcli.businesshours import BusinessHours
from .test_businesshours import TestBusinessHours

class TestBackupAgent(unittest.TestCase):
    """Unit tests for class BackupAgent."""

    def test_should_run_full_backup(self):
        """Test BackupAgent.should_run_full_backup"""
        sample_data = TestBusinessHours.sample_data()
        business_hours = BusinessHours.parse_tag_str(sample_data)
        db_backup_interval_min = business_hours.min
        db_backup_interval_max = business_hours.max

        five_day_backup = "20180601_010000"
        two_day_backup = "20180604_010000"
        same_day_backup = "20180606_010000"
        during_business_hours = "20180606_150000"
        outside_business_hours = "20180606_220000"

        # Forced
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=during_business_hours,
                force=True,
                latest_full_backup_timestamp=same_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            True
        )

        # Forced
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=during_business_hours,
                force=True,
                latest_full_backup_timestamp=two_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            True
        )

        # Forced
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=during_business_hours,
                force=True,
                latest_full_backup_timestamp=five_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            True
        )

        # respecting business hours, and not needed anyway
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=during_business_hours,
                force=False,
                latest_full_backup_timestamp=same_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            False
        )

        # respecting business hours
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=during_business_hours,
                force=False,
                latest_full_backup_timestamp=two_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            False
        )

        # a really old backup, so we ignore business hours
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=during_business_hours,
                force=False,
                latest_full_backup_timestamp=five_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            True
        )
        
        # outside_business_hours, but same_day_backup, so no backup
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=outside_business_hours,
                force=False,
                latest_full_backup_timestamp=same_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            False
        )
        
        # outside_business_hours and need to backup
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=outside_business_hours,
                force=False,
                latest_full_backup_timestamp=two_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            True
        )

        # a really old backup
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=outside_business_hours,
                force=False,
                latest_full_backup_timestamp=five_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            True
        )

        # Forced
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=outside_business_hours,
                force=True,
                latest_full_backup_timestamp=same_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            True
        )

        # Forced
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=outside_business_hours,
                force=True,
                latest_full_backup_timestamp=two_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            True
        )

        # Forced
        self.assertEqual(
            BackupAgent.should_run_full_backup(
                now_time=outside_business_hours,
                force=True,
                latest_full_backup_timestamp=five_day_backup,
                business_hours=business_hours,
                db_backup_interval_min=db_backup_interval_min,
                db_backup_interval_max=db_backup_interval_max),
            True
        )

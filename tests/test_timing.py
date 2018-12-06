# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""Unit tests for timing."""
import time
import datetime
import unittest
from asebackupcli.timing import Timing

class TestTiming(unittest.TestCase):
    """Unit tests for class Timing."""

    def test_parse(self):
        """Test parse."""
        res = Timing.parse("20180605_215959")
        self.assertEqual(
            time.struct_time(
                (2018, 6, 5, 21, 59, 59, 1, 156, -1)),
            res)

    def test_time_diff(self):
        """Test time_diff."""
        self.assertEqual(
            Timing.time_diff("20180106_120000", "20180106_120010"),
            datetime.timedelta(0, 10))
        self.assertEqual(
            Timing.time_diff("20180106_110000", "20180106_120010"),
            datetime.timedelta(0, 3610))

    def test_restore_files(self):
        """Test restore computation"""
        sample_times = [
            ('FULL', '20180110_110000', '1/2'), ('FULL', '20180110_110000', '2/2'),
            ('TRAN', '20180110_111500', '1/1'),
            ('TRAN', '20180110_113000', '1/2'), ('TRAN', '20180110_113000', '2/2'),
            ('TRAN', '20180110_114500', '1/1'),
            ('FULL', '20180110_120000', '1/2'), ('FULL', '20180110_120000', '2/2'),
            ('TRAN', '20180110_121500', '1/1'),
            ('TRAN', '20180110_123000', '1/2'), ('TRAN', '20180110_123000', '2/2'),
            ('TRAN', '20180110_124500', '1/1')
        ]

        restore = lambda restore_point: Timing.files_needed_for_recovery(
            sample_times, restore_point,
            select_end_date=lambda x: x[1],
            select_is_full=lambda x: x[0] == 'FULL')

        self.assertEqual(restore('20180110_110100'), [
            ('FULL', '20180110_110000', '1/2'), ('FULL', '20180110_110000', '2/2'),
            ('TRAN', '20180110_111500', '1/1')])
        self.assertEqual(restore('20180110_120100'), [
            ('FULL', '20180110_120000', '1/2'), ('FULL', '20180110_120000', '2/2'),
            ('TRAN', '20180110_121500', '1/1')])
        self.assertEqual(restore('20180110_121600'), [
            ('FULL', '20180110_120000', '1/2'), ('FULL', '20180110_120000', '2/2'),
            ('TRAN', '20180110_121500', '1/1'),
            ('TRAN', '20180110_123000', '1/2'), ('TRAN', '20180110_123000', '2/2')])
        self.assertEqual(restore('20180110_123200'), [
            ('FULL', '20180110_120000', '1/2'), ('FULL', '20180110_120000', '2/2'),
            ('TRAN', '20180110_121500', '1/1'),
            ('TRAN', '20180110_123000', '1/2'), ('TRAN', '20180110_123000', '2/2'),
            ('TRAN', '20180110_124500', '1/1')])

    def test_restore_files_only_full_backup(self):
        """Test restore computation for only full backups"""
        f1_s1 = ('FULL', '20181205_134002', '20181205_134113', '1/2')
        f1_s2 = ('FULL', '20181205_134002', '20181205_134113', '2/2')
        f2_s1 = ('FULL', '20181205_135001', '20181205_135044', '1/2')
        f2_s2 = ('FULL', '20181205_135001', '20181205_135044', '2/2')
        sample_times = [f1_s1, f1_s2, f2_s1, f2_s2]

        restore = lambda restore_point: Timing.files_needed_for_recovery(
            sample_times, restore_point,
            select_end_date=lambda x: x[2],
            select_is_full=lambda x: x[0] == 'FULL')

        self.assertEqual(restore('20181205_134113'), [f1_s1, f1_s2], msg="exact end time")
        self.assertEqual(restore('20181205_135130'), [f2_s1, f2_s2], msg="after F2")
        self.assertEqual(restore('20181205_134530'), [f1_s1, f1_s2], msg="in between F1 and F2")

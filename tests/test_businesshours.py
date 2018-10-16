# coding=utf-8

"""Unit tests for BusinessHours."""
import unittest
from asebackupcli.businesshours import BusinessHours

class TestBusinessHours(unittest.TestCase):
    """Unit tests for class BusinessHours."""

    @staticmethod
    def sample_data():
        """Sample data"""
        return (
            "bkp_db_schedule:"
            "mo:111111111000000000011111, "
            "tu:111111111000000000011111, "
            "we:111111111000000000011111, "
            "th:111111111000000000011111, "
            "fr:111111111000000000011111, "
            "sa:111111111111111111111111, "
            "su:111111111111111111111111, "
            "min:1d, "
            "max:3d"
        )

    def test___init__(self):
        """Test BusinessHours.__init__"""
        sample_data = TestBusinessHours.sample_data()
        parsed = BusinessHours.parse_tag_str(sample_data)

        weekday = [True, True, True, True, True, True, True, True,
                   True, False, False, False, False, False, False, False,
                   False, False, False, True, True, True, True, True]
        weekend = [True, True, True, True, True, True, True, True,
                   True, True, True, True, True, True, True, True,
                   True, True, True, True, True, True, True, True]

        self.assertEqual(parsed.hours[1], weekday)
        self.assertEqual(parsed.hours[2], weekday)
        self.assertEqual(parsed.hours[3], weekday)
        self.assertEqual(parsed.hours[4], weekday)
        self.assertEqual(parsed.hours[5], weekday)
        self.assertEqual(parsed.hours[6], weekend)
        self.assertEqual(parsed.hours[7], weekend)

    def test_parse_tag_str(self):
        """Test BusinessHours.parse_tag_str"""
        sample_data = TestBusinessHours.sample_data()
        self.assertEqual(
            BusinessHours.parse_tag_str(sample_data).tags['mo'],
            '111111111000000000011111')

    def test_parse_day(self):
        """Test BusinessHours.parse_day"""
        self.assertEqual(
            BusinessHours.parse_day('111111 111000 000000 011111'),
            [
                True, True, True, True, True, True,
                True, True, True, False, False, False,
                False, False, False, False, False, False,
                False, True, True, True, True, True
            ])

    def test_is_backup_allowed_dh(self):
        """Test BusinessHours.is_backup_allowed_dh"""
        sample_data = TestBusinessHours.sample_data()
        sample_hours = BusinessHours.parse_tag_str(sample_data)
        self.assertEqual(sample_hours.is_backup_allowed_dh(day=1, hour=4), True)
        self.assertEqual(sample_hours.is_backup_allowed_dh(day=1, hour=11), False)
        self.assertEqual(sample_hours.is_backup_allowed_dh(day=7, hour=11), True)

    def test_is_backup_allowed_time(self):
        """Test BusinessHours.is_backup_allowed_time"""
        sample_data = TestBusinessHours.sample_data()
        sample_hours = BusinessHours.parse_tag_str(sample_data)
        some_tuesday_evening = "20180605_215959"
        self.assertEqual(sample_hours.is_backup_allowed_time(some_tuesday_evening), True)
        some_tuesday_noon = "20180605_115500"
        self.assertEqual(sample_hours.is_backup_allowed_time(some_tuesday_noon), False)
        some_sunday_noon = "20180610_115500"
        self.assertEqual(sample_hours.is_backup_allowed_time(some_sunday_noon), True)

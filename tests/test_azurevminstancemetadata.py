# coding=utf-8

"""Unit tests for AzureVMInstanceMetadata."""
import json
import unittest
import datetime
from asebackupcli.azurevminstancemetadata import AzureVMInstanceMetadata
from asebackupcli.businesshours import BusinessHours

class TestAzureVMInstanceMetadata(unittest.TestCase):
    """Unit tests for class AzureVMInstanceMetadata."""

    @staticmethod
    def instance_metadata_filename():
        return "tests/instancemetadata.json"

    @staticmethod
    def demo_tag():
        """Demo data"""
        with open(TestAzureVMInstanceMetadata.instance_metadata_filename(),
                  mode='rt') as config_file:
            json_meta = config_file.read()
            return str(json.JSONDecoder().decode(json_meta)['compute']['tags'])

    @staticmethod
    def demo_instance_metadata():
        """Demo data"""
        with open(TestAzureVMInstanceMetadata.instance_metadata_filename(),
                  mode='rt') as config_file:
            json_meta = config_file.read()
            return AzureVMInstanceMetadata(lambda: (json.JSONDecoder()).decode(json_meta))

    def test_read_data(self):
        """Parsing VM instance metadata"""
        meta = TestAzureVMInstanceMetadata.demo_instance_metadata()

        self.assertEqual(meta.subscription_id, "724467b5-bee4-484b-bf13-d6a5505d2b51")
        self.assertEqual(meta.resource_group_name, "backuptest")
        self.assertEqual(meta.vm_name, "somevm")

        db_business_hours = BusinessHours(tags=meta.get_tags())
        self.assertEqual(db_business_hours.min, datetime.timedelta(1))
        self.assertEqual(db_business_hours.max, datetime.timedelta(3))

        db_business_hours = BusinessHours(tags=meta.get_tags(), schedule="bkp_db_schedule")
        self.assertEqual(db_business_hours.min, datetime.timedelta(1))
        self.assertEqual(db_business_hours.max, datetime.timedelta(3))

        log_business_hours = BusinessHours(tags=meta.get_tags(), schedule="bkp_log_schedule")
        self.assertEqual(log_business_hours.min, datetime.timedelta(0, 600))
        self.assertEqual(log_business_hours.max, datetime.timedelta(0, 1800))

        fs_business_hours = BusinessHours(tags=meta.get_tags(), schedule="bkp_fs_schedule")
        self.assertEqual(fs_business_hours.min, datetime.timedelta(7))
        self.assertEqual(fs_business_hours.max, datetime.timedelta(8))

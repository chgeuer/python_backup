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

        self.assertEqual(meta.vm_name, "somevm")
        self.assertEqual(meta.subscription_id, "724467b5-bee4-484b-bf13-d6a5505d2b51")

        business_hours = BusinessHours(tags=meta.get_tags(), schedule="bkp_fs_schedule")
        self.assertEqual(business_hours.min, datetime.timedelta(7))
        self.assertEqual(business_hours.max, datetime.timedelta(8))

# coding=utf-8
# pylint: disable=c0301

import urllib2
import json
from .backupexception import BackupException

def lazy_property(fn):
    """Decorator that makes a property lazy-evaluated."""
    attr_name = '_lazy_' + fn.__name__
    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

class AzureVMInstanceMetadata(object):
    """Read Azure VM instance metadata."""

    @staticmethod
    def request_metadata(api_version="2017-12-01"):
        url = "http://169.254.169.254/metadata/instance?api-version={v}".format(v=api_version)
        try:
            return json.loads(
                urllib2.urlopen(
                    urllib2.Request(
                        url, None, {'metadata': 'true'})).read())
        except Exception as exception:
            raise BackupException(
                "Failed to connect to Azure instance metadata endpoint {}:\n{}".format(
                    url, exception.message))

    @staticmethod
    def create_instance():
        real_request = lambda : AzureVMInstanceMetadata.request_metadata()
        return AzureVMInstanceMetadata(real_request)

    def __init__(self, req):
        self.req = req

    @lazy_property
    def json_data(self):
        return self.req()

    def get_tags(self):
        try:
            tags_value = str(self.json_data['compute']['tags'])
            if tags_value is None:
                return dict()
            return dict(kvp.split(":", 1) for kvp in tags_value.split(";"))
        except Exception as exception:
            raise BackupException(
                "Cannot parse tags value from instance metadata endpoint: {}".format(
                    exception.message))

    @property
    def subscription_id(self):
        """The subscription ID"""
        try:
            return str(self.json_data["compute"]["subscriptionId"])
        except Exception:
            raise BackupException(
                "Cannot read subscriptionId from instance metadata endpoint")

    @property
    def resource_group_name(self):
        try:
            return str(self.json_data["compute"]["resourceGroupName"])
        except Exception:
            raise BackupException(
                "Cannot read resourceGroupName from instance metadata endpoint")

    @property
    def location(self):
        try:
            return str(self.json_data["compute"]["location"])
        except Exception:
            raise BackupException(
                "Cannot read location from instance metadata endpoint")

    @property
    def vm_name(self):
        """Return the virtual machine's name."""
        try:
            return str(self.json_data["compute"]["name"])
        except Exception as e:
            raise BackupException(
                "Cannot read VM name from instance metadata endpoint: {}".format(e.message))

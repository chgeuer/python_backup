# coding=utf-8
# pylint: disable=c0301

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import urllib2
import json
import os
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
    def set_no_proxy_for_instance_metadata_endpoint():
        #
        # The instance metadata endpoint must be contacted directly, even if an HTTP proxy is set.
        #
        IMDS_IP = '169.254.169.254'
        vars_to_handle = []
        for no_proxy_key in ['NO_PROXY', 'no_proxy']:
            if os.environ.has_key(no_proxy_key):
                vars_to_handle.append(no_proxy_key)
        if vars_to_handle == []:
            vars_to_handle.append('NO_PROXY')
        for no_proxy_key in vars_to_handle:
            if not os.environ.has_key(no_proxy_key):
                os.environ[no_proxy_key] = IMDS_IP
            elif not IMDS_IP in os.environ[no_proxy_key]:
                os.environ[no_proxy_key] = os.environ[no_proxy_key] + ',' + IMDS_IP

    @staticmethod
    def request_metadata(api_version="2017-12-01"):
        AzureVMInstanceMetadata.set_no_proxy_for_instance_metadata_endpoint()

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
        except Exception as exception:
            raise BackupException(
                "Cannot read VM name from instance metadata endpoint: {}".format(exception.message))

    @property
    def vm_id(self):
        """Return the virtual machine's unique ID."""
        # https://github.com/MicrosoftDocs/azure-docs/blob/2dc1e3beced45a75d6410a3edff33449fff1ebb3/articles/virtual-machines/virtual-machines-linux-unique-vm-id.md
        # Similar to `sudo dmidecode | grep UUID`, with Big Endian bit ordering corrected
        try:
            return str(self.json_data["compute"]["vmId"])
        except Exception as exception:
            raise BackupException(
                "Cannot read VM ID from instance metadata endpoint: {}".format(exception.message))

# coding=utf-8
# pylint: disable=c0301

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

import re
import logging
import os.path
from .backupexception import BackupException

class BackupConfigurationFile(object):
    """Parse the backup configuration file."""

    def __init__(self, filename):
        if not os.path.isfile(filename):
            raise BackupException("Cannot find configuration file {}:".format(filename))
        self.filename = filename

        try:
            vals = BackupConfigurationFile.read_key_value_file(filename=self.filename)
            logging.debug("Configuration {}".format(str(vals)))
        except Exception as exception:
            raise BackupException("Error parsing config file {}:\n{}".format(
                filename, exception.message))

    def get_value(self, key):
        """ Parse a configuration file and return a single value."""
        values = BackupConfigurationFile.read_key_value_file(filename=self.filename)
        return values[key]

    def get_keys_prefix(self, prefix):
        """Retrieve all keys that match a certain prefix."""
        values = BackupConfigurationFile.read_key_value_file(filename=self.filename)
        return [i for i in values.keys() if re.match(prefix, i)]

    def key_exists(self, key):
        """Return True if a key exists in the configuration file."""
        values = BackupConfigurationFile.read_key_value_file(filename=self.filename)
        return values.has_key(key)

    @staticmethod
    def read_key_value_file(filename):
        """
            >>> values = BackupConfigurationFile.read_key_value_file(filename="config.txt.template")
            >>> values["sap.CID"]
            'AZU'
        """
        with open(filename, mode='rt') as config_file:
            lines = config_file.readlines()
            # skip comments and empty lines
            content_lines = [line for line in lines if not re.match(r"^\s*#|^\s*$", line)]
            content = dict(re.split(":|=", line, maxsplit=1) for line in content_lines)
            return dict([(x[0].strip(), x[1].strip()) for x in content.iteritems()])

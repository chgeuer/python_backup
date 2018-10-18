# coding=utf-8
"""Backup configuration module"""

import os
import logging
from pkgutil import get_data
from azure.storage.blob import BlockBlobService
from msrestazure.azure_active_directory import MSIAuthentication
from .azurevminstancemetadata import AzureVMInstanceMetadata
from .backupconfigurationfile import BackupConfigurationFile
from .businesshours import BusinessHours
from .backupexception import BackupException

class BackupConfiguration(object):
    """Access to the backup configuration."""

    def __init__(self, config_filename, machine_config_filename="/usr/sap/backup/backup.conf"):
        """
            >>> cfg = BackupConfiguration(config_filename="config.txt")
            >>> cfg.get_value("sap.CID")
            'ABC'
            >>> cfg.get_db_backup_interval_min()
            datetime.timedelta(1)
            >>> some_tuesday_evening = "20180605_215959"
            >>> cfg.get_db_business_hours().is_backup_allowed_time(some_tuesday_evening)
            True
        """
        self.db_config_file = BackupConfigurationFile(filename=config_filename)
        self.machine_config_file = BackupConfigurationFile(filename=machine_config_filename)
        self.instance_metadata = AzureVMInstanceMetadata.create_instance()
        self._block_blob_service = None

    def db_config_file_value(self, name):
        """Get value from tool-specific configuration."""
        try:
            return self.db_config_file.get_value(name)
        except Exception:
            raise BackupException("Cannot read value {} from config file '{}'".format(
                name, self.db_config_file.filename
            ))

    def machine_config_file_value(self, name):
        """Get value from machine-wide backup configuration."""
        try:
            return self.machine_config_file.get_value(name)
        except Exception:
            raise BackupException("Cannot read value {} from config file '{}'".format(
                name, self.machine_config_file.filename
            ))

    def instance_metadata_tag_value(self, name):
        """Get value from instance metadata tag."""
        try:
            return self.instance_metadata.get_tags()[name]
        except Exception:
            raise BackupException("Cannot read value {} from VM's tag configuration".format(name))

    def environment_value(self, name):
        """Get value from OS environment variable."""
        if not os.environ.has_key(name):
            return None
        return os.environ[name]

    def get_vm_name(self):
        """Get VM name."""
        return self.instance_metadata.vm_name

    def get_subscription_id(self):
        """Get Azure Subscription ID"""
        return self.instance_metadata.subscription_id

    def get_resource_group_name(self):
        """Get Resource Group name."""
        return self.instance_metadata.resource_group_name

    def get_location(self):
        """Get location."""
        return self.instance_metadata.location

    def get_db_business_hours(self):
        """Get database full backup business hours."""
        return BusinessHours(
            tags=self.instance_metadata.get_tags(),
            schedule="bkp_db_schedule")

    def get_log_business_hours(self):
        """Get database transaction log backup business hours."""
        return BusinessHours(
            tags=self.instance_metadata.get_tags(),
            schedule="bkp_log_schedule")

    def get_db_backup_interval_min(self):
        """Get minimum database full backup interval."""
        return self.get_db_business_hours().min

    def get_db_backup_interval_max(self):
        """Get maximum database full backup interval."""
        return self.get_db_business_hours().max

    def get_log_backup_interval_min(self):
        """Get minimum transaction log backup interval."""
        return self.get_log_business_hours().min

    def get_customer_id(self):
        """Get the customer ID (CID)"""
        return self.machine_config_file_value("DEFAULT.CID").strip('"')

    def get_system_id(self):
        """Get the system ID (SID)"""
        return self.machine_config_file_value("DEFAULT.SID").strip('"')

    def get_db_server_name(self):
        """Get the database server name"""
        if self.db_config_file.key_exists("server_name"):
            return self.db_config_file_value("server_name").strip('"')
        return self.get_system_id()

    def get_standard_local_directory(self):
        return self.db_config_file_value("local_temp_directory")

    def get_database_password_generator(self):
        return self.db_config_file_value("database_password_generator")

    def get_notification_command(self):
        return self.db_config_file_value("notification_command")

    def get_notification_template(self):
        """Path and filename of the template for backup notifications."""
        if self.db_config_file.key_exists("notification_template_file"):
            filename = self.db_config_file_value("notification_template_file").strip('"')
            return open(filename, 'rt').read()

        return get_data("asebackupcli", "notification.json")

    def get_databases_to_skip(self):
        return ["dbccdb"]

    def get_azure_storage_account_name(self):
        """
        Get storage account name. It can be specified explicitly in a
        instance metadata tag, otherwise is assembled using configuration
        information.
        """
        try:
            account = self.instance_metadata.get_tags()['bkp_storage_account']
            logging.debug("Using storage account name from instance metadata: %s", account)
        except Exception:
            cid = self.get_customer_id().lower()
            name = self.get_vm_name()[0:5]
            account = "sa{}{}backup0001".format(name, cid)
            logging.debug("No storage account in instance metadata, using generated: %s", account)
        return account

    @property
    def azure_storage_container_name(self):
        """
        Get storage container name. It can be specified explicitly in the
        configuration file, otherwise will default to the VM name.
        """
        if self.db_config_file.key_exists('azure.storage.container_name'):
            return self.db_config_file_value('azure.storage.container_name')
        return self.get_vm_name()

    @property
    def azure_storage_container_name_temp(self):
        """
        Get the storage container name for the container which is not immutable.
        """
        return self.azure_storage_container_name + "temp"

    @property
    def storage_client(self):
        if not self._block_blob_service:
            account_name = self.get_azure_storage_account_name()
            #
            # Use the Azure Managed Service Identity ('MSI') to fetch an Azure AD token to talk to Azure Storage (PREVIEW!!!)
            #
            cloud_environment_storage_suffix = 'core.windows.net'
            token_credential = MSIAuthentication(
                resource='https://{account_name}.blob.{cloud_environment_storage_suffix}'.format(
                    account_name=account_name,
                    cloud_environment_storage_suffix=cloud_environment_storage_suffix))
            # pylint: disable=unexpected-keyword-arg
            self._block_blob_service = BlockBlobService(
                account_name=account_name, token_credential=token_credential)
            # pylint: enable=unexpected-keyword-arg
        return self._block_blob_service

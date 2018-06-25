# coding=utf-8

from azure.storage.blob import BlockBlobService

from .azurevminstancemetadata import AzureVMInstanceMetadata
from .backupconfigurationfile import BackupConfigurationFile
from .businesshours import BusinessHours
from .scheduleparser import ScheduleParser

class BackupConfiguration:
    def __init__(self, config_filename):
        """
            >>> cfg = BackupConfiguration(config_filename="config.txt")
            >>> cfg.get_value("sap.CID")
            'ABC'
            >>> cfg.get_db_backup_interval_min()
            datetime.timedelta(1)
            >>> some_tuesday_evening = "20180605_215959"
            >>> cfg.get_business_hours().is_backup_allowed_time(some_tuesday_evening)
            True
        """
        self.cfg_file = BackupConfigurationFile(filename=config_filename)
        self.instance_metadata = AzureVMInstanceMetadata.create_instance()
        self._block_blob_service = None

        #
        # This dict contains function callbacks (lambdas) to return the value based on the current value
        #
        self.data = {
            "sap.CID": lambda: self.cfg_file.get_value("sap.CID"),
            "sap.SID": lambda: self.cfg_file.get_value("sap.SID"),
            "sap.ase.version": lambda: self.cfg_file.get_value("sap.ase.version"),
            "local_temp_directory": lambda: self.cfg_file.get_value("local_temp_directory"),
            "azure.storage.account_name": lambda: self.cfg_file.get_value("azure.storage.account_name"),
            "azure.storage.account_key": lambda: self.cfg_file.get_value("azure.storage.account_key"),
            "azure.storage.container_name": lambda: self.cfg_file.get_value("azure.storage.container_name"),

            "vm_name": lambda: self.instance_metadata.vm_name,
            "subscription_id": lambda: self.instance_metadata.subscription_id,
            "resource_group_name": lambda: self.instance_metadata.resource_group_name,

            "db_backup_interval_min": lambda: ScheduleParser.parse_timedelta(self.instance_metadata.get_tags()["db_backup_interval_min"]),
            "db_backup_interval_max": lambda: ScheduleParser.parse_timedelta(self.instance_metadata.get_tags()["db_backup_interval_max"]),
            "log_backup_interval_min": lambda: ScheduleParser.parse_timedelta(self.instance_metadata.get_tags()["log_backup_interval_min"]),

            "backup.businesshours": lambda: BusinessHours(self.instance_metadata.get_tags())
        }

    def get_value(self, key): return self.data[key]()
    def get_vm_name(self): return self.get_value("vm_name")
    def get_subscription_id(self): return self.get_value("subscription_id")
    def get_resource_group_name(self): return self.get_value("resource_group_name")
    def get_CID(self): return self.get_value("sap.CID")
    def get_SID(self): return self.get_value("sap.SID")
    def get_ase_version(self): return self.get_value("sap.ase.version")
    def get_db_backup_interval_min(self): return self.get_value("db_backup_interval_min")
    def get_db_backup_interval_max(self): return self.get_value("db_backup_interval_max")
    def get_log_backup_interval_min(self): return self.get_value("log_backup_interval_min")
    def get_business_hours(self): return self.get_value("backup.businesshours")
    def get_standard_local_directory(self): return self.get_value("local_temp_directory")

    def get_databases_to_skip(self): return [ "dbccdb" ]

    def __get_azure_storage_account_name(self): return self.get_value("azure.storage.account_name")
    def __get_azure_storage_account_key(self): return self.get_value("azure.storage.account_key")

    @property 
    def azure_storage_container_name(self): return self.get_value("azure.storage.container_name")

    @property
    def storage_client(self):
        if not self._block_blob_service:
            self._block_blob_service = BlockBlobService(
                account_name=self.__get_azure_storage_account_name(), 
                account_key=self.__get_azure_storage_account_key())
            _created = self._block_blob_service.create_container(container_name=self.azure_storage_container_name)
        return self._block_blob_service


# coding=utf-8

import requests
import json
from .backupexception import BackupException

class AzureVMInstanceMetadata:
    @staticmethod
    def request_metadata(api_version="2017-12-01"):
        url="http://169.254.169.254/metadata/instance?api-version={v}".format(v=api_version)
        try:
            response = requests.get(url=url, headers={"Metadata": "true"})
            return response.json()
        except Exception as e:
            raise(Exception("Failed to connect to Azure instance metadata endpoint {}\n\n{}".format(url, e.message)))

    @staticmethod
    def create_instance():
        """
            >>> json_meta = '{ "compute": { "subscriptionId": "724467b5-bee4-484b-bf13-d6a5505d2b51", "resourceGroupName": "backuptest", "name": "somevm", "tags":"db_backup_interval_min:24h;db_backup_interval_max:3d;log_backup_interval_min:600s;log_backup_interval_max:30m;db_backup_window_1:111111 111000 000000 011111;db_backup_window_2:111111 111000 000000 011111;db_backup_window_3:111111 111000 000000 011111;db_backup_window_4:111111 111000 000000 011111;db_backup_window_5:111111 111000 000000 011111;db_backup_window_6:111111 111111 111111 111111;db_backup_window_7:111111 111111 111111 111111" } }'
            >>> meta = AzureVMInstanceMetadata(lambda: (json.JSONDecoder()).decode(json_meta))
            >>> meta.vm_name
            'somevm'
        """
        #return AzureVMInstanceMetadata(lambda: (json.JSONDecoder()).decode('{ "compute": { "subscriptionId": "724467b5-bee4-484b-bf13-d6a5505d2b51", "resourceGroupName": "backuptest", "name": "somevm", "tags":"db_backup_interval_min:24h;db_backup_interval_max:3d;log_backup_interval_min:600s;log_backup_interval_max:30m;db_backup_window_1:111111 111000 000000 011111;db_backup_window_2:111111 111000 000000 011111;db_backup_window_3:111111 111000 000000 011111;db_backup_window_4:111111 111000 000000 011111;db_backup_window_5:111111 111000 000000 011111;db_backup_window_6:111111 111111 111111 111111;db_backup_window_7:111111 111111 111111 111111" } }'))
        return AzureVMInstanceMetadata(lambda: AzureVMInstanceMetadata.request_metadata())

    def __init__(self, req):
        self.req = req

    def json(self):
        return self.req()

    def get_tags(self):
        tags_value = str(self.json()['compute']['tags'])
        if tags_value == None:
            return dict()
        return dict(kvp.split(":", 1) for kvp in (tags_value.split(";")))

    @property
    def subscription_id(self): return str(self.req()["compute"]["subscriptionId"])

    @property
    def resource_group_name(self): return str(self.req()["compute"]["resourceGroupName"])

    @property
    def vm_name(self): return str(self.req()["compute"]["name"])

import requests

class AzureVMInstanceMetadata:
    @staticmethod
    def request_metadata(api_version="2017-12-01"):
        url="http://169.254.169.254/metadata/instance?api-version={v}".format(v=api_version)
        response = requests.get(url=url, headers={"Metadata": "true"})
        return response.json()

    @staticmethod
    def create_instance():
        """
            >>> json_meta = '{ "compute": { "subscriptionId": "724467b5-bee4-484b-bf13-d6a5505d2b51", "resourceGroupName": "backuptest", "name": "somevm", "tags":"db.backup.interval.min:24h;db.backup.interval.max:3d;log.backup.interval.min:600s;log.backup.interval.max:30m;db.backup.window.1:111111 111000 000000 011111;db.backup.window.2:111111 111000 000000 011111;db.backup.window.3:111111 111000 000000 011111;db.backup.window.4:111111 111000 000000 011111;db.backup.window.5:111111 111000 000000 011111;db.backup.window.6:111111 111111 111111 111111;db.backup.window.7:111111 111111 111111 111111" } }'
            >>> meta = AzureVMInstanceMetadata(lambda: (json.JSONDecoder()).decode(json_meta))
            >>> meta.vm_name
            'somevm'
        """
        return AzureVMInstanceMetadata(lambda: AzureVMInstanceMetadata.request_metadata())

    def __init__(self, req):
        self.req = req
        first_json = self.req()
        self.subscription_id = str(first_json["compute"]["subscriptionId"])
        self.resource_group_name = str(first_json["compute"]["resourceGroupName"])
        self.vm_name = str(first_json["compute"]["name"])

    def json(self):
        return self.req()

    def get_tags(self):
        tags_value = str(self.json()['compute']['tags'])
        return dict(kvp.split(":", 1) for kvp in (tags_value.split(";")))

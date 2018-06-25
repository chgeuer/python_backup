import logging
import io
import os

from .funcmodule import printe
#from azure.storage.blob import BlockBlobService

class PipeUploader:
    def __init__(self, blob_client, pipe_path, container_name, blob_name):
        self.blob_client = blob_client
        self.pipe_path = pipe_path
        self.container_name = container_name
        self.blob_name = blob_name

    def run(self):
        # open with O_RDONLY
        # Nonblocking I/O is possible by using the fcntl(2) F_SETFL operation to enable the O_NONBLOCK open file status flag.

        print("Create pipe {}".format(self.pipe_path))
        os.mkfifo(self.pipe_path)

        print("Start upload")

        self.blob_client.create_blob_from_path(
            container_name=self.container_name,
            file_path=self.pipe_path,
            blob_name=self.blob_name,
            validate_content=True,
            max_connections=4)

        print("Remove pipe")
        os.remove(self.pipe_path)

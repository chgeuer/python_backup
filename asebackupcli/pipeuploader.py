import logging
import io
import os

from .funcmodule import printe
from azure.storage.blob import BlockBlobService

class PipeUploader:
    def __init__(self, blob_client, pipe_path, container_name, blob_name):
        self.blob_client = blob_client
        self.pipe_path = pipe_path
        self.container_name = container_name
        self.blob_name = blob_name


    def run(self):
        os.mkfifo(self.pipe_path, 0666)
        with io.open(self.pipe_path, 'rb') as file:
            stream = io.BufferedReader(file)
            self.blob_client.create_blob_from_stream(self.container_name, self.blob_name, stream)

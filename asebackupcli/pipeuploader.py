import logging
import threading
import io
import os

from .funcmodule import printe
#from azure.storage.blob import BlockBlobService

class PipeUploader:
    def __init__(self, blob_client, container_name, stripe_count):
        self.blob_client = blob_client
        self.container_name = container_name
        self.stripe_count = stripe_count

    def run(self):
        threads = []
        for i in range(self.stripe_count - 1):
            pipe_path = "{i}.pipe".format(i=i)
            os.mkfifo(pipe_path)
            print("Create pipe {}".format(pipe_path))

            blob_name = "{i}.txt".format(i=i)
            thread = threading.Thread(
                target=self.upload,
                args=(pipe_path, blob_name, ))
            threads.append(thread)

        [t.start() for t in threads]
        print("Started {} threads".format(len(threads)))

        [t.join() for t in threads]
        print("Finished {} threads".format(len(threads)))

    def upload(self, pipe_path, blob_name):
        print("Start upload for {}".format(pipe_path))
        with open(pipe_path, "rb", buffering=0) as stream:
            self.blob_client.create_blob_from_stream(
                container_name=self.container_name,
                blob_name=blob_name, 
                stream=stream,
                use_byte_buffer=True,
                max_connections=1)

            print("Finished {}".format(pipe_path))
            os.remove(pipe_path)

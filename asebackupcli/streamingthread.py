# coding=utf-8

import os
import logging
import threading

class StreamingThread(threading.Thread):
    def __init__(self, storage_client, container_name, blob_name, pipe_path):
        super(StreamingThread, self).__init__()
        
        self.storage_client = storage_client
        self.container_name = container_name
        self.blob_name = blob_name
        self.pipe_path = pipe_path
        self.exception = None

    def get_exception(self):
        return self.exception

    def start(self):
        try:
            logging.debug("Start streaming upload for {} to {}/{}".format(
                self.pipe_path, self.container_name, self.blob_name))
            with open(self.pipe_path, "rb", buffering=0) as stream:
                #
                # For streaming to work, we need to ensure that 
                # use_byte_buffer=True and 
                # max_connections=1 are set
                #
                self.blob_client.create_blob_from_stream(
                    container_name=self.container_name,
                    blob_name=self.blob_name, stream=stream,
                    use_byte_buffer=True, max_connections=1)
                logging.debug("Finished streaming upload of {}/{}".format(self.container_name, self.blob_name))
                os.remove(self.pipe_path)

                logging.debug("Finished streaming upload of {}/{}".format(self.container_name, self.blob_name))
        except Exception as e:
            self.exception = e

    def stop(self):
        logging.debug("Requested cancellation of upload to {}/{}".format(self.container_name, self.blob_name))

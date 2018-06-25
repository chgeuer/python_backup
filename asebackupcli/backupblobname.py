# coding=utf-8

from .naming import Naming

class BackupBlobName:
    def __init__(self, blobname):
        self.blobname = blobname
        parts = Naming.parse_blobname(self.blobname)
        (dbname, is_full, start_timestamp, end_timestamp, stripe_index, stripe_count) = parts
        self.dbname = dbname
        self.is_full = is_full
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.stripe_index = stripe_index
        self.stripe_count = stripe_count

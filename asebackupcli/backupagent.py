# coding=utf-8
# pylint: disable=c0301

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

"""Backup agent module"""

import logging
import os
import os.path
import datetime
from itertools import groupby
import subprocess

from .__init__ import version
from .funcmodule import printe, out, log_stdout_stderr
from .naming import Naming
from .timing import Timing
from .databaseconnector import DatabaseConnector
from .backupexception import BackupException
from .streamingthread import StreamingThread

class BackupAgent(object):
    """The backup business logic implementation."""
    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration
        self.database_connector = DatabaseConnector(self.backup_configuration)

    def existing_backups_for_db(self, dbname, is_full):
        existing_blobs_dict = dict()
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                prefix=Naming.construct_blobname_prefix(dbname=dbname, is_full=is_full),
                marker=marker)
            for blob in results:
                blob_name = blob.name
                parts = Naming.parse_blobname(blob_name)
                if parts is None:
                    continue

                end_time_of_existing_blob = parts[3]
                if not existing_blobs_dict.has_key(end_time_of_existing_blob):
                    existing_blobs_dict[end_time_of_existing_blob] = []

                val = {'blob_name': blob_name, 'content_length': blob.properties.content_length}
                existing_blobs_dict[end_time_of_existing_blob].append(val)

            if results.next_marker:
                marker = results.next_marker
            else:
                break
        return existing_blobs_dict

    def existing_backups(self, databases=[]):
        existing_blobs_dict = dict()
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                marker=marker)

            for blob in results:
                blob_name = blob.name
                parts = Naming.parse_blobname(blob_name)
                if parts is None:
                    continue

                (dbname_of_existing_blob, _is_full, _start_timestamp, end_time_of_existing_blob, _stripe_index, _stripe_count) = parts
                if len(databases) == 0 or dbname_of_existing_blob in databases:
                    if not existing_blobs_dict.has_key(end_time_of_existing_blob):
                        existing_blobs_dict[end_time_of_existing_blob] = []
                    val = {'blob_name': blob_name, 'content_length': blob.properties.content_length}
                    existing_blobs_dict[end_time_of_existing_blob].append(val)

            if results.next_marker:
                marker = results.next_marker
            else:
                break
        return existing_blobs_dict

    def latest_backup_timestamp(self, dbname, is_full):
        existing_blobs_dict = self.existing_backups_for_db(dbname=dbname, is_full=is_full)
        if len(existing_blobs_dict.keys()) == 0:
            return "19000101_000000"
        return Timing.sort(existing_blobs_dict.keys())[-1:][0]

    @staticmethod
    def should_run_full_backup(now_time, force, latest_full_backup_timestamp, business_hours, db_backup_interval_min, db_backup_interval_max):
        """Determine whether a backup should be executed."""
        allowed_by_business = business_hours.is_backup_allowed_time(now_time)
        age_of_latest_backup_in_storage = Timing.time_diff(latest_full_backup_timestamp, now_time)
        min_interval_allows_backup = age_of_latest_backup_in_storage > db_backup_interval_min
        max_interval_requires_backup = age_of_latest_backup_in_storage > db_backup_interval_max
        perform_full_backup = (allowed_by_business and min_interval_allows_backup or max_interval_requires_backup or force)

        # logging.info("Full backup requested. Current time: {now}. Last backup in storage: {last}. Age of backup {age}".format(now=now_time, last=latest_full_backup_timestamp, age=age_of_latest_backup_in_storage))
        # logging.info("Backup requirements: min=\"{min}\" max=\"{max}\"".format(min=db_backup_interval_min,max=db_backup_interval_max))
        # logging.info("Forced by user: {force}. Backup allowed by business hours: {allowed_by_business}. min_interval_allows_backup={min_interval_allows_backup}. max_interval_requires_backup={max_interval_requires_backup}".format(force=force, allowed_by_business=allowed_by_business, min_interval_allows_backup=min_interval_allows_backup, max_interval_requires_backup=max_interval_requires_backup))
        # logging.info("Decision to backup: {perform_full_backup}.".format(perform_full_backup=perform_full_backup))

        return perform_full_backup

    @staticmethod
    def should_run_tran_backup(now_time, force, latest_tran_backup_timestamp, log_backup_interval_min):
        if force:
            return True

        age_of_latest_backup_in_storage = Timing.time_diff(latest_tran_backup_timestamp, now_time)
        min_interval_allows_backup = age_of_latest_backup_in_storage > log_backup_interval_min
        perform_tran_backup = min_interval_allows_backup
        return perform_tran_backup

    def should_run_backup(self, dbname, is_full, force, start_timestamp):
        if is_full:
            result = BackupAgent.should_run_full_backup(
                now_time=start_timestamp,
                force=force,
                latest_full_backup_timestamp=self.latest_backup_timestamp(dbname=dbname, is_full=is_full),
                business_hours=self.backup_configuration.get_db_business_hours(),
                db_backup_interval_min=self.backup_configuration.get_db_backup_interval_min(),
                db_backup_interval_max=self.backup_configuration.get_db_backup_interval_max())
        else:
            result = BackupAgent.should_run_tran_backup(
                now_time=start_timestamp,
                force=force,
                latest_tran_backup_timestamp=self.latest_backup_timestamp(dbname=dbname, is_full=is_full),
                log_backup_interval_min=self.backup_configuration.get_log_backup_interval_min())

        return result

    def backup(self, is_full, databases, output_dir, force, skip_upload, use_streaming):
        databases_to_backup = self.database_connector.determine_databases(user_selected_databases=databases, is_full=is_full)
        skip_dbs = self.backup_configuration.get_databases_to_skip()
        # databases_to_backup = filter(lambda db: not (db in skip_dbs), databases_to_backup)
        databases_to_backup = [db for db in databases_to_backup if not db in skip_dbs]

        for dbname in databases_to_backup:
            self.backup_single_db(dbname=dbname, is_full=is_full, force=force, skip_upload=skip_upload, output_dir=output_dir, use_streaming=use_streaming)

        if not is_full and not skip_upload and not use_streaming:
            self.upload_local_backup_files_from_previous_operations(output_dir=output_dir)

    def start_streaming_threads(self, dbname, is_full, start_timestamp, stripe_count, output_dir, container_name):
        threads = []
        for stripe_index in range(1, stripe_count + 1):
            pipe_path = Naming.pipe_name(output_dir=output_dir,
                                         dbname=dbname,
                                         is_full=is_full,
                                         stripe_index=stripe_index,
                                         stripe_count=stripe_count)
            blob_name = Naming.construct_filename(dbname=dbname,
                                                  is_full=is_full,
                                                  start_timestamp=start_timestamp,
                                                  stripe_index=stripe_index,
                                                  stripe_count=stripe_count)

            if os.path.exists(pipe_path):
                logging.warning("Remove old pipe file %s", pipe_path)
                os.remove(pipe_path)

            logging.debug("Create named pipe %s", pipe_path)

            # pylint: disable=no-member
            os.mkfifo(pipe_path)
            # pylint: enable=no-member

            t = StreamingThread(
                storage_client=self.backup_configuration.storage_client,
                container_name=container_name, blob_name=blob_name, pipe_path=pipe_path)
            threads.append(t)

        try:
            _ = [t.start() for t in threads]
            logging.debug("Started {} threads for upload".format(len(threads)))
            return threads
        except Exception as e:
            printe(e.message)

    def finalize_streaming_threads(self, threads):
        _ = [t.join() for t in threads]

    def streaming_backup_single_db(self, dbname, is_full, start_timestamp, stripe_count, output_dir):
        storage_client = self.backup_configuration.storage_client
        #
        # The container where backups end up (dest_container_name) could be set with an immutability policy. 
        # We have to "rename" the blobs with the end-times for restore logic to work.
        # Rename is non-existent in blob storage, so copy & delete
        #
        temp_container_name = self.backup_configuration.azure_storage_container_name_temp
        dest_container_name = self.backup_configuration.azure_storage_container_name

        threads = self.start_streaming_threads(
            dbname=dbname, is_full=is_full, start_timestamp=start_timestamp,
            stripe_count=stripe_count, output_dir=output_dir, container_name=temp_container_name)
        logging.debug("Start streaming backup SQL call")
        try:
            stdout, stderr, returncode = self.database_connector.create_backup_streaming(
                dbname=dbname, is_full=is_full, stripe_count=stripe_count,
                output_dir=output_dir)
        except BackupException:
            storage_client.delete_container(container_name=temp_container_name)
            _ = [t.stop() for t in threads]
            raise

        self.finalize_streaming_threads(threads)
        end_timestamp = Timing.now_localtime()

        #
        # Rename
        # - copy from temp_container_name/old_blob_name to dest_container_name/new_blob_name)
        #
        source_blobs = []
        copied_blobs = []
        for stripe_index in range(1, stripe_count + 1):
            old_blob_name = Naming.construct_filename(dbname=dbname, is_full=is_full, start_timestamp=start_timestamp, stripe_index=stripe_index, stripe_count=stripe_count)
            source_blobs.append(old_blob_name)
            new_blob_name = Naming.construct_blobname(dbname=dbname, is_full=is_full, start_timestamp=start_timestamp, end_timestamp=end_timestamp, stripe_index=stripe_index, stripe_count=stripe_count)
            copied_blobs.append(new_blob_name)

            copy_source = storage_client.make_blob_url(temp_container_name, old_blob_name)
            storage_client.copy_blob(dest_container_name, new_blob_name, copy_source=copy_source)

        #
        # Wait for all copies to succeed
        #
        get_all_copy_statuses = lambda: map(lambda b: storage_client.get_blob_properties(dest_container_name, b).properties.copy.status, copied_blobs)
        while not all(status == "success" for status in get_all_copy_statuses()):
            logging.debug("Waiting for all blobs to be copied {}".format(get_all_copy_statuses()))

        #
        # Delete sources
        #
        _ =     [storage_client.delete_blob(temp_container_name, b) for b in source_blobs]

        return (stdout, stderr, returncode, end_timestamp)

    def file_backup_single_db(self, dbname, is_full, start_timestamp, stripe_count, output_dir):
        stdout, stderr, returncode = self.database_connector.create_backup(
            dbname=dbname, is_full=is_full, start_timestamp=start_timestamp,
            stripe_count=stripe_count, output_dir=output_dir)
        end_timestamp = Timing.now_localtime()

        return (stdout, stderr, returncode, end_timestamp)

    def backup_single_db(self, dbname, is_full, force, skip_upload, output_dir, use_streaming):
        previous_backup_timestamp = self.latest_backup_timestamp(dbname, is_full)

        start_timestamp = Timing.now_localtime()
        if not self.should_run_backup(dbname=dbname, is_full=is_full, force=force, start_timestamp=start_timestamp):
            out("Skip backup of database {}".format(dbname))
            return

        stripe_count = self.database_connector.determine_database_backup_stripe_count(
            dbname=dbname, is_full=is_full)

        backup_exception = None
        stdout = None
        stderr = None
        end_timestamp = None
        try:
            if not use_streaming:
                out("Start file-based backup for database {dbname}".format(dbname=dbname))
                stdout, stderr, _returncode, end_timestamp = self.file_backup_single_db(
                    dbname=dbname, is_full=is_full, start_timestamp=start_timestamp,
                    stripe_count=stripe_count, output_dir=output_dir)
                log_stdout_stderr(stdout, stderr)
            else:
                out("Start streaming-based backup for database {dbname}".format(dbname=dbname))
                stdout, stderr, _returncode, end_timestamp = self.streaming_backup_single_db(
                    dbname=dbname, is_full=is_full, start_timestamp=start_timestamp,
                    stripe_count=stripe_count, output_dir=output_dir)
                log_stdout_stderr(stdout, stderr)
        except BackupException as be:
            backup_exception = be

        success = stdout != None and DatabaseConnector.MAGIC_SUCCESS_STRING in stdout

        if success and backup_exception is None:
            out("Backup of {dbname} ({is_full}) ran from {start_timestamp} to {end_timestamp} with status {success}".format(
                dbname=dbname, is_full={True:"full DB", False:"transactions"}[is_full],
                start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                success={True:"success", False:"failure"}[success]))
        else:
            #
            # Clean up resources
            #
            for stripe_index in range(1, stripe_count + 1):
                file_name = Naming.construct_filename(dbname=dbname, is_full=is_full, start_timestamp=start_timestamp, stripe_index=stripe_index, stripe_count=stripe_count)
                file_path = os.path.join(output_dir, file_name)
                if os.path.exists(file_path):
                    os.remove(file_path)

                blob_name = Naming.construct_blobname(dbname=dbname, is_full=is_full, start_timestamp=start_timestamp, end_timestamp=end_timestamp, stripe_index=stripe_index, stripe_count=stripe_count)
                if self.backup_configuration.storage_client.exists(container_name=self.backup_configuration.azure_storage_container_name, blob_name=blob_name):
                    self.backup_configuration.storage_client.delete_blob(container_name=self.backup_configuration.azure_storage_container_name, blob_name=blob_name)

            message = None
            if not success:
                message = "SQL statement did not successfully end: \nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}".format(stdout=stdout, stderr=stderr)
            if backup_exception != None:
                message = backup_exception.message
            logging.fatal(message)
            raise BackupException(message)

        ddl_content = self.database_connector.create_ddlgen(dbname=dbname)
        ddlgen_file_name = Naming.construct_ddlgen_name(dbname=dbname, start_timestamp=start_timestamp)
        if not skip_upload:
            self.backup_configuration.storage_client.create_blob_from_text(
                container_name=self.backup_configuration.azure_storage_container_name,
                blob_name=ddlgen_file_name,
                text=ddl_content)
        else:
            local_ddlgen_file = os.path.join(output_dir, ddlgen_file_name)
            with open(local_ddlgen_file, 'wt') as ddlgen_file:
                ddlgen_file.write(ddl_content)
                out("Wrote database structure to {}".format(local_ddlgen_file))

        if not skip_upload and not use_streaming:
            #
            # After isql run, rename all generated dump files to the blob naming scheme (including end-time).
            #
            # If the machine reboots during an isql run, then that rename doesn't happen, and we do
            # not upload these potentially corrupt dump files
            #
            for stripe_index in range(1, stripe_count + 1):
                file_name = Naming.construct_filename(dbname=dbname, is_full=is_full, start_timestamp=start_timestamp, stripe_index=stripe_index, stripe_count=stripe_count)
                blob_name = Naming.construct_blobname(dbname=dbname, is_full=is_full, start_timestamp=start_timestamp, end_timestamp=end_timestamp, stripe_index=stripe_index, stripe_count=stripe_count)
                file_path = os.path.join(output_dir, file_name)
                blob_path = os.path.join(output_dir, blob_name)

                os.rename(file_path, blob_path)
                out("Move {} to Azure Storage".format(blob_path))
                self.backup_configuration.storage_client.create_blob_from_path(
                    container_name=self.backup_configuration.azure_storage_container_name,
                    file_path=blob_path, blob_name=blob_name, validate_content=True, max_connections=4)
                os.remove(blob_path)

        backup_size_in_bytes = 0
        if not skip_upload:
            for stripe_index in range(1, stripe_count + 1):
                blob_name = Naming.construct_blobname(dbname=dbname, is_full=is_full, start_timestamp=start_timestamp, end_timestamp=end_timestamp, stripe_index=stripe_index, stripe_count=stripe_count)
                blob_props = self.backup_configuration.storage_client.get_blob_properties(
                    container_name=self.backup_configuration.azure_storage_container_name,
                    blob_name=blob_name)
                backup_size_in_bytes += blob_props.properties.content_length
                print "Blob {n} has size {l}".format(n=blob_name, l=blob_props.properties.content_length)

        self.send_notification(
            dbname=dbname, is_full=is_full,
            previous_backup_timestamp=previous_backup_timestamp,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            success=success,
            overall_size_in_bytes=backup_size_in_bytes,
            use_streaming=use_streaming)

    def upload_local_backup_files_from_previous_operations(self, output_dir):
        """Upload ASE-generated cdmp files"""
        for existing_file in os.listdir(output_dir):
            parts = Naming.parse_ase_generated_filename(existing_file)
            file_path = os.path.join(output_dir, existing_file)
            if parts is None:
                #
                # seems not to be an ASE-generated transaction dump
                #
                continue

            (dbname, start_timestamp, stripe_index, stripe_count) = parts

            previous_backup_timestamp = self.latest_backup_timestamp(dbname, False)

            #
            # For ASE-generated dumps, we can get the start_timestamp from the filename.
            # For end_timestamp, we rely on file 'modification' time, i.e. when the file was closed
            #
            # -rw-r-----  1 sybaz3 sapsys     49152 2018-11-30 14:15:33.067917234 +0000 AZ3_trans_20181130_141532_S01-01.cdmp
            #
            utc_epoch = os.path.getmtime(file_path)
            end_timestamp = Timing.epoch_to_localtime_string(utc_epoch)

            blob_name = Naming.construct_blobname(dbname=dbname, is_full=False,
                                                  start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                                  stripe_index=stripe_index, stripe_count=stripe_count)

            out("Move ASE-generated dump '{file_path}' to Azure Storage as '{blob_name}'".format(file_path=file_path, blob_name=blob_name))
            backup_size_in_bytes = os.stat(file_path).st_size
            try:
                self.backup_configuration.storage_client.create_blob_from_path(
                    container_name=self.backup_configuration.azure_storage_container_name, file_path=file_path,
                    blob_name=blob_name, validate_content=True, max_connections=4)
                os.remove(file_path)
                self.send_notification(
                    dbname=dbname, is_full=False,
                    previous_backup_timestamp=previous_backup_timestamp,
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    success=True,
                    overall_size_in_bytes=backup_size_in_bytes,
                    use_streaming=False)
            except Exception as exception:
                self.send_notification(
                    dbname=dbname, is_full=False,
                    previous_backup_timestamp=previous_backup_timestamp,
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    success=False,
                    overall_size_in_bytes=backup_size_in_bytes,
                    use_streaming=False,
                    error_msg=exception.message)

    def list_backups(self, databases=[]):
        """Lists backups in the given storage account."""
        baks_dict = self.existing_backups(databases=databases)
        for end_timestamp in baks_dict:
            # http://mark-dot-net.blogspot.com/2014/03/python-equivalents-of-linq-methods.html
            stripes = baks_dict[end_timestamp]

            stripes = [{
                "parts": Naming.parse_blobname(x["blob_name"]),
                "content_length": x["content_length"]
            } for x in stripes]

            stripes = [{
                "dbname": x["parts"][0],
                "is_full": x["parts"][1],
                "begin": x["parts"][2],
                "end": x["parts"][3],
                "stripe_index": x["parts"][4],
                "content_length": x["content_length"]
            } for x in stripes]

            group_by_key = lambda x: "db {dbname: <30} start {begin} end {end} ({type})".format(
                dbname=x["dbname"], end=x["end"], begin=x["begin"], type=Naming.backup_type_str(x["is_full"]))

            for group, values in groupby(stripes, key=group_by_key):
                values = [x for x in values] # Expand interable
                print "{backup} {size:>20,} bytes, stripes: {files} ".format(
                    backup=group,
                    files=[s["stripe_index"] for s in values],
                    size=sum([s["content_length"] for s in values]))

    def prune_old_backups(self, older_than, databases):
        """Delete (prune) old backups from Azure storage."""
        minimum_deletable_age = datetime.timedelta(7, 0)
        logging.warn("Deleting files older than {}".format(older_than))
        if older_than < minimum_deletable_age:
            msg = "This script does not delete files younger than {}, ignoring this order".format(minimum_deletable_age)
            logging.warn(msg)
            return

        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                marker=marker)
            for blob in results:
                parts = Naming.parse_blobname(blob.name)
                if parts is None:
                    continue

                (dbname, _is_full, _start_timestamp, end_timestamp, _stripe_index, _stripe_count) = parts
                if (databases != None) and not dbname in databases:
                    continue

                diff = Timing.time_diff(end_timestamp, Timing.now_localtime())
                delete = diff > older_than

                if delete:
                    logging.warn("Deleting {}".format(blob.name))
                    self.backup_configuration.storage_client.delete_blob(
                        container_name=self.backup_configuration.azure_storage_container_name,
                        blob_name=blob.name)
                else:
                    logging.warn("Keeping {}".format(blob.name))

            if results.next_marker:
                marker = results.next_marker
            else:
                break

    def restore(self, restore_point, output_dir, databases):
        print "Retriving point-in-time restore {} for databases {}".format(restore_point, str(databases))
        databases = self.database_connector.determine_databases(user_selected_databases=databases, is_full=True)
        skip_dbs = self.backup_configuration.get_databases_to_skip()
        # databases = [db for db in databases if not db in skip_dbs]
        databases = filter(lambda db: not (db in skip_dbs), databases)
        for dbname in databases:
            self.restore_single_db(dbname=dbname, output_dir=output_dir, restore_point=restore_point)

    def download_ddlgen(self, dbname, start_timestamp, output_dir):
        """Download a database layout"""
        ddlgen_file_name = Naming.construct_ddlgen_name(dbname=dbname, start_timestamp=start_timestamp)
        ddlgen_file_path = os.path.join(output_dir, ddlgen_file_name)

        storage_client = self.backup_configuration.storage_client
        container_name = self.backup_configuration.azure_storage_container_name
        if storage_client.exists(container_name=container_name, blob_name=ddlgen_file_name):
            storage_client.get_blob_to_path(container_name=container_name, blob_name=ddlgen_file_name, file_path=ddlgen_file_path)
            out("Downloaded ddlgen description {}".format(ddlgen_file_path))

    def restore_single_db(self, dbname, restore_point, output_dir):
        """Restore a single database"""
        blobs = self.list_restore_blobs(dbname=dbname)
        times = [Naming.parse_blobname(b) for b in blobs]
        restore_files = Timing.files_needed_for_recovery(
            times, restore_point,
            select_end_date=lambda x: x[3],
            select_is_full=lambda x: x[1])

        storage_client = self.backup_configuration.storage_client
        container_name = self.backup_configuration.azure_storage_container_name
        for (_dbname, is_full, start_timestamp, end_timestamp, stripe_index, stripe_count) in restore_files:
            # For full database files, download the ddlgen SQL description, but only along with the 1st stripe
            if is_full and stripe_index == 1:
                self.download_ddlgen(dbname=dbname, start_timestamp=start_timestamp, output_dir=output_dir)

            blob_name = "{dbname}_{type}_{start}--{end}_S{idx:03d}-{cnt:03d}.cdmp".format(
                dbname=dbname, type=Naming.backup_type_str(is_full),
                start=start_timestamp, end=end_timestamp,
                idx=stripe_index, cnt=stripe_count)
            file_name = "{dbname}_{type}_{start}_S{idx:03d}-{cnt:03d}.cdmp".format(
                dbname=dbname, type=Naming.backup_type_str(is_full),
                start=start_timestamp, idx=stripe_index, cnt=stripe_count)

            file_path = os.path.join(output_dir, file_name)
            storage_client.get_blob_to_path(container_name=container_name, blob_name=blob_name, file_path=file_path)
            out("Downloaded dump {}".format(file_path))

    def list_restore_blobs(self, dbname):
        existing_blobs = []
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                prefix="{dbname}_".format(dbname=dbname),
                marker=marker)
            for blob in results:
                existing_blobs.append(blob.name)
            if results.next_marker:
                marker = results.next_marker
            else:
                break
        #
        # restrict to dump files
        #
        return [x for x in existing_blobs if x.endswith(".cdmp")]

    def show_configuration(self, output_dir):
        return "\n".join(self.get_configuration_printable(output_dir=output_dir))

    def get_configuration_printable(self, output_dir):
        business_hours = self.backup_configuration.get_db_business_hours()
        day_f = lambda d: [None, "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][d]
        b2s_f = lambda x: {True:"1", False:"0"}[x]
        s_f = lambda day: "business_hours_{}:                 {}".format(day_f(day), "".join(map(b2s_f, business_hours.hours[day])))
        hours = [s_f(day) for day in range(1, 8)]

        return [
            "azure.vm_name:                      {}".format(self.backup_configuration.get_vm_name()),
            "azure.vm_id:                        {}".format(self.backup_configuration.get_vm_id()),
            "azure.resource_group_name:          {}".format(self.backup_configuration.get_resource_group_name()),
            "azure.subscription_id:              {}".format(self.backup_configuration.get_subscription_id()),
            "",
            "sap.SID:                            {}".format(self.backup_configuration.get_system_id()),
            "sap.CID:                            {}".format(self.backup_configuration.get_customer_id()),
            "DB Server Name:                     {}".format(self.backup_configuration.get_db_server_name()),
            "Output dir:                         {}".format(output_dir),
            "",
            "skipped databases:                  {}".format(self.backup_configuration.get_databases_to_skip()),
            "db_backup_interval_min:             {}".format(self.backup_configuration.get_db_backup_interval_min()),
            "db_backup_interval_max:             {}".format(self.backup_configuration.get_db_backup_interval_max()),
            "log_backup_interval_min:            {}".format(self.backup_configuration.get_log_backup_interval_min())
        ] + hours + [
            "",
            "azure_storage_account_name:         {}".format(self.backup_configuration.get_azure_storage_account_name()),
            "azure_storage_container_name:       {}".format(self.backup_configuration.azure_storage_container_name),
            "azure_storage_container_name_temp:  {}".format(self.backup_configuration.azure_storage_container_name_temp),
            "notification_command:               {}".format(self.backup_configuration.get_notification_command()),
            "script version:                     {}".format(version())
        ]

    def send_notification(self, dbname, is_full, previous_backup_timestamp, start_timestamp, end_timestamp,
                          success, overall_size_in_bytes, use_streaming, error_msg=None):
        """Send notification"""
        try:
            notify_cmd = self.backup_configuration.get_notification_command()
            if notify_cmd is None:
                logging.error("Cannot send notification, 'notification_command' is not configured. ")
                return

            template = self.backup_configuration.get_notification_template()
            env = os.environ

            # Can use a `notification_command` like so:
            #
            # notification_command: /usr/bin/cat > ~/log-${dbname}-${start_timestamp}-${end_timestamp}.txt
            #
            env["start_timestamp"] = start_timestamp
            env["end_timestamp"] = end_timestamp
            env["dbname"] = dbname

            env["sap_cloud"] = "azure"
            env["sap_hostname"] = self.backup_configuration.get_vm_name()
            env["sap_instance_id"] = self.backup_configuration.get_vm_id()
            env["sap_state"] = {True:"success", False:"fail"}[success]
            env["sap_type"] = {True:"db", False:"log"}[is_full]
            env["sap_method"] = {True:"backint", False:"file"}[use_streaming]
            env["sap_level"] = {True:"full", False:""}[is_full] # full, incr, diff, ""
            env["sap_account_id"] = self.backup_configuration.get_subscription_id()
            env["sap_customer_id"] = self.backup_configuration.get_customer_id()
            env["sap_system_id"] = self.backup_configuration.get_system_id()
            env["sap_database_name"] = dbname
            env["sap_database_id"] = "" # Always empty
            env["sap_s3_path"] = "https://{accountname}.blob.core.windows.net/{container}/".format(
                accountname=self.backup_configuration.get_azure_storage_account_name(),
                container=self.backup_configuration.azure_storage_container_name)
            env["sap_timestamp_last_successful"] = str(Timing.local_string_to_utc_epoch(previous_backup_timestamp))
            env["sap_timestamp_bkp_begin"] = str(Timing.local_string_to_utc_epoch(start_timestamp))
            env["sap_timestamp_bkp_end"] = str(Timing.local_string_to_utc_epoch(end_timestamp))
            env["sap_timestamp_send"] = str(Timing.local_string_to_utc_epoch(Timing.now_localtime()))
            env["sap_backup_size"] = str(overall_size_in_bytes)
            env["sap_dbtype"] = "ase"
            if error_msg is None:
                env["sap_error_message"] = "Success"
            else:
                env["sap_error_message"] = str(error_msg)
            env["sap_script_version"] = version()

            print "Sending notification via '{notify_cmd}'".format(notify_cmd=notify_cmd)

            notify_process = subprocess.Popen(
                "/usr/bin/envsubst | {notify_cmd}".format(notify_cmd=notify_cmd),
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                shell=True, env=env)

            stdout, stderr = notify_process.communicate(template)
            returncode = notify_process.returncode
            return (stdout, stderr, returncode)
        except Exception as ex:
            msg = "Could not send notification: {}".format(ex.message)
            printe(msg)
            raise BackupException(msg)

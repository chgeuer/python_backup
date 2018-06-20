import logging
import os
import datetime
from itertools import groupby

from .funcmodule import printe
from .naming import Naming
from .timing import Timing
from .databaseconnector import DatabaseConnector

class BackupAgent:
    """
        The backup business logic implementation.
    """
    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration

    def existing_backups_for_db(self, dbname, is_full):
        existing_blobs_dict = dict()
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                prefix=Naming.construct_blobname_prefix(dbname=dbname, is_full=is_full), 
                marker=marker)
            for blob in results:
                blob_name=blob.name
                parts = Naming.parse_blobname(blob_name)
                if parts == None:
                    continue

                end_time_of_existing_blob = parts[3]
                if not existing_blobs_dict.has_key(end_time_of_existing_blob):
                    existing_blobs_dict[end_time_of_existing_blob] = []
                existing_blobs_dict[end_time_of_existing_blob].append(blob_name)

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
                blob_name=blob.name
                parts = Naming.parse_blobname(blob_name)
                if parts == None:
                    continue

                (dbname_of_existing_blob, _is_full, _start_timestamp, end_time_of_existing_blob, _stripe_index, _stripe_count) = parts
                if len(databases) == 0 or dbname_of_existing_blob in databases:
                    if not existing_blobs_dict.has_key(end_time_of_existing_blob):
                        existing_blobs_dict[end_time_of_existing_blob] = []
                    existing_blobs_dict[end_time_of_existing_blob].append(blob_name)

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

    def upload_local_backup_files_from_previous_operations(self, is_full, output_dir):
        print("Upload files from previous runs")
        for file in os.listdir(output_dir):
            parts = Naming.parse_blobname(file)
            if parts == None:
                continue
            (_dbname, is_full_file, _start_timestamp, _end_timestamp, _stripe_index, _stripe_count) = parts
            if (is_full != is_full_file):
                continue

            blob_name = file
            blob_path = os.path.join(output_dir, blob_name)

            self.backup_configuration.storage_client.create_blob_from_path(
                container_name=self.backup_configuration.azure_storage_container_name, 
                file_path=blob_path, blob_name=blob_name, 

                validate_content=True, max_connections=4)
            os.remove(blob_path)

    def full_backup(self, output_dir, force=False, skip_upload=False, databases=None):
        is_full=True
        database_connector = DatabaseConnector(self.backup_configuration)
        databases_to_backup = database_connector.determine_databases(databases, is_full=is_full)

        skip_dbs = self.backup_configuration.get_databases_to_skip()
        databases_to_backup = filter(lambda db: not (db in skip_dbs), databases_to_backup)

        for dbname in databases_to_backup:
            self.full_backup_single_db(
                dbname=dbname, 
                force=force, 
                skip_upload=skip_upload, 
                output_dir=output_dir)

        if not skip_upload:
            self.upload_local_backup_files_from_previous_operations(is_full=is_full, output_dir=output_dir)

    @staticmethod
    def should_run_full_backup(now_time, force, latest_full_backup_timestamp, business_hours, db_backup_interval_min, db_backup_interval_max):
        """
            Determine whether a backup should be executed. 

            >>> business_hours=BusinessHours.parse_tag_str(BusinessHours._BusinessHours__sample_data())
            >>> db_backup_interval_min=ScheduleParser.parse_timedelta("24h")
            >>> db_backup_interval_max=ScheduleParser.parse_timedelta("3d")
            >>> five_day_backup =                     "20180601_010000"
            >>> two_day_backup =                      "20180604_010000"
            >>> same_day_backup =                     "20180606_010000"
            >>> during_business_hours  = Timing.parse("20180606_150000")
            >>> outside_business_hours = Timing.parse("20180606_220000")
            >>> 
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=True, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=True, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=True, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # respecting business hours, and not needed anyway
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=False, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            False
            >>> # respecting business hours
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=False, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            False
            >>> # a really old backup, so we ignore business hours
            >>> BackupAgent.should_run_full_backup(now_time=during_business_hours, force=False, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # outside_business_hours, but same_day_backup, so no backup
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=False, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            False
            >>> # outside_business_hours and need to backup
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=False, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # a really old backup
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=False, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=True, latest_full_backup_timestamp=same_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=True, latest_full_backup_timestamp=two_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
            >>> # Forced
            >>> BackupAgent.should_run_full_backup(now_time=outside_business_hours, force=True, latest_full_backup_timestamp=five_day_backup, business_hours=business_hours, db_backup_interval_min=db_backup_interval_min, db_backup_interval_max=db_backup_interval_max)
            True
        """
        allowed_by_business = business_hours.is_backup_allowed_time(now_time)
        age_of_latest_backup_in_storage = Timing.time_diff(latest_full_backup_timestamp, Timing.datetime_to_timestr(now_time))
        min_interval_allows_backup = age_of_latest_backup_in_storage > db_backup_interval_min
        max_interval_requires_backup = age_of_latest_backup_in_storage > db_backup_interval_max
        perform_full_backup = (allowed_by_business and min_interval_allows_backup or max_interval_requires_backup or force)

        # logging.info("Full backup requested. Current time: {now}. Last backup in storage: {last}. Age of backup {age}".format(now=Timing.datetime_to_timestr(now_time), last=latest_full_backup_timestamp, age=age_of_latest_backup_in_storage))
        # logging.info("Backup requirements: min=\"{min}\" max=\"{max}\"".format(min=db_backup_interval_min,max=db_backup_interval_max))
        # logging.info("Forced by user: {force}. Backup allowed by business hours: {allowed_by_business}. min_interval_allows_backup={min_interval_allows_backup}. max_interval_requires_backup={max_interval_requires_backup}".format(force=force, allowed_by_business=allowed_by_business, min_interval_allows_backup=min_interval_allows_backup, max_interval_requires_backup=max_interval_requires_backup))
        # logging.info("Decision to backup: {perform_full_backup}.".format(perform_full_backup=perform_full_backup))

        return perform_full_backup

    def full_backup_single_db(self, dbname, force, skip_upload, output_dir):
        is_full=True
        if not BackupAgent.should_run_full_backup(
                now_time=Timing.now_localtime(), force=force, 
                latest_full_backup_timestamp=self.latest_backup_timestamp(dbname=dbname, is_full=is_full),
                business_hours=self.backup_configuration.get_business_hours(),
                db_backup_interval_min=self.backup_configuration.get_db_backup_interval_min(),
                db_backup_interval_max=self.backup_configuration.get_db_backup_interval_max()):
            logging.info("Skipping backup of database {dbname}".format(dbname=dbname))
            print("Skipping backup of database {dbname}".format(dbname=dbname))

            return

        db_connector = DatabaseConnector(self.backup_configuration)
        stripe_count = db_connector.determine_database_backup_stripe_count(
            dbname=dbname, is_full=is_full)

        start_timestamp = Timing.now_localtime()
        stdout, stderr = db_connector.create_backup(
            dbname=dbname, is_full=is_full,
            start_timestamp=start_timestamp,
            stripe_count=stripe_count,
            output_dir=output_dir)
        end_timestamp = Timing.now_localtime()

        logging.info(stdout)
        logging.warning(stderr)

        ddlgen_file_name=Naming.construct_ddlgen_name(dbname=dbname, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        ddlgen_file_path = os.path.join(output_dir, ddlgen_file_name)
        with open(ddlgen_file_path, mode='wt') as file:
            ddl_gen_sql = db_connector.create_ddlgen(
                dbname=dbname, 
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp)
            file.write(ddl_gen_sql)

        #
        # After isql run, rename all generated dump files to the blob naming scheme (including end-time). 
        #
        # If the machine reboots during an isql run, then that rename doesn't happen, and we do 
        # not upload these potentially corrupt dump files
        #
        for stripe_index in range(1, stripe_count + 1):
            file_name = Naming.construct_filename(
                dbname=dbname, is_full=is_full, 
                start_timestamp=start_timestamp,
                stripe_index=stripe_index, stripe_count=stripe_count)
            blob_name = Naming.construct_blobname(
                dbname=dbname, is_full=is_full, 
                start_timestamp=start_timestamp, end_timestamp=end_timestamp, 
                stripe_index=stripe_index, stripe_count=stripe_count)
            os.rename(
                os.path.join(output_dir, file_name), 
                os.path.join(output_dir, blob_name))

        if not skip_upload:
            # Upload & delete the SQL description
            self.backup_configuration.storage_client.create_blob_from_path(
                container_name=self.backup_configuration.azure_storage_container_name, 
                file_path=ddlgen_file_path, blob_name=ddlgen_file_name, 
                validate_content=True, max_connections=4)
            os.remove(ddlgen_file_path)

            # Upload & delete all stripes
            for stripe_index in range(1, stripe_count + 1):
                blob_name = Naming.construct_blobname(
                    dbname=dbname, 
                    is_full=is_full, 
                    start_timestamp=start_timestamp, 
                    end_timestamp=end_timestamp, 
                    stripe_index=stripe_index, 
                    stripe_count=stripe_count)
                blob_path = os.path.join(output_dir, blob_name)

                self.backup_configuration.storage_client.create_blob_from_path(
                    container_name=self.backup_configuration.azure_storage_container_name, 
                    file_path=blob_path, blob_name=blob_name, 
                    validate_content=True, max_connections=4)
                os.remove(blob_path)

    def transaction_backup(self, output_dir, force=False, skip_upload=False, databases=None):
        is_full=False
        database_connector = DatabaseConnector(self.backup_configuration)
        databases_to_backup = database_connector.determine_databases(databases, is_full=is_full)
        skip_dbs = self.backup_configuration.get_databases_to_skip()
        databases_to_backup = filter(lambda db: not (db in skip_dbs), databases_to_backup)

        for dbname in databases_to_backup:
            self.tran_backup_single_db(
                dbname=dbname, 
                output_dir=output_dir, 
                force=force,
                skip_upload=skip_upload)

        if not skip_upload:
            self.upload_local_backup_files_from_previous_operations(is_full=is_full, output_dir=output_dir)

    @staticmethod
    def should_run_tran_backup(now_time, force, latest_tran_backup_timestamp, log_backup_interval_min):
        if force:
            return True

        age_of_latest_backup_in_storage = Timing.time_diff(latest_tran_backup_timestamp, Timing.datetime_to_timestr(now_time))
        min_interval_allows_backup = age_of_latest_backup_in_storage > log_backup_interval_min
        perform_tran_backup = min_interval_allows_backup
        return perform_tran_backup 

    def tran_backup_single_db(self, dbname, output_dir, force, skip_upload):
        is_full=False
        if not BackupAgent.should_run_tran_backup(
                now_time=Timing.now_localtime(), 
                force=force,
                latest_tran_backup_timestamp=self.latest_backup_timestamp(dbname=dbname, is_full=is_full),
                log_backup_interval_min=self.backup_configuration.get_log_backup_interval_min()):

            log_msg="Skipping backup of transactions for {dbname}. (min='{min}' latest='{latest}' now='{now}'".format(dbname=dbname,
                min=self.backup_configuration.get_log_backup_interval_min(),
                latest=self.latest_backup_timestamp(dbname=dbname, is_full=is_full),
                now=Timing.now_localtime_string())
            logging.info(log_msg)
            print(log_msg)
            return

        db_connector = DatabaseConnector(self.backup_configuration)
        stripe_count = db_connector.determine_database_backup_stripe_count(
            dbname=dbname, is_full=is_full)

        start_timestamp = Timing.now_localtime()
        stdout, stderr = db_connector.create_backup(
            dbname=dbname, 
            is_full=is_full,
            start_timestamp=start_timestamp, 
            stripe_count=stripe_count, 
            output_dir=output_dir)
        end_timestamp = Timing.now_localtime()

        logging.info(stdout)
        logging.warning(stderr)

        #
        # After isql run, rename all generated dump files to the blob naming scheme (including end-time). 
        #
        # If the machine reboots during an isql run, then that rename doesn't happen, and we do 
        # not upload these potentially corrupt dump files
        #
        for stripe_index in range(1, stripe_count + 1):
            file_name = Naming.construct_filename(
                dbname=dbname, is_full=is_full,
                start_timestamp=start_timestamp,
                stripe_index=stripe_index, stripe_count=stripe_count)
            blob_name = Naming.construct_blobname(
                dbname=dbname, is_full=is_full,
                start_timestamp=start_timestamp, end_timestamp=end_timestamp, 
                stripe_index=stripe_index, stripe_count=stripe_count)
            os.rename(
                os.path.join(output_dir, file_name),
                os.path.join(output_dir, blob_name))

        if not skip_upload:
            for stripe_index in range(1, stripe_count + 1):
                blob_name = Naming.construct_blobname(
                    dbname=dbname,
                    is_full=is_full,
                    start_timestamp=start_timestamp, 
                    end_timestamp=end_timestamp, 
                    stripe_index=stripe_index, 
                    stripe_count=stripe_count)
                blob_path = os.path.join(output_dir, blob_name)

                self.backup_configuration.storage_client.create_blob_from_path(
                    container_name=self.backup_configuration.azure_storage_container_name, 
                    file_path=blob_path, blob_name=blob_name, 
                    validate_content=True, max_connections=4)
                os.remove(blob_path)

    def list_backups(self, databases = []):
        baks_dict = self.existing_backups(databases=databases)
        for end_timestamp in baks_dict.keys():
            # http://mark-dot-net.blogspot.com/2014/03/python-equivalents-of-linq-methods.html
            stripes = baks_dict[end_timestamp]
            stripes = map(lambda blobname: {
                    "blobname":blobname,
                    "filename": Naming.blobname_to_filename(blobname),
                    "parts": Naming.parse_blobname(blobname)
                }, stripes)
            stripes = map(lambda x: {
                    "blobname": x["blobname"],
                    "filename": x["filename"],
                    "parts": x["parts"],
                    "dbname": x["parts"][0],
                    "is_full": x["parts"][1],
                    "end": x["parts"][3],
                    "stripe_index": x["parts"][4],
                }, stripes)

            group_by_key=lambda x: "Database \"{dbname}\" ended {end} - {type} ".format(
                dbname=x["dbname"], end=x["end"], type=Naming.backup_type_str(x["is_full"]))

            for group, values in groupby(stripes, key=group_by_key): 
                files = list(map(lambda s: s["stripe_index"], values))
                print("{backup} {files}".format(backup=group, files=files))

    def prune_old_backups(self, older_than, databases):
        """
            Delete (prune) old backups from Azure storage. 
        """
        minimum_deletable_age = datetime.timedelta(7, 0)
        logging.warn("Deleting files older than {}".format(older_than))
        if (older_than < minimum_deletable_age):
            msg="This script does not delete files younger than {}, ignoring this order".format(minimum_deletable_age)

            logging.warn(msg)
            printe(msg)

            return

        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                marker=marker)
            for blob in results:
                parts = Naming.parse_blobname(blob.name)
                if (parts == None):
                    continue

                (dbname, _is_full, _start_timestamp, end_timestamp, _stripe_index, _stripe_count) = parts
                if (databases != None) and not (dbname in databases):
                    continue

                diff = Timing.time_diff(end_timestamp, Timing.now_localtime_string())
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
        database_connector = DatabaseConnector(self.backup_configuration)
        databases = database_connector.determine_databases(databases, is_full=True)
        skip_dbs = self.backup_configuration.get_databases_to_skip()
        databases = filter(lambda db: not (db in skip_dbs), databases)
        for dbname in databases:
            self.restore_single_db(dbname=dbname, output_dir=output_dir, restore_point=restore_point)

    def restore_single_db(self, dbname, restore_point, output_dir):
        blobs = self.list_restore_blobs(dbname=dbname)
        times = map(Naming.parse_blobname, blobs)
        restore_files = Timing.files_needed_for_recovery(times, restore_point, 
            select_end_date=lambda x: x[3], select_is_full=lambda x: x[1])

        storage_client = self.backup_configuration.storage_client
        for (dbname, is_full, start_timestamp, end_timestamp, stripe_index, stripe_count) in restore_files:
            if is_full:
                # For full database files, download the SQL description
                ddlgen_file_name=Naming.construct_ddlgen_name(dbname=dbname, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
                ddlgen_file_path=os.path.join(output_dir, ddlgen_file_name)
                if storage_client.exists(container_name=self.backup_configuration.azure_storage_container_name, blob_name=ddlgen_file_name):
                    storage_client.get_blob_to_path(
                        container_name=self.backup_configuration.azure_storage_container_name,
                        blob_name=ddlgen_file_name,
                        file_path=ddlgen_file_path)

            blob_name = "{dbname}_{type}_{start}--{end}_S{idx:03d}-{cnt:03d}.cdmp".format(
                dbname=dbname, type=Naming.backup_type_str(is_full), 
                start=start_timestamp, end=end_timestamp,
                idx=stripe_index, cnt=stripe_count)
            file_name = "{dbname}_{type}_{start}_S{idx:03d}-{cnt:03d}.cdmp".format(
                dbname=dbname, type=Naming.backup_type_str(is_full), 
                start=start_timestamp, idx=stripe_index, cnt=stripe_count)

            file_path = os.path.join(output_dir, file_name)
            storage_client.get_blob_to_path(
                container_name=self.backup_configuration.azure_storage_container_name,
                blob_name=blob_name,
                file_path=file_path)
            print("Downloaded {} to {}".format(blob_name, file_path))

    def list_restore_blobs(self, dbname):
        existing_blobs = []
        marker = None
        while True:
            results = self.backup_configuration.storage_client.list_blobs(
                container_name=self.backup_configuration.azure_storage_container_name,
                prefix="{dbname}_".format(dbname=dbname), 
                marker=marker)
            for blob in results:
                if not blob.name.endswith(".cdmp"):
                    continue

                existing_blobs.append(blob.name)
            if results.next_marker:
                marker = results.next_marker
            else:
                break
        return existing_blobs

    def show_configuration(self, output_dir):
        print("azure.vm_name:                      {}".format(self.backup_configuration.get_vm_name()))
        print("azure.resource_group_name:          {}".format(self.backup_configuration.get_resource_group_name()))
        print("azure.subscription_id:              {}".format(self.backup_configuration.get_subscription_id()))
        print("sap.SID:                            {}".format(self.backup_configuration.get_SID()))
        print("sap.CID:                            {}".format(self.backup_configuration.get_CID()))
        print("skipped databases:                  {}".format(self.backup_configuration.get_databases_to_skip()))
        print("db_backup_interval_min:             {}".format(self.backup_configuration.get_db_backup_interval_min()))
        print("db_backup_interval_max:             {}".format(self.backup_configuration.get_db_backup_interval_max()))
        print("log_backup_interval_min:            {}".format(self.backup_configuration.get_log_backup_interval_min()))
        business_hours = self.backup_configuration.get_business_hours()
        for day in range(1, 8):
            print("business_hours_{}:                 {}".format(["", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][day], 
                "".join(map(lambda x: {True:"1", False:"0"}[x], business_hours.hours[day]))))
        print("azure_storage_container_name:       {}".format(self.backup_configuration.azure_storage_container_name))
        print("azure_storage_account_name:         {}".format(self.backup_configuration._BackupConfiguration__get_azure_storage_account_name()))
        print("azure_storage_account_key:          {}...".format(self.backup_configuration._BackupConfiguration__get_azure_storage_account_key()[0:10]))
        print("ASE Version:                        {}".format(self.backup_configuration.get_ase_version()))
        print("ASE Directory:                      {}".format(DatabaseConnector(self.backup_configuration).get_ase_base_directory()))
        print("working directory:                  {}".format(output_dir))

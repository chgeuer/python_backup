#!/usr/bin/env python2.7


class TestMethods(unittest.TestCase):

    def test_when_to_run_a_full_backup(self):
        test_data = [
            [ False, { 'now': "20180604_005900", 'at': "01:00:00", 'age': 23*3600 } ], # One minute before official schedule do not run a full backup
            [ False, { 'now': "20180604_235959", 'at': "00:00:00", 'age': 23*3600 } ], # One minute before official schedule do not run a full backup
            [ False, { 'now': "20180604_010200", 'at': "01:00:00", 'age': 39 } ],      #  Two minutes after full backup schedule, skip, because the successful backup is 39 seconds old
            [ True,  { 'now': "20180604_010200", 'at': "01:00:00", 'age': 23*3600 } ], #  Two minutes after full backup schedule, run, because the successful backup is 23 hours old
            [ True,  { 'now': "20180604_000000", 'at': "00:00:00", 'age': 23*3600 } ], #  Two minutes after full backup schedule, run, because the successful backup is 23 hours old
            [ True,  { 'now': "20180604_000001", 'at': "00:00:00", 'age': 2 } ],       #  Two minutes after full backup schedule, run, because the successful backup is from before scheduled time
        ]
        for d in test_data:
            print("Try now={now} at={at} age={age}".format(now=Timing.parse(d[1]['now']), at=d[1]['at'], age=d[1]['age']))
            self.assertEqual(d[0], Timing.should_run_regular_backup_now(
                fullbackupat_str=d[1]['at'],
                age_of_last_backup_in_seconds=d[1]['age'],
                now=Timing.parse(d[1]['now'])
            ))

class BackupTimestampBlob:
    def __init__(self, storage_cfg, instance_metadata, is_full):
        self.storage_cfg=storage_cfg
        self.instance_metadata=instance_metadata
        self.is_full=is_full
        self.blob_name="{subscription_id}-{resource_group_name}-{vm_name}-{type}.json".format(
            subscription_id=self.instance_metadata.subscription_id,
            resource_group_name=self.instance_metadata.resource_group_name,
            vm_name=self.instance_metadata.vm_name,
            type=Naming.backup_type_str(self.is_full)
        )

    def age_of_last_backup_in_seconds(self):
        return Timing.time_diff_in_seconds(self.read(), Timing.now())

    def full_backup_required(self):
        age_of_last_backup_in_seconds = self.age_of_last_backup_in_seconds()

        now=time.gmtime()
        now_epoch=calendar.timegm(now)

        sched = time.strptime(self.instance_metadata.fullbackupat, "%H:%M:%S")
        planned_today=datetime.datetime(
            now.tm_year, now.tm_mon, now.tm_mday, 
            sched.tm_hour, sched.tm_min, sched.tm_sec).utctimetuple()
        planned_today_epoch=calendar.timegm(planned_today)

        if age_of_last_backup_in_seconds > self.instance_metadata.maximum_age_still_respect_business_hours:
            return True

        if (now_epoch < planned_today_epoch):
            return False

        return age_of_last_backup_in_seconds > now_epoch - planned_today_epoch

    def write(self):
        self.storage_cfg.block_blob_service.create_blob_from_text(
            container_name=self.storage_cfg.container_name, 
            blob_name=self.blob_name,
            encoding="utf-8",
            content_settings=ContentSettings(content_type="application/json"),
            text=(json.JSONEncoder()).encode({ 
                "backup_type": Naming.backup_type_str(self.is_full), 
                "utc_time": Timing.now()
            })
        )

    def read(self):
        try:
            blob=self.storage_cfg.block_blob_service.get_blob_to_text(
                container_name=self.storage_cfg.container_name, 
                blob_name=self.blob_name,
                encoding="utf-8"
            )
            return (json.JSONDecoder()).decode(blob.content)["utc_time"]
        except AzureMissingResourceHttpError:
            return "19000101_000000"

class BackupAgent:
    def __init__(self, filename):
        self.storage_cfg = StorageConfiguration(filename)
        self.instance_metadata = AzureVMInstanceMetadata.create_instance()

    def backup(self):
        timestamp_file_full = BackupTimestampBlob(
            storage_cfg=self.storage_cfg, 
            instance_metadata=self.instance_metadata, 
            is_full=True)

        full_backup_required=timestamp_file_full.full_backup_required()

        full_backup_was_already_running=False
        if full_backup_required:
            try:
                with pid.PidFile(pidname='backup-ase-full', piddir=".") as _p:
                    logging.info("Run full backup")
                    self.do_full_backup(timestamp_file_full)
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip full backup, already running")
                full_backup_was_already_running=True

        if full_backup_was_already_running or not full_backup_required:
            try:
                with pid.PidFile(pidname='backup-ase-tran', piddir=".") as _p:
                    logging.info("Run transaction log backup")
                    self.do_transaction_backup()
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip transaction log backup, already running")

    def do_full_backup(self, timestamp_file_full):
        logging.info("Last full backup : {age_in_seconds} secs ago".format(
            age_in_seconds=timestamp_file_full.age_of_last_backup_in_seconds()))

        start == now()
        subprocess.check_output(["./isql.py", "-f"])
        end == now()

        filename_in_local_disk_bak      = "{start}.cdmp"
        filename_in_blob_storage =        "{start}__{end}__cdmp"
        filename_in_local_disk_restore  = "{start}.cdmp"

        source = "."
        pattern = "*.cdmp"
        # TODO only for full files
        for filename in glob.glob1(dirname=source, pattern=pattern):
            file_path = os.path.join(source, filename)
            exists = self.storage_cfg.block_blob_service.exists(
                container_name=self.storage_cfg.container_name, 
                blob_name=filename)
            if not exists:
                logging.info("Upload {}".format(filename))
                self.storage_cfg.block_blob_service.create_blob_from_path(
                    container_name=self.storage_cfg.container_name, 
                    blob_name=filename, file_path=file_path,
                    validate_content=True, max_connections=4)
            os.remove(file_path)
        timestamp_file_full.write() # make sure we use proper timestamp info

    def do_transaction_backup(self):
        timestamp_file_tran = BackupTimestampBlob(
            storage_cfg=self.storage_cfg, 
            instance_metadata=self.instance_metadata, 
            is_full=False)

        timestamp_file_tran.write()

    def restore(self, restore_point):
        print "Perform restore for restore point \"{}\"".format(restore_point)


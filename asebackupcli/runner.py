import logging
import argparse
import pid

from .funcmodule import printe
from .backupagent import BackupAgent
from .backupconfiguration import BackupConfiguration
from .scheduleparser import ScheduleParser

class Runner:
    @staticmethod
    def configure_logging():
        logfile_name='backup.log'
        logging.basicConfig(
            filename=logfile_name,
            level=logging.DEBUG,
            format="%(asctime)-15s pid-%(process)d line-%(lineno)d %(levelname)s: \"%(message)s\""
            )
        logging.getLogger('azure.storage').setLevel(logging.FATAL)

    @staticmethod
    def arg_parser():
        parser = argparse.ArgumentParser()
        parser.add_argument("-c",  "--config", help="the path to the config file")
        parser.add_argument("-x",  "--show-configuration", help="Shows the VM's configuration values", action="store_true")
        parser.add_argument("-u",  "--unit-tests", help="Run unit tests", action="store_true")

        parser.add_argument("-f",  "--full-backup", help="Perform full backup", action="store_true")
        parser.add_argument("-t",  "--transaction-backup", help="Perform transactions backup", action="store_true")
        parser.add_argument("-r",  "--restore", help="Perform restore for date")
        parser.add_argument("-l",  "--list-backups", help="Lists all backups in Azure storage", action="store_true")
        parser.add_argument("-p",  "--prune-old-backups", help="Removes old backups from Azure storage ('--prune-old-backups 30d' removes files older 30 days)")

        parser.add_argument("-y",  "--force", help="Perform forceful backup (ignores age of last backup or business hours)", action="store_true")
        parser.add_argument("-s",  "--skip-upload", help="Skip uploads of backup files", action="store_true")
        parser.add_argument("-o",  "--output-dir", help="Specify target folder for backup files")
        parser.add_argument("-db", "--databases", help="Select databases to backup or restore ('--databases A,B,C')")
        return parser

    @staticmethod
    def get_output_dir(args, backup_configuration):
        if args.output_dir:
            return args.output_dir
        else:
            return backup_configuration.get_standard_local_directory()

    @staticmethod
    def main():
        Runner.configure_logging()
        parser = Runner.arg_parser() 
        args = parser.parse_args()

        backup_configuration = BackupConfiguration(args.config)
        output_dir = Runner.get_output_dir(args, backup_configuration)

        if args.full_backup:
            try:
                with pid.PidFile(pidname='backup-ase-full', piddir=".") as _p:
                    BackupAgent(backup_configuration).full_backup(
                        force=args.force, 
                        skip_upload=args.skip_upload,
                        output_dir=output_dir,
                        databases=args.databases)
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip full backup, already running")
                printe("Skipping full backup, there is a full-backup in flight currently")
        elif args.transaction_backup:
            try:
                with pid.PidFile(pidname='backup-ase-tran', piddir=".") as _p:
                    BackupAgent(backup_configuration).transaction_backup(
                        force=args.force,
                        skip_upload=args.skip_upload,
                        output_dir=output_dir,
                        databases=args.databases)
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip transaction log backup, already running")
        elif args.restore:
            BackupAgent(backup_configuration).restore(
                restore_point=args.restore, 
                output_dir=output_dir, 
                databases=args.databases)
        elif args.list_backups:
            if args.databases:
                databases = args.databases.split(",")
            else:
                databases = []

            BackupAgent(backup_configuration).list_backups(
                databases=databases)
        elif args.prune_old_backups:
            if args.databases:
                databases = args.databases.split(",")
            else:
                databases = None

            age = ScheduleParser.parse_timedelta(args.prune_old_backups)

            BackupAgent(backup_configuration).prune_old_backups(older_than=age, databases=databases)
        elif args.unit_tests:
            import doctest
            doctest.testmod()
            # doctest.testmod(verbose=True)
        elif args.show_configuration:
            BackupAgent(backup_configuration).show_configuration()
        else:
            parser.print_help()

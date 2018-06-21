import logging
import argparse
import pid
import sys
import os
import getpass
import socket
import os.path

from .funcmodule import printe
from .backupagent import BackupAgent
from .backupconfiguration import BackupConfiguration
from .databaseconnector import DatabaseConnector
from .scheduleparser import ScheduleParser
from .timing import Timing
from .__init__ import version

class Runner:
    @staticmethod
    def configure_logging():
        logging.basicConfig(
            filename="asebackupcli.log",
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
    def log_script_invocation():
        return ", ".join([
            "Script version v{}".format(version()),
            "Script arguments: {}".format(str(sys.argv)),
            "Current directory: {}".format(os.getcwd()),
            "User: {} (uid {}, gid {})".format(getpass.getuser(), os.getuid(), os.getgid),
            "Hostname: {}".format(socket.gethostname()),
            "uname: {}".format(str(os.uname())),
            "ProcessID: {}".format(os.getpid()),
            "Parent ProcessID: {}".format(os.getppid())
        ])

    @staticmethod
    def get_config_file(args, parser):
        if args.config:
            config_file = os.path.abspath(args.config)
            if not os.path.isfile(config_file):
                raise(Exception("Cannot find configuration {}".format(config_file)))

            return config_file
        else:
            raise(Exception(parser.print_help()))

    @staticmethod
    def get_output_dir(args):
        if args.output_dir:
            output_dir = os.path.abspath(args.output_dir)
            logging.debug("Output dir is user-supplied: {}".format(output_dir))
            return output_dir
        elif args.config:
            output_dir = os.path.abspath(BackupConfiguration(args.config).get_standard_local_directory())
            logging.debug("Output dir via config file {}".format(output_dir))
            return output_dir
        else:
            output_dir = os.path.abspath("/tmp")
            logging.debug("Output dir is fallback: {}".format(output_dir))
            return output_dir

    @staticmethod
    def get_databases(args):
        if args.databases:
            databases = args.databases.split(",")
            logging.debug("User manually selected databases: {}".format(str(databases)))
            return databases
        else:
            logging.debug("User did not select databases, trying to backup all databases")
            return []

    @staticmethod
    def main():
        Runner.configure_logging()
        parser = Runner.arg_parser() 
        args = parser.parse_args()

        logging.debug(Runner.log_script_invocation())
        config_file = Runner.get_config_file(args=args, parser=parser)
        backup_configuration = BackupConfiguration(config_file)
        backup_agent = BackupAgent(backup_configuration)
        output_dir = Runner.get_output_dir(args)
        databases = Runner.get_databases(args)
        DatabaseConnector(backup_configuration).log_env()

        for line in backup_agent.get_configuration_printable(output_dir=output_dir):
            logging.debug(line)

        if args.full_backup:
            try:
                with pid.PidFile(pidname='backup-ase-full', piddir=".") as _p:
                    backup_agent.full_backup(
                        force=args.force, skip_upload=args.skip_upload,
                        output_dir=output_dir, databases=databases)
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip full backup, already running")
        elif args.transaction_backup:
            try:
                with pid.PidFile(pidname='backup-ase-tran', piddir=".") as _p:
                    backup_agent.transaction_backup(
                        force=args.force, skip_upload=args.skip_upload,
                        output_dir=output_dir, databases=databases)
            except pid.PidFileAlreadyLockedError:
                logging.warn("Skip transaction log backup, already running")
        elif args.restore:
            backup_agent.restore(
                restore_point=args.restore, 
                output_dir=output_dir, 
                databases=databases)
        elif args.list_backups:
            backup_agent.list_backups(databases=databases)
        elif args.prune_old_backups:
            age = ScheduleParser.parse_timedelta(args.prune_old_backups)
            backup_agent.prune_old_backups(older_than=age, databases=databases)
        elif args.unit_tests:
            import doctest
            doctest.testmod()
            # doctest.testmod(verbose=True)
        elif args.show_configuration:
            print(backup_agent.show_configuration(output_dir=output_dir))
        else:
            parser.print_help()

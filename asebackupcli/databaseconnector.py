# coding=utf-8
# pylint: disable=c0301

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
from os.path import expanduser
import glob
import subprocess
import logging

from .naming import Naming
from .backupexception import BackupException

class DatabaseConnector(object):
    """The ASE DB logic."""
    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration

    def get_executable_path(self, path):
        return glob.glob(os.path.join(expanduser("~"), path))[0]

    def get_backup_user_name(self):
        return "sapsa"

    def get_database_password(self):
        try:
            gen = self.backup_configuration.get_database_password_generator()
            password = subprocess.check_output(gen, shell=True)
            return str(password).strip()
        except Exception as e:
            raise BackupException("Failed to retrieve the database password\n{}".format(e.message))

    def isql(self):
        width = "999"
        supress_header = "-b"
        return [
            self.get_executable_path('OCS-*/bin/isql'),
            "-S", self.backup_configuration.get_db_server_name(),
            "-U", self.get_backup_user_name(),
            "-P", self.get_database_password(),
            "-w", width,
            supress_header
        ]

    def ddlgen(self, dbname, args=[]):
        return [
            self.get_executable_path('ASE-*/bin/ddlgen'),
            "-S{}".format(self.backup_configuration.get_system_id()),
            "-D{}".format(dbname),
            "-U{}".format(self.get_backup_user_name()),
            "-P{}".format(self.get_database_password())
        ] + args

    def create_ddlgen(self, dbname):
        """Create a SQL sidecar file with the database schema."""
        # echo "$(ddlgen -Usapsa -S${SID} -D${DB} -P${SAPSA_PWD} -F% -TDBD -N%)" >  20180678.sql
        # echo "$(ddlgen -Usapsa -S${SID} -D${DB} -P${SAPSA_PWD} -F%)"           >> 20180678.sql
        stdout1, _stderr1, _returncode1 = DatabaseConnector.call_process(
            command_line=self.ddlgen(dbname=dbname, args=["-F%", "-TDBD", "-N%"]))
        stdout2, _stderr2, _returncode2 = DatabaseConnector.call_process(
            command_line=self.ddlgen(dbname=dbname, args=["-F%"]))

        return "\n".join([str(stdout1), "", str(stdout2)])

    @staticmethod
    def sql_statement_stripe_count(dbname, is_full):
        return "\n".join([
            "set nocount on",
            "go",
            "declare @dbname varchar(30),",
            "        @dumptype varchar(12),",
            "        @stripes int,",
            "        @data_free numeric (10,2),",
            "        @data_size numeric (10,2),",
            "        @log_free numeric (10,2),",
            "        @log_size numeric (10,2),",
            "        @max_stripe_size_in_GB int",
            "",
            "select @dbname = '{dbname}'".format(dbname=dbname),
            "select @stripes = 0",
            "select @max_stripe_size_in_GB = 10",
            "select",
            "    @data_free = convert(numeric(10,2),sum(curunreservedpgs(dbid, lstart, unreservedpgs)) * (@@maxpagesize / 1024. / 1024)),",
            "    @data_size = convert(numeric(10,2),sum(u.size * (@@maxpagesize / 1024. / 1024.))),",
            "    @log_free = convert(numeric(10,2),lct_admin('logsegment_freepages', u.dbid) * (@@maxpagesize / 1024. /1024. ))",
            "    from master..sysusages u, master..sysdevices d",
            "    where d.vdevno = u.vdevno",
            "        and d.status &2 = 2",
            "        and u.segmap <> 4",
            "        and u.segmap < 5",
            "        and db_name(u.dbid) = @dbname",
            "    group by u.dbid",
            "select @log_size =  sum(us.size * (@@maxpagesize / 1024. / 1024.))",
            "    from master..sysdatabases db, master..sysusages us",
            "    where db.dbid = us.dbid",
            "        and us.segmap = 4",
            "        and db_name(db.dbid) = @dbname",
            "    group by db.dbid",
            "select @data_free = isnull (@data_free, 0),",
            "       @data_size = isnull (@data_size ,0),",
            "       @log_free  = isnull (@log_free, 0),",
            "       @log_size  = isnull (@log_size, 0)"
        ]
        +
        {
            True: [
                "select @stripes = convert (int, ((@data_size - @data_free + @log_size - @log_free) / 1024 + @max_stripe_size_in_GB ) / @max_stripe_size_in_GB)",
                "if(( @stripes < 2 ) and ( @data_size - @data_free + @log_size - @log_free > 1024 ))"
                ],
            False: [
                "select @stripes = convert (int, ((@log_size - @log_free) / 1024 + @max_stripe_size_in_GB ) / @max_stripe_size_in_GB)",
                "if(( @stripes < 2 ) and ( @log_size - @log_free > 1024 ))"
            ]
        }[is_full]
        +
        [
            "begin",
            "    select @stripes = 2",
            "end",
            "",
            "if @stripes > 8",
            "begin",
            "    select @stripes = 8",
            "end",
            "",
            "select @stripes",
            "go",
            ""
        ])

    @staticmethod
    def sql_statement_list_databases(is_full):
        return "\n".join(
            [
                "set nocount on",
                "go",
                "select name, status, status2 into #dbname",
                "    from master..sysdatabases",
                "    where dbid <> 2 and status3 & 256 = 0"
            ]
            +
            {
                False:[
                    "delete from #dbname where status2 & 16 = 16 or status2 & 32 = 32 or status & 8 = 8",
                    "delete from #dbname where tran_dumpable_status(name) <> 0"
                ],
                True: []
            }[is_full]
            +
            [
                "declare @inputstrg varchar(1000)",
                "declare @delim_pos int",
                "select @inputstrg = ''",
                "if char_length(@inputstrg) > 1",
                "begin",
                "    create table #selected_dbs(sequence int identity, dbname varchar(50))",
                "    while char_length(@inputstrg) > 0",
                "    begin",
                "        select @delim_pos = charindex(',', @inputstrg)",
                "        if @delim_pos = 0",
                "        begin",
                "            select @delim_pos = char_length(@inputstrg) + 1",
                "        end",
                "        insert into #selected_dbs(dbname) select substring(@inputstrg, 1, @delim_pos - 1)",
                "        select @inputstrg = substring(@inputstrg, @delim_pos + 1, char_length(@inputstrg))",
                "    end",
                "    delete from #dbname where name not in (select dbname from #selected_dbs)",
                "end",
                "select name from #dbname order by 1",
                "go",
                ""
            ]
            )

    @staticmethod
    def sql_statement_create_backup(dbname, is_full, start_timestamp, stripe_count, output_dir):
        files = [Naming.local_filesystem_name(
            directory=output_dir, dbname=dbname,
            is_full=is_full, start_timestamp=start_timestamp,
            stripe_index=stripe_index, stripe_count=stripe_count
            ) for stripe_index in range(1, stripe_count + 1)]

        return DatabaseConnector.sql_statement_create_backup_for_filenames(
            dbname=dbname, is_full=is_full, files=files)

    @staticmethod
    def sql_statement_create_backup_for_filenames(dbname, is_full, files):
        return "\n".join(
            [
                "use master",
                "go"
            ]
            # sp_dboption AZU, 'trunc log on chkpt', 'false'\ngo\n
            +
            [
                "dump {type} {dbname} to {file_names}".format(
                    type={True:"database", False:"transaction"}[is_full],
                    dbname=dbname,
                    file_names="\n    stripe on ".join(
                        ["'{fn}'".format(fn=fn) for fn in files]
                    )
                ),
                "with compression = '101'",
                "",
                "if @@error = 0",
                "begin",
                "  print '{}'".format(DatabaseConnector.MAGIC_SUCCESS_STRING),
                "end",
                "",
                "go",
                ""
            ]
        )

    MAGIC_SUCCESS_STRING = "ASE_AZURE_BACKUP_SUCCESS"

    def determine_database_backup_stripe_count(self, dbname, is_full):
        (stdout, _stderr, _returncode) = self.call_isql(
            stdin=DatabaseConnector.sql_statement_stripe_count(dbname=dbname, is_full=is_full))
        try:
            return int(stdout)
        except Exception:
            raise BackupException("Cannot determine stripe_count: {}".format(stdout))

    def determine_databases(self, user_selected_databases, is_full):
        if user_selected_databases:
            return user_selected_databases
        else:
            return self.list_databases(is_full=is_full)

    def list_databases(self, is_full):
        (stdout, _stderr, _returncode) = self.call_isql(
            stdin=DatabaseConnector.sql_statement_list_databases(is_full=is_full))

        return filter(
            lambda e: e != "",
            map(lambda s: s.strip(), stdout.split("\n")))

    def create_backup(self, dbname, is_full, start_timestamp, stripe_count, output_dir):
        return self.call_isql(
            stdin=DatabaseConnector.sql_statement_create_backup(
                dbname=dbname, is_full=is_full,
                start_timestamp=start_timestamp,
                stripe_count=stripe_count,
                output_dir=output_dir))

    def create_backup_streaming(self, dbname, is_full, stripe_count, output_dir):
        return self.call_isql(
            stdin=DatabaseConnector.sql_statement_create_backup_for_filenames(
                dbname=dbname, is_full=is_full,
                files=Naming.pipe_names(
                    dbname=dbname, is_full=is_full,
                    stripe_count=stripe_count,
                    output_dir=output_dir)))

    ERR_DATABASE_SERVICE_NOT_AVAILABLE = "Database service not reachable"
    ERR_BACKUP_SERVICE_NOT_AVAILABLE = "Backup server not reachable"
    ERR_BACKUP_SERVICE_KILLED = "Backup service died"
    ERR_UNKNOWN_DATABASE = "Unknown database"
    ERR_FILESYSTEM_FULL = "Filesystem full"

    def call_isql(self, stdin):
        """Call `isql` and send `stdin`"""
        stdout, stderr, returncode = DatabaseConnector.call_process(command_line=self.isql(), stdin=stdin)

        if returncode == 255 and "ct_connect(): network packet layer:" in stdout:
            raise BackupException(DatabaseConnector.ERR_DATABASE_SERVICE_NOT_AVAILABLE)

        if "Attempt to locate entry in sysdatabases for database" in stdout:
            # "Attempt to locate entry in sysdatabases for database 'not_there' by name failed -
            # no entry found under that name. Make sure that name is entered properly."
            raise BackupException(DatabaseConnector.ERR_UNKNOWN_DATABASE)

        bs1_err = "Can't open a connection to site 'SYB_BACKUP'. " in stdout
        bs2_err = "Could not establish communication with Backup Server" in stdout
        bs3_err = "Please make sure that there is an entry in Sysservers for this server, and that the correct server is running." in stdout
        if bs1_err or bs2_err or bs3_err:
            raise BackupException(DatabaseConnector.ERR_BACKUP_SERVICE_NOT_AVAILABLE)

        if "error number 28 (No space left on device)" in stdout:
            raise BackupException(DatabaseConnector.ERR_FILESYSTEM_FULL)

        if "SYBMULTBUF ERROR: Emulator interprocess communication failed with error state " in stdout:
            raise BackupException(DatabaseConnector.ERR_BACKUP_SERVICE_KILLED)

        return (stdout, stderr, returncode)

    @staticmethod
    def call_process(command_line, stdin=None):
        """Execute a shell command"""
        logging.debug("Executing %s", command_line[0])

        p = subprocess.Popen(
            command_line,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # env=self.get_ase_environment()
        )
        stdout, stderr = p.communicate(stdin)
        returncode = p.returncode
        if returncode != 0:
            logging.debug("Error {} calling \"{}\"".format(returncode, command_line[0]))

        return (stdout, stderr, returncode)

    def log_env(self):
        """Log environment information"""
        ase_env = os.environ
        for key in ["INCLUDE", "LIB", "LD_LIBRARY_PATH", "PATH", "LANG", "COCKPIT_JAVA_HOME",
                    "SAP_JRE7", "SAP_JRE7_64", "SYBASE_JRE_RTDS", "SAP_JRE8", "SAP_JRE8_64",
                    "SYBASE", "SYBROOT", "SYBASE_OCS", "SYBASE_ASE", "SYBASE_WS"]:
            if ase_env.has_key(key):
                logging.debug("Environment %s=%s", key, ase_env[key])
            else:
                logging.debug("Environment %s=", key)

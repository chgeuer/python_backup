import subprocess
import os

from .naming import Naming

class DatabaseConnector:
    def get_ase_base_directory(self):
        return "/sybase/{}".format(self.backup_configuration.get_SID())

    def isql_path(self):
        return os.path.join(
            self.get_ase_base_directory(), 
            "OCS-{}/bin/isql".format(self.backup_configuration.get_ase_version()))

    def ddlgen_path(self):
        return os.path.join(
            self.get_ase_base_directory(), 
            "ASE-{}/bin/ddlgen".format(self.backup_configuration.get_ase_version()))

    def __init__(self, backup_configuration):
        self.backup_configuration = backup_configuration

    def get_database_password(self, sid):
        sid = self.backup_configuration.get_SID()
        executable = os.path.join(self.get_ase_base_directory(), "dba/bin/dbsp")
        arg = "***REMOVED***"
        stdout, _stderr = self.call_process(command_line=[executable, arg], stdin="")
        return str(stdout).strip()

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
            "go"
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
                "go"
            ])

    @staticmethod
    def sql_statement_create_backup(dbname, is_full, start_timestamp, stripe_count, output_dir):
        """
            >>> print(DatabaseConnector.sql_statement_create_backup(output_dir="/tmp", dbname="AZU", is_full=True, start_timestamp=Timing.parse("20180629_124500"), stripe_count=1))
            use master
            go
            sp_dboption AZU, 'trunc log on chkpt', 'false'
            go
            dump database AZU to '/tmp/AZU_full_20180629_124500_S001-001.cdmp'
            with compression = '101'
            go

            >>> print(DatabaseConnector.sql_statement_create_backup(output_dir="/tmp", dbname="AZU", is_full=True, start_timestamp=Timing.parse("20180629_124500"), stripe_count=4))
            use master
            go
            sp_dboption AZU, 'trunc log on chkpt', 'false'
            go
            dump database AZU to '/tmp/AZU_full_20180629_124500_S001-004.cdmp'
                stripe on '/tmp/AZU_full_20180629_124500_S002-004.cdmp'
                stripe on '/tmp/AZU_full_20180629_124500_S003-004.cdmp'
                stripe on '/tmp/AZU_full_20180629_124500_S004-004.cdmp'
            with compression = '101'
            go

            >>> print(DatabaseConnector.sql_statement_create_backup(output_dir="/tmp", dbname="AZU", is_full=False, start_timestamp=Timing.parse("20180629_124500"), stripe_count=1))
            use master
            go
            dump transaction AZU to '/tmp/AZU_tran_20180629_124500_S001-001.cdmp'
            with compression = '101'
            go

            >>> print(DatabaseConnector.sql_statement_create_backup(output_dir="/tmp", dbname="AZU", is_full=False, start_timestamp=Timing.parse("20180629_124500"), stripe_count=4))
            use master
            go
            dump transaction AZU to '/tmp/AZU_tran_20180629_124500_S001-004.cdmp'
                stripe on '/tmp/AZU_tran_20180629_124500_S002-004.cdmp'
                stripe on '/tmp/AZU_tran_20180629_124500_S003-004.cdmp'
                stripe on '/tmp/AZU_tran_20180629_124500_S004-004.cdmp'
            with compression = '101'
            go
        """

        files = map(lambda stripe_index: 
            Naming.local_filesystem_name(
                directory=output_dir, 
                dbname=dbname, 
                is_full=is_full, 
                start_timestamp=start_timestamp, 
                stripe_index=stripe_index, 
                stripe_count=stripe_count), range(1, stripe_count + 1))

        return "\n".join(
            [
                "use master",
                "go"
            ]
            +
            {
                False: [],
                True: [
                    "sp_dboption {dbname}, 'trunc log on chkpt', 'false'".format(dbname=dbname),
                    "go"
                ]
            }[is_full]
            +
            [
                "dump {type} {dbname} to {file_names}".format(
                    type={True:"database", False:"transaction"}[is_full],
                    dbname=dbname,
                    file_names="\n    stripe on ".join(
                        map(lambda fn: "'{fn}'".format(fn=fn), files)
                    )
                ),
                "with compression = '101'",
                "go"
            ]
        )

    def determine_database_backup_stripe_count(self, dbname, is_full):
        (stdout, _stderr) = self.call_process(
            command_line=self.isql(),
            stdin=DatabaseConnector.sql_statement_stripe_count(dbname=dbname, is_full=is_full))
        return int(stdout)

    def determine_databases(self, database_str, is_full):
        if database_str != None:
            return database_str.split(",")
        else:
            return self.list_databases(is_full=is_full)

    def list_databases(self, is_full):
        (stdout, _stderr) = self.call_process(
            command_line=self.isql(),
            stdin=DatabaseConnector.sql_statement_list_databases(is_full=is_full))

        return filter(
            lambda e: e != "", 
            map(lambda s: s.strip(), stdout.split("\n")))

    def create_backup(self, dbname, is_full, start_timestamp, stripe_count, output_dir):
        sql = DatabaseConnector.sql_statement_create_backup(
                dbname=dbname, is_full=is_full, 
                start_timestamp=start_timestamp, 
                stripe_count=stripe_count,
                output_dir=output_dir)

        return self.call_process(command_line=self.isql(), stdin=sql)

    def create_isql_commandline(self, server_name, username, password):
        supress_header = "-b"
        return [
            self.isql_path(),
            "-S", server_name,
            "-U", username,
            "-P", password,
            "-w", "999",
            supress_header
        ]

    def isql(self):
        return DatabaseConnector.create_isql_commandline(
            server_name=self.backup_configuration.get_CID(),
            username="sapsa",
            password=self.get_database_password(
                sid=self.backup_configuration.get_SID()))

    def create_ddlgen_commandline(self, dbname, username, password):
        # ddlgen -Usapsa -S${SID} -D${DB} -P${SAPSA_PWD} -F% -TDBD -N%
        # ddlgen -Usapsa -S${SID} -D${DB} -P${SAPSA_PWD} -F%
        return [
            self.ddlgen_path(),
            "-U{}".format(username),
            "-D{}".format(dbname),
            "-P{}".format(password),
            "-S{}".format(self.backup_configuration.get_SID())
        ]

    def ddlgen(self, dbname):
        return self.create_ddlgen_commandline(
            dbname=dbname,
            username="sapsa",
            password=self.get_database_password(sid=self.backup_configuration.get_SID()))

    def get_ase_environment(self):
        ase_env = os.environ.copy()

        p=lambda path: os.path.join(self.get_ase_base_directory(), path)
        val=lambda name: ase_env.get(name, "")
        
        jre7=p("shared/SAPJRE-7_1_049_64BIT")
        jre8=p("shared/SAPJRE-8_1_029_64BIT")

        ase_env["SAP_JRE7"]=jre7
        ase_env["SAP_JRE7_64"]=jre7
        ase_env["SYBASE_JRE_RTDS"]=jre7
        ase_env["SAP_JRE8"]=jre8
        ase_env["SAP_JRE8_64"]=jre8
        ase_env["COCKPIT_JAVA_HOME"]=jre8
        ase_env["SYBASE"]=p("")
        ase_env["SYBROOT"]=p("")
        ase_env["SYBASE_OCS"]="OCS-{}".format(self.backup_configuration.get_ase_version())
        ase_env["SYBASE_ASE"]="ASE-{}".format(self.backup_configuration.get_ase_version())
        ase_env["SYBASE_WS"]="WS-{}".format(self.backup_configuration.get_ase_version())

        ase_env["INCLUDE"] = os.pathsep.join([
            p("OCS-{}/include".format(self.backup_configuration.get_ase_version())),
            val("INCLUDE")
        ])

        ase_env["LIB"] = os.pathsep.join([
            p("OCS-{}/lib".format(self.backup_configuration.get_ase_version())),
            val("LIB")
        ])

        ase_env["LD_LIBRARY_PATH"] = os.pathsep.join([
            p("ASE-{}/lib".format(self.backup_configuration.get_ase_version())),
            p("OCS-{}/lib".format(self.backup_configuration.get_ase_version())),
            p("OCS-{}/lib3p".format(self.backup_configuration.get_ase_version())),
            p("OCS-{}/lib3p64".format(self.backup_configuration.get_ase_version())),
            p("DataAccess/ODBC/lib"),
            p("DataAccess64/ODBC/lib"),
            val("LD_LIBRARY_PATH")
        ])

        ase_env["PATH"] = os.pathsep.join([
            p("ASE-{}/bin".format(self.backup_configuration.get_ase_version())),
            p("ASE-{}/install".format(self.backup_configuration.get_ase_version())),
            p("ASE-{}/jobscheduler/bin".format(self.backup_configuration.get_ase_version())),
            p("OCS-{}/bin".format(self.backup_configuration.get_ase_version())),
            p("COCKPIT-4/bin"),
            val("PATH")
         ])

        if ase_env.has_key("LANG"):
            del(ase_env["LANG"])

        return ase_env

    def call_process(self, command_line, stdin):
        p = subprocess.Popen(
            command_line,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self.get_ase_environment()
        )
        stdout, stderr = p.communicate(stdin)
        return (stdout, stderr)

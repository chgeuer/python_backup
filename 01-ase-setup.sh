#!/bin/bash

# Customer ID
export CID="AZU"
# System ID
export SID="JLD"

export PASSWD_SA="Sybase123"
export PASSWD_SAPSA="Test123.-"

source /sybase/${SID}/SYBASE.sh
unset LANG

#
# Setup ASE instance
#
cat > "${CID}.RS" <<-EOF
sqlsrv.server_name:                             ${CID}
sqlsrv.default_backup_server:                   ${CID}_BS
sqlsrv.sa_login:                                sa
sqlsrv.sa_password:                             ${PASSWD_SA}
sybinit.boot_directory:                         /sybase/${SID}/
sybinit.release_directory:                      /sybase/${SID}/
sqlsrv.master_device_physical_name:             /sybase/${SID}/data/master.dat
sqlsrv.sybpcidb_device_physical_name:           /sybase/${SID}/data/sybpcidbdev_data.dat 
sqlsrv.sybsystemprocs_device_physical_name:     /sybase/${SID}/data/sysprocs.dat
sqlsrv.sybsystemdb_db_device_physical_name:     /sybase/${SID}/data/sybsysdb.dat
sqlsrv.tempdb_device_physical_name:             /sybase/${SID}/saptemp_1/tempdb.dat
sqlsrv.errorlog:                                /sybase/${SID}/ASE-16_0/install/sap_ase.log

sqlsrv.addl_cmdline_parameters:
sqlsrv.application_type:                        MIXED
sqlsrv.atr_name_qinstall:                       no
sqlsrv.atr_name_shutdown_required:              yes
sqlsrv.avail_cpu_num:                           1
sqlsrv.avail_physical_memory:                   2048
sqlsrv.characterset_install_list:
sqlsrv.characterset_remove_list:
sqlsrv.configure_remote_command_and_control_agent_ase: no
sqlsrv.connect_retry_count:                     5
sqlsrv.connect_retry_delay_time:                5
sqlsrv.default_characterset:                    utf8
sqlsrv.default_language:                        us_english
sqlsrv.disk_mirror_name:
sqlsrv.do_add_server:                           yes
sqlsrv.do_configure_pci:                        no
sqlsrv.do_create_sybsystemdb_db_device:         no
sqlsrv.do_create_sybsystemdb:                   no
sqlsrv.do_create_sybsystemprocs_device:         yes
sqlsrv.do_optimize_config:                      no
sqlsrv.do_upgrade:                              no
sqlsrv.enable_ase_for_ase_cockpit_monitor:      no
sqlsrv.eventlog:                                yes
sqlsrv.language_install_list:
sqlsrv.language_remove_list:
sqlsrv.master_db_size:                          128
sqlsrv.master_device_size:                      256
sqlsrv.network_hostname_list:                   localhost
sqlsrv.network_name_alias_list:
sqlsrv.network_port_list:                       4901
sqlsrv.network_protocol_list:                   tcp
sqlsrv.new_config:                              yes
sqlsrv.notes:
sqlsrv.preupgrade_succeeded:                    no
sqlsrv.resword_conflict:                        0
sqlsrv.resword_done:                            no
sqlsrv.server_page_size:                        2k
sqlsrv.shared_memory_directory:
sqlsrv.sort_order:                              binaryalt
sqlsrv.sybpcidb_database_size:                  96
sqlsrv.sybpcidb_device_size:                    96
sqlsrv.sybsystemdb_db_device_logical_name:      systemdbdev
sqlsrv.sybsystemdb_db_device_physical_size:     128
sqlsrv.sybsystemdb_db_size:                     128
sqlsrv.sybsystemprocs_db_size:                  256
sqlsrv.sybsystemprocs_device_logical_name:      sysprocsdev
sqlsrv.sybsystemprocs_device_size:              256
sqlsrv.technical_user_password:
sqlsrv.technical_user:
sqlsrv.tempdb_database_size:                    256
sqlsrv.tempdb_device_size:                      256
sybinit.charset:                                utf8
sybinit.language:                               us_english
sybinit.log_file:
sybinit.product:                                sqlsrv
sybinit.resource_file:
EOF

/sybase/${SID}/ASE-16_0/bin/srvbuildres -r "${CID}.RS"

#
# Setup ASE Backup instance
#

cat > "${CID}_BS.RS" <<-EOF
sqlsrv.server_name:                             ${CID}_BS
bsrv.server_name:                               ${CID}_BS
sqlsrv.sa_login:                                sa
sqlsrv.sa_password:                             ${PASSWD_SA}
sybinit.boot_directory:                         /sybase/${SID}
sybinit.release_directory:                      /sybase/${SID}
bsrv.errorlog:                                  /sybase/${SID}/ASE-16_0/install/${CID}_BS.log
sybinit.product:                                bsrv
bsrv.do_add_backup_server:                      yes
bsrv.network_port_list:                         4902
bsrv.network_hostname_list:                     localhost
bsrv.network_protocol_list:                     tcp
bsrv.character_set:                             utf-8
bsrv.language:                                  us_english
bsrv.connect_retry_delay_time:                  5
bsrv.connect_retry_count:                       5
bsrv.new_config:                                yes
bsrv.do_upgrade:                                no
bsrv.addl_cmdline_parameters:
bsrv.allow_hosts_list:
bsrv.network_name_alias_list:
bsrv.notes:
EOF

/sybase/${SID}/ASE-16_0/bin/srvbuildres -r "${CID}_BS.RS"

unset LANG

mkdir /sybase/${SID}/sapdata_1

/sybase/${SID}/OCS-16_0/bin/isql -U sa -S "${CID}" -w 999 -P "${PASSWD_SA}" <<-EOF
use master
go

disk init name = "${CID}_data_01", size = "256M", physname = "/sybase/${SID}/sapdata_1/${CID}_data_01.dat"
go

disk init name = "${CID}_log_01", size = "128M", physname = "/sybase/${SID}/sapdata_1/${CID}_log_01.dat"
go

create database ${CID} on ${CID}_data_01 = 256 log on ${CID}_log_01 = 128
go

create login sapsa with password "${PASSWD_SAPSA}"
go

grant role sa_role          to sapsa
grant role replication_role to sapsa
grant role js_admin_role    to sapsa
grant role sybase_ts_role   to sapsa
grant role mon_role         to sapsa 
go
EOF

#
# list databases
#
/sybase/${SID}/OCS-16_0/bin/isql -S "${CID}" -U sapsa -P "${PASSWD_SAPSA}" -w 999 -b <<-EOF
sp_helpdb
go
EOF

#
# dump database and tx log
#
/sybase/${SID}/OCS-16_0/bin/isql -S "${CID}" -U sapsa -P "${PASSWD_SAPSA}" -w 999 <<-EOF
dump database ${CID} to './test1db_full_20180606_120000-S01_01.cdmp' with compression = '101'
go

dump transaction ${CID} to './test1db_tran_20180606_120000-S01_01.cdmp' with compression = '101'
go
EOF

########################

source /sybase/${SID}/SYBASE.sh
unset LANG

# /sybase/${SID}/ASE-16_0/install/RUN_${CID}
# /sybase/${SID}/ASE-16_0/install/RUN_${CID}_BS

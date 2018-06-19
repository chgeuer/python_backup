
# ASE Backup

## Install the script's dependencies in production environment

```
./install_runtime_dependencies.sh
```

## Install the mocks fora demo environment

```
./install_developer_mocks.sh
```

## Command line samples

### Run backup for a single database named 'AZU'

```bash
sudo ./backup.py \
    --config config.txt \
    --full-backup \
    --force \
    --output-dir /mnt/resource \
    --databases AZU
```

### Force full backup

```bash
chgeuer@saphec2:~/python_backup> sudo ./backup.py -c config.txt --full-backup --force

    Upload /tmp/AZU_full_20180614_235510_S001-001.cdmp to AZU_full_20180614_235510--20180614_235514_S001-001.cdmp
    Upload /tmp/master_full_20180614_235514_S001-001.cdmp to master_full_20180614_235514--20180614_235516_S001-001.cdmp
    Upload /tmp/model_full_20180614_235516_S001-001.cdmp to model_full_20180614_235516--20180614_235517_S001-001.cdmp
    Upload /tmp/sybsystemdb_full_20180614_235517_S001-001.cdmp to sybsystemdb_full_20180614_235517--20180614_235519_S001-001.cdmp
    Upload /tmp/sybsystemprocs_full_20180614_235519_S001-001.cdmp to sybsystemprocs_full_20180614_235519--20180614_235525_S001-001.cdmp

chgeuer@saphec2:~/python_backup> sudo ./backup.py -c config.txt --full-backup

    Skipping backup of database AZU
    Skipping backup of database master
    Skipping backup of database model
    Skipping backup of database sybsystemdb
    Skipping backup of database sybsystemprocs

chgeuer@saphec2:~/python_backup>
```

### Download restore files for a certain database

```bash
chgeuer@saphec2:~/python_backup> mkdir ./1

chgeuer@saphec2:~/python_backup> sudo ./backup.py -c config.txt -o ./1 -r 20180614_190056 -db AZU

Downloaded AZU_full_20180614_175508--20180614_175512_S001-001.cdmp to 1/AZU_full_20180614_175508_S001-001.cdmp
Downloaded AZU_tran_20180614_175516--20180614_175516_S001-001.cdmp to 1/AZU_tran_20180614_175516_S001-001.cdmp
Downloaded AZU_tran_20180614_175521--20180614_175521_S001-001.cdmp to 1/AZU_tran_20180614_175521_S001-001.cdmp
Downloaded AZU_tran_20180614_175523--20180614_175523_S001-001.cdmp to 1/AZU_tran_20180614_175523_S001-001.cdmp
Downloaded AZU_tran_20180614_175527--20180614_175527_S001-001.cdmp to 1/AZU_tran_20180614_175527_S001-001.cdmp
Downloaded AZU_tran_20180614_175529--20180614_175529_S001-001.cdmp to 1/AZU_tran_20180614_175529_S001-001.cdmp
Downloaded AZU_tran_20180614_190055--20180614_190055_S001-001.cdmp to 1/AZU_tran_20180614_190055_S001-001.cdmp
Downloaded AZU_tran_20180614_190100--20180614_190100_S001-001.cdmp to 1/AZU_tran_20180614_190100_S001-001.cdmp
```

### List backup files in storage

#### List all backup files in storage

```bash
./backup.py -c config.txt --list-backups | sort

Database "AZU" ended 20180614_233134 - full  [1]
Database "AZU" ended 20180614_233327 - tran  [1]
Database "AZU" ended 20180614_233329 - tran  [1]
Database "AZU" ended 20180614_233330 - tran  [1]
Database "AZU" ended 20180614_233346 - tran  [1]
Database "master" ended 20180612_004856 - full  [1]
Database "master" ended 20180612_005929 - full  [1, 2, 3]
Database "master" ended 20180612_203904 - full  [1]
Database "master" ended 20180614_233137 - full  [1]
Database "model" ended 20180612_004857 - full  [1]
Database "model" ended 20180612_005931 - full  [1, 2, 3]
Database "model" ended 20180612_203905 - full  [1]
Database "model" ended 20180614_233138 - full  [1]
Database "sybsystemdb" ended 20180612_004859 - full  [1]
Database "sybsystemdb" ended 20180612_005933 - full  [1, 2, 3]
Database "sybsystemdb" ended 20180612_203907 - full  [1]
Database "sybsystemdb" ended 20180614_233140 - full  [1]
Database "sybsystemprocs" ended 20180612_004905 - full  [1]
Database "sybsystemprocs" ended 20180612_005938 - full  [1, 2, 3]
Database "sybsystemprocs" ended 20180612_203913 - full  [1]
Database "sybsystemprocs" ended 20180614_233145 - full  [1]
```

#### List all backup files in storage for a certain DB

```bash
./backup.py -c config.txt --list-backups --databases AZU | sort

Database "AZU" ended 20180612_005926 - full  [1, 2, 3]
Database "AZU" ended 20180612_073326 - tran  [1]
Database "AZU" ended 20180612_162812 - tran  [1]
Database "AZU" ended 20180612_163919 - tran  [1]
Database "AZU" ended 20180612_165924 - tran  [1]
Database "AZU" ended 20180612_171247 - tran  [1]
Database "AZU" ended 20180612_171304 - full  [1, 2, 3]
Database "AZU" ended 20180612_172204 - tran  [1]
Database "AZU" ended 20180612_180321 - tran  [1]
Database "AZU" ended 20180612_180337 - tran  [1]
Database "AZU" ended 20180612_181831 - tran  [1]
```

## Inspect the machine's configuration

![tags in the portal][tags]

### Live view on raw instance metadata

```bash
chgeuer@saphec2:~/python_backup> curl -sH Metadata:true "http://169.254.169.254/metadata/instance?api-version=2017-12-01" | jq

{
  "compute": {
    "subscriptionId": "deadbeef-bee4-484b-bf13-d6a5505d2b51",
    "resourceGroupName": "backuptest",
    "name": "saphec2",
    "location": "westeurope",
    "tags": "db_backup_window_1:111111 111000 000000 011111;
             db_backup_window_2:111111 111000 000000 011111;
             db_backup_window_3:111111 111000 000000 011111;
             db_backup_window_4:111111 111000 000000 011111;
             db_backup_window_5:111111 111000 000000 011111;
             db_backup_window_6:111111 111111 111111 111111;
             db_backup_window_7:111111 111111 111111 111111;
             db_backup_interval_max:3d;
             db_backup_interval_min:1d;
             log_backup_interval_min:15s",
    "offer": "openSUSE-Leap",
    "sku": "42.3",
    ...
  },
  "network": {
     ...

```

### Config file contents

```
sap.SID:                       JLD
sap.CID:                       ABC
azure.storage.account_name:    erlang
azure.storage.account_key:     UqhiqGVB...
azure.storage.container_name:  foo
```

### Show consolidated config from config file and instance metadata

```bash
chgeuer@saphec2:~/python_backup> ./backup.py -c config.txt -x

    azure.vm_name:                      saphec2
    azure.resource_group_name:          backuptest
    azure.subscription_id:              deadbeef-bee4-484b-bf13-d6a5505d2b51
    sap.SID:                            JLD
    sap.CID:                            AZU
    skipped databases:                  ['dbccdb']
    db_backup_interval_min:             1 day, 0:00:00
    db_backup_interval_max:             3 days, 0:00:00
    log_backup_interval_min:            0:00:15
    business_hours_Mon:                 111111111000000000011111
    business_hours_Tue:                 111111111000000000011111
    business_hours_Wed:                 111111111000000000011111
    business_hours_Thu:                 111111111000000000011111
    business_hours_Fri:                 111111111000000000011111
    business_hours_Sat:                 111111111111111111111111
    business_hours_Sun:                 111111111111111111111111
    azure_storage_container_name:       foo
    azure_storage_account_name:         erlang
    azure_storage_account_key:          UqhiqGVBWN...

chgeuer@saphec2:~/python_backup>
```

In that output, the displayed values come from different locations: 

- The Azure VM instance metadata endpoint results in these values:
  - `azure.vm_name`, `azure.resource_group_name` and `azure.subscription_id`
  - The VM tags (via instance metadata) provide the following information: 
    - `db_backup_interval_min`, `db_backup_interval_max`, `log_backup_interval_min` and the business hours



`virtualenv --python=python2.7 ~/py`


[tags]: docs/tags.png "tags in the portal"


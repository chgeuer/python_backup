
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

### List backup files in storage

#### List all backup files in storage

```bash
./backup.py -c config.txt --list-backups | sort
```

#### List all backup files in storage for a certain DB

```bash
./backup.py -c config.txt --list-backups --databases AZU | sort
```

## Inspect the machine's configuration

```bash
chgeuer@db:~/python_backup> sudo ./backup.py --config config.txt --show-configuration
azure.vm_name:                      db
azure.resource_group_name:          backuptest
azure.subscription_id:              deadbeef-bee4-484b-bf13-d6a5505d2b51
sap.SID:                            JLD
sap.CID:                            AZU
skipped databases:                  ['dbccdb']
db_backup_interval_min:             1 day, 0:00:00
db_backup_interval_max:             3 days, 0:00:00
log_backup_interval_min:            0:00:15
log_backup_interval_max:            0:30:00
azure_storage_container_name:       foo
azure_storage_account_name:         erlang
azure_storage_account_key:          UqhiqGVBWN...
```


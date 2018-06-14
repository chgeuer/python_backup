
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
sudo ./backup.py --config config.txt --full-backup --force --databases AZU
```


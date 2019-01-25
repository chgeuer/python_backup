#!/bin/bash

command="asebackupcli -c ~/config -f -S -y -db master"

v=$(python setup.py --version)

python setup.py sdist && \
    scp dist/asebackupcli-${v}.tar.gz chgeuer@ase1:~ && \
    ssh chgeuer@ase1 "\
        source ~/backup/bin/activate && \
        pip install asebackupcli-${v}.tar.gz && \
        source /sybase/AZ3/SYBASE.sh && unset LANG && ${command}"

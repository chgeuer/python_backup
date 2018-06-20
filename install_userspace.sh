#!/bin/bash

envname=backup

pip install --user virtualenv
~/.local/bin/virtualenv --python=python2.7 ~/${envname}
source ~/${envname}/bin/activate
pip install git+https://github.com/chgeuer/python_backup.git#egg=asebackupcli

ln -s ~/${envname}/bin/asebackupcli ~/bin

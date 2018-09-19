#!/bin/bash

envname=backup

#pip install --user virtualenv

~/.local/bin/virtualenv --python=python2.7 ~/${envname}
source ~/${envname}/bin/activate
pip install -e .
python setup.py test
deactivate


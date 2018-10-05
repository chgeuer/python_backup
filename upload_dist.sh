#!/bin/bash

dest="asebackupcli-$(python setup.py --version).tar.gz"

python setup.py sdist

az storage blob upload \
	--account-name "$(cat .azurestorageaccountname)" \
	--account-key "$(cat .azurestorageaccountkey)" \
	--container-name "software" \
	--name "${dest}" --file "dist/${dest}"

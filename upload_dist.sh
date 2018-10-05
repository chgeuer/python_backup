#!/bin/bash

dest="asebackupcli-0.3.4.tar.gz"

az storage blob upload \
	--account-name "$(cat .\azurestorageaccountname)" \
	--account-key "$(cat .\.azurestorageaccountkey)" \
	--container-name "software" \
	--name "${dest}" --file "${dest}"


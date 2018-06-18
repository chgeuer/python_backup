#!/bin/bash

# Customer ID
export CID="AZU"
# System ID
export SID="JLD"

path="/sybase/${SID}/dba/bin"
sudo mkdir --parents "${path}"
sudo cp ./dbsp "${path}"
sudo chmod 0755 "${path}/dbsp"

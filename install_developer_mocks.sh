#!/bin/bash

sid="AZU"
path="/sybase/${sid}/dba/bin"
sudo mkdir --parents "${path}"
sudo cp ./dbsp "${path}"
sudo chmod 0755 "${path}/dbsp"

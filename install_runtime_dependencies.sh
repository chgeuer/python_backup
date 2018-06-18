#!/bin/bash

sudo python2.7 -m ensurepip --default-pip
sudo python2.7 -m pip install --upgrade pip
sudo python2.7 -m pip install -r requirements.txt

# sudo python2.7 -m pip install pid azure-storage-blob

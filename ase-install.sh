#!/bin/bash

# Source: https://www.sap.com/cmp/syb/crm-xu15-int-asexprdm/index.html
wget http://d1cuw2q49dpd0p.cloudfront.net/ASE16/Linux16SP03/ASE_Suite.linuxamd64.tgz
mkdir ase_setup_src && cd ase_setup_src && tar xvfz ../ASE_Suite.linuxamd64.tgz

echo "Make the folloging choices during installation:" 
echo "Default Install Folder:             /opt/sap"
echo "Choose Install Set:                 1- Typical"
echo "Software License Type Selection:    2- Install Express Edition of SAP Adaptive Server Enterprise"

sudo ./setup.bin
cd ..


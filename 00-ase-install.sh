#!/bin/bash

export SID="JLD"

# Source: https://www.sap.com/cmp/syb/crm-xu15-int-asexprdm/index.html
wget http://d1cuw2q49dpd0p.cloudfront.net/ASE16/Linux16SP03/ASE_Suite.linuxamd64.tgz
mkdir ase_setup_src && cd ase_setup_src && tar xvfz ../ASE_Suite.linuxamd64.tgz

echo "$(tput setaf 1)Make the folloging choices during installation:$(tput sgr 0)" 
echo "$(tput setaf 1)Default Install Folder:             $(tput setab 7)/sybase/${SID} $(tput sgr 0)"
echo "$(tput setaf 1)Choose Install Set:                 $(tput setab 7)1 - Typical$(tput sgr 0)"
echo "$(tput setaf 1)Software License Type Selection:    $(tput setab 7)2 - Install Express Edition of SAP Adaptive Server Enterprise$(tput sgr 0)"

sudo ./setup.bin
cd ..

# For color output:
# https://stackoverflow.com/questions/5947742/how-to-change-the-output-color-of-echo-in-linux
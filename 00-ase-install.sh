#!/bin/bash

export SID="JLD"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Source: https://www.sap.com/cmp/syb/crm-xu15-int-asexprdm/index.html
wget http://d1cuw2q49dpd0p.cloudfront.net/ASE16/Linux16SP03/ASE_Suite.linuxamd64.tgz
mkdir ase_setup_src && cd ase_setup_src && tar xvfz ../ASE_Suite.linuxamd64.tgz

echo "${RED}Make the folloging choices during installation:${NC}" 
echo "${RED}Default Install Folder:             ${GREEN}/sybase/${SID} ${NC}"
echo "${RED}Choose Install Set:                 ${GREEN}1 - Typical${NC}"
echo "${RED}Software License Type Selection:    ${GREEN}2 - Install Express Edition of SAP Adaptive Server Enterprise${NC}"

sudo ./setup.bin
cd ..

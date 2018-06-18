#!/bin/bash

for file in $(ls *)
do 
  vi +':w ++ff=unix' +':q' ${file}
done

for file in $(ls *.sh)
do 
  chmod +x  ${file}
done

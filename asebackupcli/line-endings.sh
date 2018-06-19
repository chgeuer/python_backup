#!/bin/bash

for file in $(ls *.py)
do 
  vi +':w ++ff=unix' +':q' ${file}
done

for file in $(ls *.py)
do 
  chmod +x  ${file}
done

#!/bin/bash
IFS=$(echo -en "\n\b")
for i in ./CCSO*.pdf; do
    python ./extract.py "$i" 1 168 571 533 960
done

#!/bin/sh

TRANCO_ID=$(curl -L https://tranco-list.eu/top-1m-id)
curl -L -O https://tranco-list.eu/top-1m.csv.zip || exit 1
unzip top-1m.csv.zip && rm top-1m.csv.zip || exit 1
chmod 664 top-1m.csv || exit 1
sed -i 's/\r//g' top-1m.csv

cat >source.txt <<EOF
DATA URL: https://tranco-list.eu/top-1m.csv.zip
ID URL:https://tranco-list.eu/top-1m-id 
ID VALUE: $TRANCO_ID
DATE: $(date)
EOF

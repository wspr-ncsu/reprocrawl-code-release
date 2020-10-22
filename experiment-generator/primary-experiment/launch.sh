#!/bin/sh

read -p "are the DBs scrubbed and tubbed and adequately fed? (type 'yes' to continue)" YES
if [ "$YES" != "yes" ]; then
	echo "no jobs for you!"
	exit 1
fi

cd $(dirname $0)
echo "Running in $(pwd)..."
time ../mkjobs.py ../../tranco/top-1m.csv 25000 matrix.json
time ../qjobs.py

#! /bin/bash

REL_DIR=`dirname $0`
DB_DIR=`cd ${REL_DIR}  && pwd`/debugdb

mserver5 --daemon=yes --dbpath=${DB_DIR} -d10 --forcemito > output.txt 2>&1 &

SERVER_PID=$!

echo ${SERVER_PID} > server.pid

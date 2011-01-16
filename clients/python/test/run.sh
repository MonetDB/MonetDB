#!/bin/bash

export TSTDB="demo"
export TSTHOSTNAME="localhost"
export TSTUSERNAME="monetdb"
export TSTPASSWORD="monetdb"
export TSTDEBUG="no"

python2 ./runtests.py
python3 ./runtests.py


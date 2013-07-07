#!/bin/bash

export TSTDB="demo"
export TSTHOSTNAME="localhost"
export TSTUSERNAME="monetdb"
export TSTPASSWORD="monetdb"
export TSTDEBUG="no"

nosetests3 ./runtests.py
nosetests3 ./test_control.py

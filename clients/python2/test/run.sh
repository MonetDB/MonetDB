#!/bin/bash

export TSTDB="demo"
export TSTHOSTNAME="localhost"
export TSTUSERNAME="monetdb"
export TSTPASSWORD="monetdb"
export TSTDEBUG="no"

nosetests ./runtests.py
nosetests ./test_control.py
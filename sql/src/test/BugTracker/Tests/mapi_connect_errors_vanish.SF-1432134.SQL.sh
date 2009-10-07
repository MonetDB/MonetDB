#!/bin/sh

echo "user=invalid" > .monetdb
echo "password=invalid" >> .monetdb
Mlog -x "$SQL_CLIENT"
rm -f .monetdb

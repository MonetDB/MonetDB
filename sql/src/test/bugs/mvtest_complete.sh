#!/bin/sh

# rename all entries and update CVS
echo "$1.sql"
mv $1.sql $2.sql
cvs rm $1.sql
cvs add $2.sql

cd Tests
sh mvtest.sh $1 $2

#!/bin/sh

if [ "$1" = "" ];
then
	echo "usage: $0 old new"
	exit 1
elif [ "$2" = "" ];
then
	echo "usage: $0 $1 new"
	exit 1
fi

# rename all entries and update CVS
for key in `ls $1.* | sed "s/$1\.//"`;
do
	echo "$1.$key"
	mv $1.$key $2.$key
	cvs rm $1.$key
	cvs add $2.$key
done
# patch the stdout and stderr
if [ -f $2.stable.out ];
then
	sed "s/$1/$2/" $2.stable.out > $2.stable.out.tmp
	mv $2.stable.out.tmp $2.stable.out
fi;
if [ -f $2.stable.err ];
then
	sed "s/$1/$2/" $2.stable.err > $2.stable.err.tmp
	mv $2.stable.err.tmp $2.stable.err
fi;
# update the source redirect if it exists
if [ -f $2.sql.src ];
then
	sed "s/$1/$2/" $2.sql.src > $2.sql.src.tmp
	mv $2.sql.src.tmp $2.sql.src
fi
# if this test was in the All file, update it
if [ -f All ];
then
	sed "s/$1/$2/" All > All.tmp
	mv All.tmp All
fi

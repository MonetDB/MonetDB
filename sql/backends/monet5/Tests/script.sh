#!/bin/bash
users=$1
delay=$2
runs=$3
query=$4

function query()
{
	i=0;
	while [ $i -lt $2 ]
	do
		sleep $1
		echo "fire $$.$i:" `date +"%F %T"`
		mclient -umonetdb -Pmonetdb -lsql -p50000 <$3
		echo "done $$.$i";
		i=`expr $i + 1`
	done
}

if [ -z "$delay" -o -z "$users" -o -z "$query" -o -z "runs" ]
then
	echo "usage: script <users> <delay> <runs> <queryfile>"
	exit
fi

while [ $users -gt 0 ]
do
	echo "start client"
	query  $delay $runs $query &
	users=`expr $users - 1 `
done

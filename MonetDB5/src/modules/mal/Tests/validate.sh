#!/bin/sh

n=100
echo "n:=$n"
./set_n.sh $n >/dev/null
for x in 0 1 2 3 4 5 6 7 8 9 11 12 13 15 16 17 23 24 25 31 32 33 47 48 49 63 64 65 95 96 97 127 128 129 191 192 193 255 256 257 382 384 385 511 512 513 1023 1024 1025 ; do
	echo "tuples:=$x"
	./set_tuples.sh $x >/dev/null
	for i in {Zero,One,Three}Ordered ; do
		echo $i:
		mserver5 --set monet_prompt='' <fastcrack_$i.mal |egrep -v '^[# ]|, true\]$|^\[ "" \]$|^\[ ".*", [0-9]+\]$'
	done
	echo
done
n=10
echo "n:=$n"
./set_n.sh $n >/dev/null
for x in $[(512*1024)-1] $[(512*1024)] $[(512*1024)+1] $[(768*1024)-1] $[(768*1024)] $[(768*1024)+1] $[(1024*1024)-1] $[(1024*1024)] $[(1024*1024)+1] ; do
	echo "tuples:=$x"
	./set_tuples.sh $x >/dev/null
	for i in {Zero,One,Three}Ordered ; do
		echo $i:
		mserver5 --set monet_prompt='' <fastcrack_$i.mal |egrep -v '^[# ]|^\[ .*\]$'
	done
	echo
done
n=30
echo "n:=$n"
./set_n.sh $n >/dev/null
x=$[1024*1024]
echo "tuples:=$x"
./set_tuples.sh $x >/dev/null

#!/bin/sh

for i in {Zero,One,Three} ; do
	hdr="$hdr	$i"
done
echo "#$hdr"

n=20
echo "#n:=$n"
./set_n.sh $n >/dev/null
for x in 0 1 2 3 4 5 6 7 8 9 15 11 12 13 16 17 23 24 25 31 32 33 47 48 49 63 64 65 95 96 97 127 128 129 191 192 193 255 256 257 382 384 385 511 512 513 1023 1024 1025 ; do
	./set_tuples.sh $x >/dev/null
	row="$x"
	for i in {Zero,One,Three} ; do
		answ=`mserver5 --set monet_prompt='' <fastcrack_$i.mal |tail -1|sed -e "s/.*, //" -e "s/]//"`
		row="$row	$answ"
	done
	echo $row
done
./set_n.sh $n >/dev/null
for x in $[(512*1024)-1] $[(512*1024)] $[(512*1024)+1] $[(768*1024)-1] $[(768*1024)] $[(768*1024)+1] $[(1024*1024)-1] $[(1024*1024)] $[(1024*1024)+1] ; do
	./set_tuples.sh $x >/dev/null
	row="$x"
	for i in {Zero,One,Three} ; do
		answ=`mserver5 --set monet_prompt='' <fastcrack_$i.mal |tail -1|sed -e "s/.*, //" -e "s/]//"`
		row="$row	$answ"
	done
	echo $row
done
./set_n.sh $n >/dev/null
x=$[1024*1024]
./set_tuples.sh $x >/dev/null

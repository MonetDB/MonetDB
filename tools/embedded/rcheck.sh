#!/bin/bash

WD=/scratch/hannes/rcheck
MB=Jun2016

mkdir -p $WD

RS=$WD/r-source
RI=$WD/r-install
RB=$RI/bin/R
MS=$WD/monetdb-source
LS=$WD/monetdblite-stage
RR=$WD/r-svnrev
RT=$WD/rtmp

RUNID=`date +%s`
LG=$WD/logs/$RUNID
mkdir -p $LG
rm $WD/logs/current
ln -s $LG $WD/logs/current

if [ ! -d $RS ]; then
	svn co https://svn.r-project.org/R/trunk/ $RS
fi
(
	cd $RS
	(svn revert -R . && svn update --force) > /dev/null
	OR=`cat $RR`
	CR=`svn info | grep Revision | cut -d " " -f2`
	if [ "$OR" != "$CR" ] || [ ! -f $RB ] ; then
		./configure --prefix=$RI --with-x=no --without-recommended-packages > $LG/r-configure 2>&1
		make clean > /dev/null
		make > $LG/r-make 2>&1
		make install > $LG/r-install 2>&1
	fi
	echo $CR > $RR
)

if [ ! -f $RB ] ; then
	echo "Could not build R. FML."
	exit -1
fi

export PATH=$RI/bin:$PATH

if [ ! -d $MS ]; then
	hg clone --uncompressed https://dev.monetdb.org/hg/MonetDB/ $MS 
fi
(
	cd $MS
	(hg revert --all && hg pull && hg update $MB) > /dev/null
	./bootstrap > $LG/monetdb-bootstrap 2>&1
	./configure > $LG/monetdb-configure 2>&1
	cd ./tools/embedded/
	STAGEDIR=$LS ./build-rpkg.sh < /dev/null > $LG/build-rpkg 2>&1
)

export MAKEFLAGS="-j 16"

rm -r $RT
mkdir -p $RT
R CMD check --as-cran -o $RT $LS/MonetDBLite_*.tar.gz > $LG/check-rpkg 2>&1

FM=monetdblite-cran-check@cwi.nl
TO=hannes@cwi.nl
HS=`hostname`
SB="MonetDBLite check results"
MG=`cat $LG/check-rpkg`
echo -e "HELO $HS\r\nMAIL FROM:<$FM>\r\nRCPT TO:<$TO>\r\nDATA\r\nFrom: $FM\r\nSubject: [$HS] $SB\r\nTo: $TO\r\n\r\n$MG\r\n." | nc smtp.cwi.nl 25 > /dev/null

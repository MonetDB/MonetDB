#!/bin/sh 
#
# quick hack that sets up the documentation web, by Peter Boncz 2002
#
# somebody should reverse engineer this into the autogen system.. 
#
srcdir=$1
dstdir=$2
installdir=$3

mx()
{
	(cd /tmp; $installdir/bin/Mx -H1 -w $F.mx 2>/dev/null; mv $F.body.html $F.html 2>/dev/null; mv $F.html $installdir/doc/www/; rm -f /tmp/$F.*html)
}

rm -rf $installdir/doc/www
mkdir $installdir/doc/www

cp $srcdir/doc/monet.gif $srcdir/doc/mel.gif $installdir/doc
cp $srcdir/src/gdk/bat.gif $srcdir/src/gdk/bat1.gif $srcdir/src/gdk/bat2.gif $installdir/doc/www

for F in monet mil mel
do 
	(cd /tmp; $installdir/bin/Mx -H1 -w $srcdir/doc/$F.mx 2>/dev/null; mv $F.body.html $F.html 2>/dev/null; mv $F.html $installdir/doc/; rm -f /tmp/$F.*html)
done

F=gdk
cp $srcdir/src/gdk/$F.mx /tmp
mx

F=gdk_atoms
cp $srcdir/src/gdk/$F.mx /tmp
mx

F=monet
cp $srcdir/src/monet/$F.mx /tmp
mx

F=mel
cp $srcdir/src/mel/$F.mx /tmp
mx

F=MapiClient
cp $srcdir/src/mapi/clients/$F.mx /tmp
mx

F=Mserver
cp $srcdir/src/tools/$F.mx /tmp
mx

F=mapi
cp $srcdir/src/mapi/$F.mx /tmp
mx

F=calib
cp $srcdir/src/modules/calibrator/$F.mx /tmp
mx

for F in aggrX3 aggr alarm algebra arith ascii_io bat bitset bitvector blob counters ddbench decimal enum kernel \
	 lock logger mmath monettime mprof oo7 qt radix streams str sys tcpip tpcd trans unix url wisc xtables 
do
	cp $srcdir/src/modules/plain/$F.mx /tmp
	mx
done

(cd $srcdir/scripts/gold; cp README init.mil load.mil $dstdir/doc/www/)
(cd $srcdir/; cp HowToStart $dstdir/doc/www/)

echo '<html><body><h3><a href="mailto:niels@cwi.nl">The SQL frontend is not documented yet</a></h3></body></html>' > $dstdir/doc/www/sql.html

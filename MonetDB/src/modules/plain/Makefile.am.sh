#!/bin/sh

# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the Monet Database System.
# 
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2003 CWI.
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

for i in `ls *.mx` ; do
	f=`basename $i .mx` 
	files="$files $f"
done;

BUILT_SOURCES=""
HEADERS=""
LIBS=""
EXTRA_DIST=""
rm -f tmp_libsources
for i in $files; do
	LIB_SOURCES="lib_${i}_la_SOURCES = "
	if [ "`grep '^@h' $i.mx 2>/dev/null`" ]; then
		HEADERS="$HEADERS $i.h"
		LIB_SOURCES="$LIB_SOURCES $i.h"
	fi 
	if [ "`grep '^@c' $i.mx 2>/dev/null`" ]; then
		BUILT_SOURCES="$BUILT_SOURCES $i.c"
		LIB_SOURCES="$LIB_SOURCES $i.c"
	fi 
	if [ "`grep '^@C' $i.mx 2>/dev/null`" ]; then
		BUILT_SOURCES="$BUILT_SOURCES $i.cc"
		LIB_SOURCES="$LIB_SOURCES $i.cc"
	fi 
	if [ "`grep '^@m$' $i.mx 2>/dev/null`" ]; then
		BUILT_SOURCES="$BUILT_SOURCES ${i}_glue.c"
		HEADERS="$HEADERS $i.proto.h $i.m"
		LIB_SOURCES="$LIB_SOURCES ${i}_glue.c $i.proto.h"
		LIBS="$LIBS lib_$i.la"
	fi 
	if [ "`grep '^@mil' $i.mx 2>/dev/null`" ]; then
		BUILT_SOURCES="$BUILT_SOURCES $i.mil"
	fi
	echo $LIB_SOURCES >> tmp_libsources
	EXTRA_DIST="$EXTRA_DIST $i.mx"
done;

cat <<EOF
## Makefile.am generated using Makefile.am.sh
## Process this file with automake to produce Makefile.in

#AUTOMAKE_OPTIONS = no-dependencies

plainincludedir = \$(includedir)/plain

EOF

echo "plaininclude_HEADERS = $HEADERS"
echo "BUILT_SOURCES = \$(plaininclude_HEADERS) $BUILT_SOURCES"

cat <<EOF

INCLUDES = -I\$(top_builddir)/src/gdk -I\$(top_builddir)/src/monet
CFLAGS+=\$(thread_safe_flag_spec)
CXXFLAGS+=\$(thread_safe_flag_spec)

# interface:implementation:age
LT_VERSION=0:1:0

EOF

echo "EXTRA_DIST = Makefile.am.sh $EXTRA_DIST"

cat <<EOF

#-version-info \$(LT_VERSION)
EOF
echo "pkglib_LTLIBRARIES = $LIBS"
echo

cat tmp_libsources
rm -f tmp_libsources

cat <<EOF
lib_bitvector_la_LIBADD = lib_io.la lib_blob.la
lib_ddbench_la_LIBADD = lib_xtables.la lib_io.la lib_blob.la lib_enum.la
lib_enum_la_LIBADD = lib_io.la lib_blob.la
lib_io_la_LIBADD = lib_blob.la
lib_tpcd_la_LIBADD = lib_cluster.la
lib_vector_la_LIBADD = lib_mmath.la
lib_qt_la_LIBADD = lib_stats.la lib_uchr.la lib_bitvector.la
lib_salgebra_la_LIBADD= lib_algebra.la
EOF

cat <<EOF

include \$(top_srcdir)/rules.mk

bitvector.m: io.m
ddbench.m: enum.m xtables.m
enum.m: io.m
io.m: blob.m
tcpd.m: cluster.m
vector.m: mmath.m
qt.m: stats.m uchr.m bitvector.m
salgebra.m: algebra.m

dist-hook:
	mkdir \$(distdir)/Tests
	cd \$(srcdir)/Tests; cp [^C]* \$(distdir)/Tests
EOF

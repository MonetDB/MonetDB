#!/bin/sh

gis_FILES="geometry baserelate precision reader direction interval point box segment \
   triangle circle polystruct polyline polygon cpolygon \
   multipoint \
   distance overlap inside triangulate \
   tri_construct tri_misc tri_monotone tri_tri \
   varray varlen varheap chull minitree point3d triangle3d  box3d \
   Array Pqueue Set"
 
opengis_FILES="writer opengiswriter Geometry opengisreader MultiLineString \
   MultiPolygon GeometryCollection Polygon  \
   PointPoint LineLine PointLine LineStringLineString \
   PointLineString MultiPointMultiPoint PointMultiPoint MultiPointLine \
   LineLineString MultiPointLineString PointPolygon MultiPointPolygon \
   PointMultiLineString minirealm PolygonLineString Reverse"

gisbulk_FILES="split rtree rtree_box rtree_point rtree_segment \
   rtree_polyline rtree_polygon rtree_djoin rtree_sjoin \
   interval_tree octree"

MODULES="gis opengis gisbulk blg opengisbulk gdkmem"

BUILT_SOURCES=""
HEADERS=""
LIBS=""
EXTRA_DIST=""
rm -f tmp_libsources
for i in $MODULES; do
	LIB_SOURCES="lib_${i}_la_SOURCES = "
	case $i in 
	gis) files=$gis_FILES;;
	opengis) files=$opengis_FILES;;
	gisbulk) files=$gisbulk_FILES;;
	*) files="";;
	esac
	for j in `echo $files` $i; do
	    if [ "`grep '^@h' $j.mx 2>/dev/null`" ]; then
		HEADERS="$HEADERS $j.h"
		LIB_SOURCES="$LIB_SOURCES $j.h"
	    fi 
	    if [ "`grep '^@c' $j.mx 2>/dev/null`" ]; then
		BUILT_SOURCES="$BUILT_SOURCES $j.c"
		LIB_SOURCES="$LIB_SOURCES $j.c"
	    fi 
	    if [ "`grep '^@C' $j.mx 2>/dev/null`" ]; then
		BUILT_SOURCES="$BUILT_SOURCES $j.cc"
		LIB_SOURCES="$LIB_SOURCES $j.cc"
	    fi 
	    if [ "`grep '^@m$' $j.mx 2>/dev/null`" ]; then
		BUILT_SOURCES="$BUILT_SOURCES ${j}_glue.c"
		HEADERS="$HEADERS $j.proto.h $j.m"
		LIB_SOURCES="$LIB_SOURCES ${j}_glue.c $j.proto.h"
		LIBS="$LIBS lib_$j.la"
	    fi 
	    if [ "`grep '^@mil$' $j.mx 2>/dev/null`" ]; then
		BUILT_SOURCES="$BUILT_SOURCES $j.mil"
	    fi 
	    EXTRA_DIST="$EXTRA_DIST $j.mx"
	done
	echo $LIB_SOURCES >> tmp_libsources
done;

cat <<EOF
## Makefile.am generated using Makefile.am.sh
## Process this file with automake to produce Makefile.in

#AUTOMAKE_OPTIONS = no-dependencies

geometryincludedir = \$(includedir)/geometry
EOF

echo "geometryinclude_HEADERS = $HEADERS"
echo "BUILT_SOURCES = \$(geometryinclude_HEADERS) $BUILT_SOURCES"

cat <<EOF

INCLUDES = -I\$(top_builddir)/src/gdk -I\$(top_builddir)/src/monet \
	   -I\$(top_builddir)/src/modules/plain

# interface:implementation:age
LT_VERSION=0:1:0

EOF

echo "EXTRA_DIST = Makefile.am.sh $EXTRA_DIST"

cat <<EOF

#-version-info \$(LT_VERSION)
EOF
echo "lib_LTLIBRARIES = $LIBS"
echo

cat tmp_libsources
rm -f tmp_libsources

cat <<EOF
lib_blg_la_LIBADD =   lib_gis.la
lib_gis_la_LIBADD =   lib_gdkmem.la
lib_opengis_la_LIBADD =   lib_gis.la
lib_gisbulk_la_LIBADD =   lib_opengis.la
lib_opengisbulk_la_LIBADD =   lib_opengis.la
EOF

cat <<EOF

include \$(top_srcdir)/rules.mk

blg.m:   gis.m
gis.m:   gdkmem.m
opengis.m:   gis.m
gisbulk.m:   opengis.m
opengisbulk.m:   opengis.m

dist-hook:
	mkdir \$(distdir)/Tests
	cd \$(srcdir)/Tests; cp [^C]* \$(distdir)/Tests
EOF

###########################################################################
# any targets in this makefiles?

TARGETS=MOD

###########################################################################
# check whether the following directories exist

DIRS=$(TRGDIR) $(INCDIR)/geometry

###########################################################################
# files

MODULES= gis opengis gisbulk blg opengisbulk gdkmem

gis_FILES = geometry baserelate precision reader direction interval point box segment \
   triangle circle polystruct polyline polygon cpolygon \
   multipoint \
   distance overlap inside triangulate \
   tri_construct tri_misc tri_monotone tri_tri \
   varray varlen varheap chull minitree point3d triangle3d  box3d
 
opengis_FILES = writer opengiswriter Geometry opengisreader MultiLineString \
   MultiPolygon GeometryCollection Polygon  \
   PointPoint LineLine PointLine LineStringLineString \
   PointLineString MultiPointMultiPoint PointMultiPoint MultiPointLine \
   LineLineString MultiPointLineString PointPolygon MultiPointPolygon \
   PointMultiLineString minirealm PolygonLineString Reverse

gisbulk_FILES =	split rtree rtree_box rtree_point rtree_segment \
   rtree_polyline rtree_polygon rtree_djoin rtree_sjoin \
   interval_tree octree


gis_LIBS= 	$(CXXLIB)
gisbulk_LIBS= 	$(CXXLIB)
blg_LIBS= 	$(CXXLIB)
opengis_LIBS= 	$(CXXLIB)
opengisbulk_LIBS= $(CXXLIB)
gdkmem_LIBS=	$(CXXLIB)

OTHER_SRCS= 	$(gisbulk_FILES) $(gis_FILES) $(opengis_FILES)
OTHER_HDRS= 


BASE=		$(MODULES) $(OTHER_SRCS) $(OTHER_HDRS)
MXFILES=        $(BASE:%=%.mx) Array.mx Pqueue.mx Set.mx

###########################################################################

###########################################################################
# ok, NOW, let's get top-level settings

include $(MONETHOME)/Makefile

ifeq ($(GNU), ON)
#   CXXFLAGS += -Wall
   CXXFLAGS +=
else
   CXXFLAGS += -fullwarn -woff 1174
endif

clean-epilog:
	$(MDBG) $(RM) $(INCDIR)/geometry/*
	
install-src: $(HFILES) $(MFILES) $(MHFILES)
	cp $(HFILES) $(INCDIR)/geometry
	cp $(MFILES) $(INCDIR)/geometry
	cp $(MHFILES) $(INCDIR)/geometry

INCLUDES+= \
	-I$(TRGDIR) \
	-I$(MONETDIST)/$(PREFIX)/modules/plain

#eof

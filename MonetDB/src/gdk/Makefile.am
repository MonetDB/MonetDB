## Process this file with automake to produce Makefile.in

#AUTOMAKE_OPTIONS = no-dependencies

gdkincludedir = $(includedir)/gdk

gdkinclude_HEADERS = \
	gdk.h gdk_align.h gdk_atoms.h gdk_bat.h gdk_batop.h \
	gdk_bbp.h gdk_compress.h gdk_delta.h gdk_desc.h gdk_heap.h \
	gdk_relop.h gdk_search.h gdk_setop.h gdk_storage.h gdk_tm.h \
	gdk_utils.h gdk_system.h 

GDK_SOURCES = \
	$(gdkinclude_HEADERS) \
	gdk_align.c gdk_atoms.c gdk_bat.c gdk_batop.c gdk_bbp.c \
	gdk_compress.c gdk_delta.c gdk_desc.c gdk_heap.c gdk_qsort.c \
	gdk_relop.c gdk_search.c gdk_setop.c gdk_storage.c gdk_tm.c \
	gdk_utils.c gdk_system.c

BUILT_SOURCES = $(GDK_SOURCES) 

INCLUDES = 

CFLAGS+=$(thread_safe_flag_spec)

# Build a libtool library for installation in libdir.
lib_LTLIBRARIES = libbat.la

EXTRA_DIST = \
	bat.eps gdk_atoms.mx gdk_delta.mx    gdk_relop.mx    gdk_utils.mx \
	bat1.eps gdk_bat.mx gdk_desc.mx     gdk_search.mx  \
	bat2.eps gdk_batop.mx gdk_heap.mx     gdk_setop.mx \
	gdk.mx        gdk_bbp.mx       gdk_system.mx  gdk_storage.mx \
	gdk_align.mx  gdk_compress.mx  gdk_qsort.mx    gdk_tm.mx

libbat_la_LDFLAGS = 
libbat_la_SOURCES = $(GDK_SOURCES)

include $(top_srcdir)/rules.mk

dist-hook:
	mkdir $(distdir)/Tests
	cd $(srcdir)/Tests; cp [^C]* $(distdir)/Tests


## Process this file with automake to produce Makefile.in

#AUTOMAKE_OPTIONS = no-dependencies

BUILT_SOURCES = \
	M2M.c mil_parser_tab.y mil_parser_yy.l mil_parser.c mil_parser.h

INCLUDES = -I$(top_builddir)/src/gdk 

# interface:implementation:age
LT_VERSION=0:1:0

EXTRA_DIST = M2M.mx  mil_parser.mx

#-version-info $(LT_VERSION)
bin_PROGRAMS = mil
mil_LDADD = $(top_builddir)/src/gdk/libbat.la $(THREAD_LIBS) $(MALLOC_LIBS) 

YFLAGS = -d

mil_SOURCES = $(BUILT_SOURCES) mil_parser_tab.h

include $(top_srcdir)/rules.mk

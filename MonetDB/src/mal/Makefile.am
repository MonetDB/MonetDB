## Process this file with automake to produce Makefile.in

#AUTOMAKE_OPTIONS = no-dependencies

BUILT_SOURCES = \
	Mal.c mal_parser_tab.y mal_parser_yy.l mal_parser.h mal_parser.c

INCLUDES = -I$(top_builddir)/src/gdk

# interface:implementation:age
LT_VERSION=0:1:0

EXTRA_DIST = Mal.mx  mal_parser.mx

#-version-info $(LT_VERSION)
bin_PROGRAMS = mal
mal_LDADD = $(top_builddir)/src/gdk/libbat.la $(THREAD_LIBS) $(MALLOC_LIBS)

YFLAGS = -d

mal_SOURCES = $(BUILT_SOURCES) mal_parser_tab.h

include $(top_srcdir)/rules.mk

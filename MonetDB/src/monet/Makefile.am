## Process this file with automake to produce Makefile.in

#AUTOMAKE_OPTIONS = no-dependencies

monetincludedir = $(includedir)/monet

monet_headers = \
	monet.h monet_client.h monet_context.h monet_deparse.h \
	monet_interpreter.h monet_multiplex.h monet_parse.h \
	monet_process.h monet_queue.h monet_tbl.h searchpath.h yytree.h 

monetinclude_HEADERS = $(monet_headers) monet_parse_tab.h

BUILT_SOURCES = \
	$(monet_headers) \
	monet_parse_tab.y monet_parse_yy.l \
	monet.c monet_client.c monet_context.c monet_deparse.c \
	monet_interpreter.c monet_multiplex.c monet_parse.c \
	monet_process.c monet_queue.c monet_tbl.c searchpath.c yytree.c 


INCLUDES = -I$(top_builddir)/src/gdk
CFLAGS+=$(thread_safe_flag_spec)

# interface:implementation:age
LT_VERSION=0:1:0

EXTRA_DIST = \
	monet.mx          monet_interpreter.mx  monet_queue.mx  yytree.mx \
	monet_client.mx   monet_multiplex.mx    monet_tbl.mx \
	monet_context.mx  monet_parse.mx        monet_tem.mx \
	monet_deparse.mx  monet_process.mx      searchpath.mx

#-version-info $(LT_VERSION)
lib_LTLIBRARIES = libmonet.la

YFLAGS = -d

libmonet_la_SOURCES = $(BUILT_SOURCES) monet_parse_tab.h

include $(top_srcdir)/rules.mk

dist-hook:
	mkdir $(distdir)/Tests
	cd $(srcdir)/Tests; cp [^C]* $(distdir)/Tests

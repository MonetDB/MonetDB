## Process this file with automake to produce Makefile.in

#AUTOMAKE_OPTIONS = no-dependencies

BUILT_SOURCES = \
	Mapi.c Mapi.h Mclient.c Mserver.cc Mserver.h initmodules.c \
	Mcreatedb.c Mshutdown.c Mload.c Mdump.c Mrecover.c Mcgi.cc Mcgi.h


INCLUDES = -I../gdk -I../monet
CFLAGS+=$(thread_safe_flag_spec)
CXXFLAGS+=$(thread_safe_flag_spec)

# interface:implementation:age
LT_VERSION=0:1:0

EXTRA_DIST = \
	Mapi.mx     Mcreatedb.mx  Mload.mx     Mshutdown.mx \
	Mcgi.mx     Mdump.mx      Mrecover.mx  initmodules.mx \
	Mclient.mx  Minspect.mx   Mserver.mx   static_modules.mx


GDK_LIBS = $(THREAD_LIBS) $(MALLOC_LIBS)
MONET_LIBS = $(SOCKET_LIBS) $(DL_LIBS) $(GDK_LIBS)
MAPI_LIBS = $(SOCKET_LIBS) $(GDK_LIBS)
MODS = \
	../modules/plain/lib_kernel.la ../modules/plain/lib_arith.la \
	../modules/plain/lib_bat.la ../modules/plain/lib_algebra.la \
	../modules/plain/lib_sys.la ../modules/plain/lib_trans.la \
	../modules/plain/lib_str.la 

#-version-info $(LT_VERSION)
lib_LTLIBRARIES = libMapi.la
libMapi_la_SOURCES = Mapi.c Mapi.h

bin_PROGRAMS = Mclient Mserver Mcreatedb Mshutdown Mload Mdump Mrecover Mcgi

Mclient_SOURCES = Mclient.c 
Mclient_LDADD = $(READLINE_LIBS) $(SOCKET_LIBS)

Mserver_SOURCES = Mserver.cc Mserver.h initmodules.c
Mserver_LDADD = ../monet/libmonet.la $(MODS) ../gdk/libbat.la $(MONET_LIBS)

Mcreatedb_SOURCES = Mcreatedb.c 
Mshutdown_SOURCES = Mshutdown.c 
Mshutdown_LDADD = libMapi.la ../gdk/libbat.la $(MONET_LIBS) 

Mload_SOURCES = Mload.c initmodules.c
Mload_LDADD = ../monet/libmonet.la $(MODS) ../gdk/libbat.la $(MONET_LIBS)

Mdump_SOURCES = Mdump.c 
Mdump_LDADD = libMapi.la ../gdk/libbat.la $(MAPI_LIBS)

Mrecover_SOURCES = Mrecover.c 
Mrecover_LDADD = libMapi.la ../gdk/libbat.la $(MAPI_LIBS)

Mcgi_SOURCES = Mcgi.cc Mcgi.h
Mcgi_LDADD = libMapi.la ../gdk/libbat.la $(MAPI_LIBS)

include $(top_srcdir)/rules.mk

install-exec-local:
	$(RM) $(DESTDIR)$(bindir)/Mdestroydb
	$(LN_S) $(DESTDIR)$(bindir)/Mcreatedb $(DESTDIR)$(bindir)/Mdestroydb 

uninstall-local:
	$(RM) $(bindir)/Mdestroydb

dist-hook:
	mkdir $(distdir)/Tests
	cd $(srcdir)/Tests; cp [^C]* $(distdir)/Tests

## Process this file with automake to produce Makefile.in

#AUTOMAKE_OPTIONS = no-dependencies

BUILT_SOURCES = 

CFLAGS += -DUNIX -DMX_CXX_SUFFIX=\"$(CXXSFX)\"

EXTRA_DIST =  Mtest.in Mlog.in Mfilter.in MkillUsers.in Mlimit.c

#-version-info $(LT_VERSION)
bin_PROGRAMS = Mdiff Mtimeout 
bin_SCRIPTS = Mtest Mlog Mfilter MkillUsers

Mdiff_SOURCES = \
	difflib.c helpers.c Mdiff.c difflib.h  helpers.h

Mtimeout_SOURCES = \
	Mtimeout.c

install:
	$(RM) $(DESTDIR)$(bindir)/Mapprove 
	$(LN_S) $(DESTDIR)$(bindir)/Mtest $(DESTDIR)$(bindir)/Mapprove

uninstall:
	$(RM) $(bindir)/Mapprove 


# PF_LOADER_PROGRAMS is set to pf-shred during configure
# as long as the libxml2 library could be found. Otherwise
# PF_LOADER_PROGRAMS will be empty and this Makefile will
# not produce anything.
# automake requires all programs that might be substituted
# into bin_PROGRAMS to be listed in EXTRA_PROGRAMS.
bin_PROGRAMS = $(PF_LOADER_PROGRAMS)
EXTRA_PROGRAMS = pf-shred

pf_shred_SOURCES = pf-shred.c

pf_shred_CFLAGS = $(AM_CFLAGS) $(LIBXML2_CFLAGS) $(LIBDB_CFLAGS)

pf_shred_LDFLAGS = $(AM_LDFLAGS) $(LIBXML2_LIBS) $(LIBDB_LIBS)

pf_shred_LDADD = $(LIBXML2_LDADD) $(LIBDB_LDADD)

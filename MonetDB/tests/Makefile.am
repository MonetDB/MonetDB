## Process this file with automake to produce Makefile.in

AUTOMAKE_OPTIONS = 1.4 foreign

SUBDIRS =

dist-hook:
	mkdir $(distdir)/tests
	cd $(srcdir)/tests; cp -r [^C]* $(distdir)/tests

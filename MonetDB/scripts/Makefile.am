## Process this file with automake to produce Makefile.in

AUTOMAKE_OPTIONS = 1.4 foreign

SUBDIRS = 

EXTRA_DIST = README  general.mil  wwwgroup  wwwmake.mil wwwmake

dist-hook:
	mkdir $(distdir)/adm
	cd $(srcdir)/adm; cp [^C]* $(distdir)/adm
	mkdir $(distdir)/guest
	cd $(srcdir)/guest; cp [^C]* $(distdir)/guest
	mkdir $(distdir)/user
	cd $(srcdir)/user; cp [^C]* $(distdir)/user
	mkdir $(distdir)/user
	cd $(srcdir)/user; cp [^C]* $(distdir)/user
	mkdir $(distdir)/gold
	cd $(srcdir)/gold; cp [^C]* $(distdir)/gold

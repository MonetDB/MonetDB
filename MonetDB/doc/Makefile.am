## Process this file with automake to produce Makefile.in

AUTOMAKE_OPTIONS = 1.4 foreign

SUBDIRS = 
# TODO doc tests

EXTRA_DIST = mel.fig mel.eps mel.mx mil.mx monet.mx monet.eps  \
	odmg.fig odmg_odl.eps odmg.eps  odmg_meta.fig  \
	Example.mx        model.eps  odmg_app.eps      odmg_odl.fig \
	JavaImages.tar.Z  meta.mx  model.fig  odmg_app.fig    odmg_stdlib.eps \
	odmg_example.eps  odmg_stdlib.fig app.mx          odmg_example.fig \
	arch.eps          odl.mx     odmg_meta.eps     stdlib.mx

dist-hook:
	mkdir $(distdir)/papers
	cd $(srcdir)/papers; cp [^C]* $(distdir)/papers

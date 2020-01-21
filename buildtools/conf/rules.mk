# -*- makefile -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

CP=cp
MV=mv

%.tab.c: %.y
	$(BISON) -o $*.tab.c --defines=$*.tmph.h $(YFLAGS) $(AM_YFLAGS) $<
	rm -f $*.tmph.h

%.tab.h: %.y
	$(BISON) -o $*.tmpc.c --defines=$*.tab.h $(YFLAGS) $(AM_YFLAGS) $<
	rm -f $*.tmpc.c

%.def: %.syms
	case `(uname -s) 2> /dev/null || echo unknown` in CYGWIN*) cat $<;; *) sed '/DllMain/d;s/=.*//' $<;; esac > $@

SUFFIXES-local: $(BUILT_SOURCES)

ifdef DEB_BUILD_ARCH
# see buildtools/autogen/autogen/am.py:am_python()
PY_INSTALL_LAYOUT = --install-layout=deb
endif

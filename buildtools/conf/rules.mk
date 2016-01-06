# -*- makefile -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

CP=cp
MV=mv

%.tab.c: %.y
	touch waiting.$$$$ && until ln waiting.$$$$ waiting 2>/dev/null; do sleep 1; done && rm waiting.$$$$
	$(YACC) $(YFLAGS) $(AM_YFLAGS) $< || { $(RM) waiting ; exit 1 ; }
	if [ -f y.tab.c ]; then $(MV) y.tab.c $*.tab.c ; fi
	[ ! -f y.tab.h ] || $(RM) y.tab.h
	$(RM) waiting

%.tab.h: %.y
	touch waiting.$$$$ && until ln waiting.$$$$ waiting 2>/dev/null; do sleep 1; done && rm waiting.$$$$
	$(YACC) $(YFLAGS) $(AM_YFLAGS) $< || { $(RM) waiting ; exit 1 ; }
	if [ -f y.tab.h ]; then $(MV) y.tab.h $*.tab.h ; fi
	[ ! -f y.tab.c ] || $(RM) y.tab.c
	$(RM) waiting

%.yy.c: %.l
	touch waiting.$$$$ && until ln waiting.$$$$ waiting 2>/dev/null; do sleep 1; done && rm waiting.$$$$
	$(LEX) $(LFLAGS) $(AM_LFLAGS) $< || { $(RM) waiting ; exit 1 ; }
	[ -f $*.yy.h ] && $(RM) $*.yy.h
	$(RM) waiting

%.yy.h: %.l
	touch waiting.$$$$ && until ln waiting.$$$$ waiting 2>/dev/null; do sleep 1; done && rm waiting.$$$$
	$(LEX) $(LFLAGS) $(AM_LFLAGS) $< || { $(RM) waiting ; exit 1 ; }
	[ -f $*.yy.c ] && $(RM) $*.yy.c
	$(RM) waiting

%.def: %.syms
	case `(uname -s) 2> /dev/null || echo unknown` in CYGWIN*) cat $<;; *) sed '/DllMain/d;s/=.*//' $<;; esac > $@

SUFFIXES-local: $(BUILT_SOURCES)

ifdef DEB_BUILD_ARCH
# see buildtools/autogen/autogen/am.py:am_python()
PY_INSTALL_LAYOUT = --install-layout=deb
endif

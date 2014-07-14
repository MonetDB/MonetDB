# -*- makefile -*-

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

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
	case `(uname -s) 2> /dev/null || echo unknown` in CYGWIN*) cat $<;; *) grep -v DllMain $<;; esac > $@

SUFFIXES-local: $(BUILT_SOURCES)

ifdef DEB_BUILD_ARCH
# see buildtools/autogen/autogen/am.py:am_python()
PY_INSTALL_LAYOUT = --install-layout=deb
endif

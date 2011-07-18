# -*- makefile -*-

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

CP=cp
MV=mv
HIDE=1
MX = $(top_builddir)/buildtools/Mx/Mx

# in the next few rules, make sure that "$(CONFIG_H)" is included
# first, also with bison-generated files.  This is crucial
# to prevent inconsistent (re-)definitions of macros.
%.tab.c: %.y
	$(LOCKFILE) waiting
	$(YACC) $(YFLAGS) $< || { $(RM) waiting ; exit 1 ; }
	if [ -f y.tab.c ]; then $(MV) y.tab.c $*.tab.c ; fi
	$(MV) $*.tab.c $*.tab.c.tmp
	echo '#include <'"$(CONFIG_H)"'>' > $*.tab.c
	grep -v '^#include.*[<"]'"$(CONFIG_H)"'[">]' $*.tab.c.tmp >> $*.tab.c
	$(RM) $*.tab.c.tmp
	[ ! -f y.tab.h ] || $(RM) y.tab.h
	$(RM) waiting

%.tab.h: %.y
	$(LOCKFILE) waiting
	$(YACC) $(YFLAGS) $< || { $(RM) waiting ; exit 1 ; }
	if [ -f y.tab.h ]; then $(MV) y.tab.h $*.tab.h ; fi
	[ ! -f y.tab.c ] || $(RM) y.tab.c
	$(RM) waiting

%.def: %.syms
	case `(uname -s) 2> /dev/null || echo unknown` in CYGWIN*) cat $<;; *) grep -v DllMain $<;; esac > $@

%.h: %.mx
	$(MX) $(MXFLAGS) -l -x h $<

%.c: %.mx
	$(MX) $(MXFLAGS) -x c $<

%.y: %.mx
	$(MX) $(MXFLAGS) -x y $< 

%.mal: %.mx
	$(MX) $(MXFLAGS) -l -x mal $<

%.sql: %.mx
	$(MX) $(MXFLAGS) -l -x sql $<

%: %.mx 
	$(MX) $(MXFLAGS) -l -x sh $<
	chmod a+x $@

# The following rules generate two files using swig, the .xx.c and the
# .xx file.  There may be a race condition here when using a parallel
# make.  We try to alleviate the problem by sending the .xx.c output
# to a dummy file in the second rule.
# We also make sure that "$(CONFIG_H)" is included first, also with
# swig-generated files.  This is crucial to prevent inconsistent
# (re-)definitions of macros.
%.pm.c: %.pm.i
	$(SWIG) -perl5 $(SWIGFLAGS) -outdir . -o $@ $<
	$(MV) $@ $@.tmp
	echo '#include <'"$(CONFIG_H)"'>' > $@
	grep -v '^#include.*[<"]'"$(CONFIG_H)"'[">]' $@.tmp >> $@
	$(RM) $@.tmp

%.pm: %.pm.i
	$(SWIG) -perl5 $(SWIGFLAGS) -outdir . -o dummy.c $<
	$(RM) dummy.c

%.tex: %.mx
	$(MX) -1 -H$(HIDE) -t $< 

%.bdy.tex: %.mx
	$(MX) -1 -H$(HIDE) -t -B $<

%.html: %.mx
	$(MX) -1 -H$(HIDE) -w $<

%.bdy.html: %.mx
	$(MX) -1 -H$(HIDE) -w -B $<

# if the .tex source file is found in srcdir (via VPATH), there might
# be a '.'  in the path, which latex2html doesn't like; hence, we
# temporarly link the .tex file to the local build dir.
%.html: %.tex
	if [ "$<" != "$(<F)" ] ; then $(LN_S) $< $(<F) ; fi
	$(LATEX2HTML) -split 0 -no_images -info 0 -no_subdir  $(<F)
	if [ "$<" != "$(<F)" ] ; then $(RM) $(<F) ; fi

%.pdf: %.tex
	$(PDFLATEX) $< 

%.dvi: %.tex
	$(LATEX) $< 

%.ps: %.dvi
	$(DVIPS) $< -o $@

%.eps: %.fig
	$(FIG2DEV) -L$(FIG2DEV_EPS) -e $< > $@

%.eps: %.feps
	$(CP) $< $@

SUFFIXES-local: $(BUILT_SOURCES)

distdir: check_dist
check_dist:
	@if [ "$(SWIG)" = "no" ]; then echo "Cannot create distribution because one of the necessary programs or libraries is missing"; echo "swig	= $(SWIG)"; exit 1; fi

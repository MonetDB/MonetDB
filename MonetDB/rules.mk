# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the Monet Database System.
# 
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2003 CWI.
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

#
# ! this file should be kept identical in                                            !
# ! MonetDB, monet5, sql, xml, acoi, gis, template, playpen, misq, times100, pruning !
#

CP=cp
MV=mv
MXFLAGS= -n

%.h: %.mx
	$(MX) $(MXFLAGS) -x h $<

%.c: %.mx
	$(MX) $(MXFLAGS) -x c $<

%.y: %.mx
	$(MX) $(MXFLAGS) -x y $< 

%.l: %.mx
	$(MX) $(MXFLAGS) -x l $< 

%.yy.c: %.l
	$(LEX) $(LFLAGS) $<
	if [ -f lex.yy.c ]; then $(MV) lex.yy.c $*.yy.c ; fi
	if [ -f lex.$(PARSERNAME).c ]; then $(MV) lex.$(PARSERNAME).c $*.yy.c ; fi
	# make sure that "config.h" is included first, also with [f]lex-generated files.
	# This is crucial to prevent inconsistent (re-)definitions of macros.
	$(MV) $*.yy.c $*.yy.c.tmp
	echo '#include <config.h>' > $*.yy.c
	grep -v '^#include.*[<"]config.h[">]' $*.yy.c.tmp >> $*.yy.c
	rm -f $*.yy.c.tmp

%.cc: %.mx
	$(MX) $(MXFLAGS) -x C $<

%.yy: %.mx
	$(MX) $(MXFLAGS) -x Y $< 

%.tab.cc: %.yy
	$(LOCKFILE) waiting
	$(YACC) $(YFLAGS) $<
	if [ -f y.tab.c ]; then $(MV) y.tab.c $*.tab.cc ; fi
	rm -f waiting

%.tab.h: %.yy
	$(LOCKFILE) waiting
	$(YACC) $(YFLAGS) $<
	if [ -f y.tab.h ]; then $(MV) y.tab.h $*.tab.h ; fi
	rm -f waiting

%.tab.c: %.y
	$(LOCKFILE) waiting
	$(YACC) $(YFLAGS) $<
	if [ -f y.tab.c ]; then $(MV) y.tab.c $*.tab.c ; fi
	rm -f waiting

%.tab.h: %.y
	$(LOCKFILE) waiting
	$(YACC) $(YFLAGS) $<
	if [ -f y.tab.h ]; then $(MV) y.tab.h $*.tab.h ; fi
	rm -f waiting

%.ll: %.mx
	$(MX) $(MXFLAGS) -x L $<

%.yy.cc: %.ll
	$(LEX) $(LFLAGS) $<
	if [ -f lex.yy.c ]; then $(MV) lex.yy.c $*.yy.cc ; fi
	# make sure that "config.h" is included first, also with [f]lex-generated files.
	# This is crucial to prevent inconsistent (re-)definitions of macros.
	$(MV) $*.yy.cc $*.yy.cc.tmp
	echo '#include <config.h>' > $*.yy.cc
	grep -v '^#include.*[<"]config.h[">]' $*.yy.cc.tmp >> $*.yy.cc
	rm -f $*.yy.cc.tmp

%.m: %.mx
	$(MX) $(MXFLAGS) -x m $<

%.mil: %.m %.tmpmil $(MEL)
	$(MEL) $(INCLUDES) -mil $*.m > $@
	cat $*.tmpmil >> $@
	test -e .libs || mkdir -p .libs
	test -e .libs/$@ || $(LN_S) ../$@ .libs/$@

%.tmpmil: %.mx
	$(MX) $(MXFLAGS) -l -x mil $<
	$(MV) $*.mil $*.tmpmil

%.mil: %.m $(MEL)
	$(MEL) $(INCLUDES) -mil $*.m > $@
	test -e .libs || mkdir -p .libs
	test -e .libs/$@ || $(LN_S) ../$@ .libs/$@

%.mil: %.mx
	$(MX) $(MXFLAGS) -x mil $<

%.mal: %.mx
	$(MX) $(MXFLAGS) -x mal $<

%: %.mx 
	$(MX) $(MXFLAGS) -x sh $<
	chmod a+x $@

%.proto.h: %.m $(MEL)
	$(MEL) $(INCLUDES) -proto $< > $@

%.glue.c: %.m $(MEL)
	$(MEL) $(INCLUDES) -glue $< > $@

%.tex: %.mx
	cat $< > /tmp/doc.mx
	$(MX) -1 -H$(HIDE) -t /tmp/doc.mx 
	$(MV) doc.tex $@
	$(RM) /tmp/doc.mx

%.html: %.mx
	cat $< > /tmp/doc.mx
	$(MX) -1 -H$(HIDE) -w /tmp/doc.mx 
	$(MV) doc.html $@
	$(RM) /tmp/doc.mx

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

$(NO_INLINE_FILES:.mx=.lo): %.lo: %.c
	$(LIBTOOL) --mode=compile $(COMPILE) $(NO_INLINE_CFLAGS) -c $<

$(patsubst %.mx,%.lo,$(filter %.mx,$(NO_OPTIMIZE_FILES))): %.lo: %.c
	$(LIBTOOL) --mode=compile $(COMPILE) -O0 -c $<

$(patsubst %.c,%.o,$(filter %.c,$(NO_OPTIMIZE_FILES))): %.o: %.c
	$(COMPILE) -O0 -c $<

SUFFIXES-local: $(BUILT_SOURCES)

$(prefix)/doc/Mx/mxdoc.tex:  $(top_srcdir)/doc/mxdoc.tex
	-@mkdir -p $(prefix)/doc/Mx
	cp $(top_srcdir)/doc/mxdoc.tex $(prefix)/doc/Mx

$(prefix)/doc/Mx/mxdoc.aux:  $(prefix)/doc/Mx/mxdoc.tex
	(cd $(prefix)/doc/Mx; latex mxdoc.tex; latex mxdoc.tex)

html:	$(prefix)/doc/Mx/mxdoc.aux
	(cd $(prefix); latex2html -ascii_mode -noimages -notiming -noaddress -style http://monetdb.cwi.nl/MonetDB.css -dir doc/Mx doc/Mx/mxdoc.tex)
	-@mkdir -p $(prefix)/doc/MapiJava
	lynx -source http://monetdb.cwi.nl/MonetDB.css > $(prefix)/doc/MapiJava/MonetDB.css
	javadoc -d $(prefix)/doc/MapiJava -stylesheetfile $(prefix)/doc/MapiJava/MonetDB.css\
		$(top_srcdir)/src/mapi/clients/java/MapiClient.java       		\
	        $(top_srcdir)/src/mapi/clients/java/mapi/Mapi.java        		\
	        $(top_srcdir)/src/mapi/clients/java/mapi/MapiException.java
	python $(top_srcdir)/doc/mkdoc.py $(top_srcdir) $(prefix)

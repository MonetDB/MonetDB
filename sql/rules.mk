MEL=$(MONETDIST)/bin/mel
MX=$(MONETDIST)/bin/Mx
MXFLAGS= -notouch

%.h: %.mx
	$(MX) $(MXFLAGS) -x h $<

%.c: %.mx
	$(MX) $(MXFLAGS) -x c $<

%.y: %.mx
	$(MX) $(MXFLAGS) -x y $< 

%.tab.c %.tab.h: %.y
	$(YACC) $(YFLAGS) $<
	if [ -f y.tab.c ]; then $(MV) y.tab.c $*.tab.c ; fi
	if [ -f y.tab.h ]; then $(MV) y.tab.h $*.tab.h ; fi

%.l: %.mx
	$(MX) $(MXFLAGS) -x l $< 

%.yy.c: %.l
	$(LEX) $(LFLAGS) $*.l
	if [ -f lex.yy.c ]; then $(MV) lex.yy.c $*.yy.c ; fi

%.m: %.mx
	$(MX) $(MXFLAGS) -x m $<

%.mil: %.mx
	$(MX) $(MXFLAGS) -x mil $<

%.java: %.mx
	$(MX) $(MXFLAGS) -x java $<

%.tcl: %.mx
	$(MX) $(MXFLAGS) -x tcl $<

%.xsl: %.mx
	$(MX) $(MXFLAGS) -x xsl $<

%: %.mx
	$(MX) $(MXFLAGS) -x sh $<

%.i: %.mx
	$(MX) $(MXFLAGS) -x swig $<

%_wrap.c: %.i
	$(SWIG) -tcl8 -o $@ $<

%.fgr: %.mx
	$(MX) $(MXFLAGS) -x fgr $<

%.proto.h: %.m
	$(MEL) $(INCLUDES) -o $@ -proto $<

%.glue.c: %.m
	$(MEL) $(INCLUDES) -o $@ -glue $<

pkgIndex.tcl: $(TCLFILES)
	@echo "pkg_mkIndex . *.tcl;exit;" | $(TCLSH)	

SUFFIXES = .m .mx .proto.h .mil .glue.c
PRECIOUS = .m 

all-local: $(BUILT_SOURCES)

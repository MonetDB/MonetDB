
MEL=$(top_builddir)/src/mel/mel
MX=$(top_builddir)/src/utils/Mx/Mx
CP=cp
MV=mv
MXFLAGS= -notouch

%.h: %.mx
	$(MX) $(MXFLAGS) -x h $<

%.c: %.mx
	$(MX) $(MXFLAGS) -x c $<

%.y: %.mx
	$(MX) $(MXFLAGS) -x y $< 

%.tab.c %.tab.h: %.y
	$(YACC) $(YFLAGS) $*.y
	if [ -f y.tab.c ]; then $(MV) y.tab.c $*.tab.c ; fi
	if [ -f y.tab.h ]; then $(MV) y.tab.h $*.tab.h ; fi

%.l: %.mx
	$(MX) $(MXFLAGS) -x l $< 

%.yy.c: %.l
	$(LEX) $(LFLAGS) $*.l
	if [ -f lex.yy.c ]; then $(MV) lex.yy.c $*.yy.c ; fi


%.cc: %.mx
	$(MX) $(MXFLAGS) -x C $<

%.yy: %.mx
	$(MX) $(MXFLAGS) -x Y $< 

%.tab.cc %.tab.h: %.yy
	$(YACC) $(YFLAGS) $*.yy
	if [ -f y.tab.c ]; then $(MV) y.tab.c $*.tab.cc ; fi
	if [ -f y.tab.h ]; then $(MV) y.tab.h $*.tab.h ; fi

%.ll: %.mx
	$(MX) $(MXFLAGS) -x L $<

%.yy.cc: %.ll
	$(LEX) $(LFLAGS) $*.ll
	if [ -f lex.yy.c ]; then $(MV) lex.yy.c $*.yy.cc ; fi

%.m: %.mx
	$(MX) $(MXFLAGS) -x m $<

%.mil: %.mx
	$(MX) $(MXFLAGS) -x mil $<

%: %.mx
	$(MX) $(MXFLAGS) -x sh $<

%.proto.h: %.m
	$(MEL) $(INCLUDES) -o $@ -proto $<

%.glue.c: %.m
	$(MEL) $(INCLUDES) -o $@ -glue $<

SUFFIXES = .m .mx .proto.h .mil .glue.c
PRECIOUS = .m 

all-local: $(BUILT_SOURCES)

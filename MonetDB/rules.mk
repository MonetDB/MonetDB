
MEL=$(top_builddir)/src/mel/mel
MX=$(top_builddir)/src/utils/Mx/Mx
MXFLAGS= -notouch

%.h: %.mx
	$(MX) $(MXFLAGS) -x h $<

%.c: %.mx
	$(MX) $(MXFLAGS) -x c $<

%_tab.y: %.mx
	$(MX) $(MXFLAGS) -x y $< 
	$(MV) $*.y $@

%_yy.l: %.mx
	$(MX) $(MXFLAGS) -x l $< 
	$(MV) $*.l $@

%.cc: %.mx
	$(MX) $(MXFLAGS) -x C $<

%_tab.yy: %.mx
	$(MX) $(MXFLAGS) -x Y $< 
	$(MV) $*.yy $@

%_yy.ll: %.mx
	$(MX) $(MXFLAGS) -x L $<
	$(MV) $*.ll $@

%.m: %.mx
	$(MX) $(MXFLAGS) -x m $<

%.mil: %.mx
	$(MX) $(MXFLAGS) -x mil $<

%.proto.h: %.m
	$(MEL) $(INCLUDES) -o $@ -proto $<

%.glue.c: %.m
	$(MEL) $(INCLUDES) -o $@ -glue $<

SUFFIXES = .m .mx .proto.h .mil .glue.c
PRECIOUS = .m 

all-local: $(BUILT_SOURCES)


MEL=$(top_builddir)/src/mel/mel
MX=$(top_builddir)/src/utils/Mx/Mx

%_tab.y: %.y
	$(MV) $^ $@

%_tab.yy: %.yy
	$(MV) $^ $@

%_yy.l: %.l
	$(MV) $^ $@

%_yy.ll: %.ll
	$(MV) $^ $@

%.cc: %.mx
	$(MX) $(MXFLAGS) -x C $<

%.c: %.mx
	$(MX) $(MXFLAGS) -x c $<

%.h: %.mx
	$(MX) $(MXFLAGS) -x h $<

%.y: %.mx
	$(MX) $(MXFLAGS) -x y $< 

%.yy: %.mx
	$(MX) $(MXFLAGS) -x y $< 
	$(MV) $*.y $@

%.l: %.mx
	$(MX) $(MXFLAGS) -x l $< 

%.ll: %.mx
	$(MX) $(MXFLAGS) -x l $<
	$(MV) $*.l $@

%.m: %.mx
	$(MX) $(MXFLAGS) -x m $<

%.mil: %.mx
	$(MX) $(MXFLAGS) -x mil $<

%.proto.h: %.m
	$(MEL) $(INCLUDES) -o $@ -proto $<

%_glue.c: %.m
	$(MEL) $(INCLUDES) -o $@ -glue $<

SUFFIXES = .m .mx .proto.h .mil
PRECIOUS = .m 

all-local: $(BUILT_SOURCES)

###########################################################################
# any targets in this makefiles?

TARGETS=YES
INCLUDE_FILES=ON

###########################################################################
# check whether the following directories exist

DIRS=$(TRGDIR) 

###########################################################################
# ok, NOW, let's get top-level settings

include $(MONETHOME)/Makefile
CFLAGS+=-DMONET_VERSION=\"$(MONET_VERSION)\" -DCOMP_TIME="\"$(shell date)\"" \
        -DMONET_ARCH="\"$(PREFIX)\"" 

###########################################################################
# overwrite debug level and parser

#MDBG=
YACC= bison -d  -p mal
QRYLANG=mal

###########################################################################
# files

BASE=		mal_parser Mal

MXFILES=	$(BASE:%=%.mx) 
OBJFILES= 	$(CFILES:%.c=%.o) $(PARSEOBJS)

###########################################################################
# targets

all:	Mal

Mal: $(BINDIR)/Mal
$(BINDIR)/Mal: $(OBJFILES)
	(cd $(LIBDIR); $(LINK.cc) -o $(BINDIR)/Mal -v \
	 $(TRGDIR)/*.o -L. -lbat $(LIBS) )

clean:
	$(MDBG) $(RM) $(OBJFILES)
	$(MDBG) $(RM) $(BINDIR)/Mal

dist-clean:
	$(MDBG) $(MAKE) clean
	$(MDBG) $(RM) $(TRGDIR)/*

install-src: $(LEXCFILES) $(YACCCFILES)

%.tab.h: %.y
	( cd $(@D); \
	$(YACC) -v $< )

%.tab.c: %.y
	( cd $(@D); \
	$(YACC) -v $< )

%.yy.c: %.l
	( cd $(@D); \
	$(LEX) $< ; \
	$(MV) lex.$(QRYLANG).c $@ )

#eof

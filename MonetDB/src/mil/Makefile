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
YACC= bison -d  -p mil
QRYLANG=mil

###########################################################################
# files

BASE=		mil_parser M2M

MXFILES=	$(BASE:%=%.mx) 
OBJFILES= 	$(CFILES:%.c=%.o) $(PARSEOBJS)

###########################################################################
# targets

all:	M2M

M2M: $(BINDIR)/M2M
$(BINDIR)/M2M: $(OBJFILES)
	(cd $(LIBDIR); $(LINK.cc) -o $(BINDIR)/M2M -v \
	 $(TRGDIR)/*.o -L. -lbat $(LIBS) )

clean:
	$(MDBG) $(RM) $(OBJFILES)
	$(MDBG) $(RM) $(BINDIR)/M2M

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

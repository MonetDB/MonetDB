###########################################################################
# working dir

TRGDIR=$(MONETHOME)/$(PREFIX)/src/utils/Mx

###########################################################################
# any targets in this makefiles?

TARGETS=YES
INCLUDE_FILES=OFF
OPTIMIZE=ON

###########################################################################
# check whether the following directories exist

DIRS=$(TRGDIR) 

###########################################################################
# ok, NOW, let's get top-level settings

OVERRIDE_PROFILE=ON
include $(MONETHOME)/Makefile

###########################################################################
# overwrite debug level

MDBG=

###########################################################################
# files

BASE=		Mx Index Def Form Tok Code Display Print Io Sys Tex2Html\
		disclaimer

SRCFILES= 	$(BASE:%=$(TRGDIR)/%.c) 
OBJFILES= 	$(BASE:%=$(TRGDIR)/%.o) 

###########################################################################
# targets

all:	$(TRGDIR) $(BINDIR)/Mx $(BINDIR)/idxmx $(BINDIR)/epsffit \
	$(BINDIR)/prefixMxFile

Mx: $(BINDIR)/Mx
$(BINDIR)/Mx:	$(OBJFILES)
	$(LINK.c) $^ -o $@ ${LINK_SUF}

$(BINDIR)/epsffit: $(TRGDIR)/epsffit.o
	$(LINK.c) $< -o $@  ${LINK_SUF}
	cp warning.gif epstogif $(BINDIR)

$(BINDIR)/idxmx: $(TRGDIR)/idxmx.o
	$(LINK.c) $< -o $@ ${LINK_SUF}

prefixMxFile: $(BINDIR)/prefixMxFile
$(BINDIR)/prefixMxFile: $(TRGDIR)/prefixMxFile.o $(TRGDIR)/disclaimer.o
	$(LINK.c) $^ -o $@ ${LINK_SUF}

clean:
	$(MDBG) $(RM) $(OBJFILES) $(BINDIR)/Mx $(BINDIR)/idxmx \
	$(TRGDIR)/idxmx.o $(TRGDIR)/prefixMxFile.o

dist-clean:
	$(MAKE) clean

$(OBJFILES): $(BASE:%=%.c)

$(TRGDIR)/%.o: %.c
	$(COMPILE.c) -o $@ $< -DUNIX -DMX_CXX_SUFFIX=\"$(CXXSFX)\"
#eof


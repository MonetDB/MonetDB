###########################################################################
# working dir

TRGDIR=$(MONETHOME)/$(PREFIX)/src/utils/rewrite

###########################################################################
# any targets in this makefiles?

TARGETS=YES
INCLUDE_FILES=OFF

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

OBJFILES= 	$(TRGDIR)/rewrite.o

###########################################################################
# targets

all:	$(TRGDIR) $(BINDIR)/rewrite

$(BINDIR)/rewrite: $(OBJFILES)
	$(LINK.c) -o $@ $^ $(LINK_SUF)

clean:
	$(MDBG) $(RM) $(OBJFILES) $(BINDIR)/rewrite

dist-clean:
	$(MAKE) clean


$(TRGDIR)/%.o: %.c
	$(COMPILE.c) -o $@ $<
#eof


###########################################################################
# working dir

THISDIR=src/utils/testing
TRGDIR=$(MONETHOME)/$(PREFIX)/$(THISDIR)

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

BASE=		Mtest Mapprove Mlog Mfilter Mdiff Mtimeout MkillUsers
BINFILES= 	$(BASE:%=$(BINDIR)/%) 

SRCFILES=	difflib.c helpers.c Mdiff.c
OBJFILES= 	$(SRCFILES:%.c=$(TRGDIR)/%.o)

###########################################################################
# targets

all:	$(TRGDIR) $(BINFILES)

$(BINDIR)/%:	%
	( sh -c 'type bash' | sed 's|^.* /|#!/|g' ; tail +2 $(@F) ) > $@
	@ chmod +x $@

$(BINDIR)/Mapprove:	$(BINDIR)/Mtest
	$(LN) $(<F) $@

$(BINDIR)/Mdiff:	$(OBJFILES)

$(BINDIR)/%:	$(TRGDIR)/%.o
	$(LINK.c) -o $@ $^ $(LINK_SUF)

clean:
	$(MDBG) $(RM) $(OBJFILES) $(BINFILES)

dist-clean:
	$(MAKE) clean


$(TRGDIR)/%.o: %.c
	$(COMPILE.c) -o $@ $<
#eof


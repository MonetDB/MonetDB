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

###########################################################################
# files

BASE=		mel FileInfo symbol symtable ops \
		module dependency atom atomops acc accops \
		command operator builtin iterator object \
		atom_arg type_arg var_arg bat_arg any_arg \
		prelude epilogue load drop \
		language use link depend proto debug html glue \
		list ListIterator

MXFILES=	$(BASE:%=%.mx)
OBJFILES=	$(COBJS) $(CXXOBJS) $(PARSEOBJS)

# Stefan.Manegold@cwi.nl (30.06.2000):
# mel compiled with native compiler and -mt 
# on apps.cwi.nl (SunOS 5.7 Generic_106541-10 sun4u sparc SUNW,Ultra-4)
# doesn't work...
ifeq ($(ARCH), SOLARIS)
ifeq ($(OSVER), 570)
ifeq ($(GNU), OFF)
CC=	cc -dalign -fnonstd
CXX=	CC
LD=	cc
endif
endif
endif

###########################################################################
# targets

all:	mel

mel: $(BINDIR)/mel 

$(BINDIR)/mel : $(OBJFILES)
	$(LINK.cc) -o $@ $^

clean:	
	$(MDBG) $(RM) $(OBJFILES)

dist-clean:
	$(MDBG) $(MAKE) clean
	$(MDBG) $(RM) -rf $(TRGDIR)/*

install-src:  $(YACCCXXFILES) $(LEXCXXFILES)

#eof



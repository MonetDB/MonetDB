###########################################################################
# any targets in this makefiles?

TARGETS=MOD

###########################################################################
# check whether the following directories exist

DIRS=$(TRGDIR) $(INCDIR)/plain

###########################################################################
# files

ifdef DDI_PATCH
MXFILES=kernel.mx bat.mx algebra.mx arith.mx str.mx sys.mx alarm.mx\
	aggr.mx bitset.mx blob.mx ddbench.mx decimal.mx enum.mx io.mx\
	lock.mx mmath.mx monettime.mx tcpip.mx trans.mx unix.mx xtables.mx
else
ifeq ($(ARCH),CYGWIN32)
MXFILES=$(filter-out iterator.mx, $(wildcard *.mx))
else
MXFILES=$(wildcard *.mx)
endif
endif
KERN_MODS=$(patsubst plain/%,%,$(KERNEL_MODULES))
MOD_NAMES=$(subst .mx,,$(MXFILES))
MODULES=$(KERN_MODS) $(filter-out $(KERN_MODS) mmath,$(MOD_NAMES)) mmath

###########################################################################
# ok, NOW, let's get top-level settings

include $(MONETHOME)/Makefile

ifeq ($(STATIC),OFF) 
mmath_LIBS = $(LIBM)
endif

ifeq ($(ARCH),LINUX)
 ifneq ($(findstring $(UNAME_CPU),i586 i686),)
  LIBPPERF=$(shell ls /usr/local/lib/libpperf.a 2>/dev/null)
  ifneq ($(LIBPPERF),)
   CFLAGS+=	-DHWCOUNTERS
   CXXFLAGS+=	-DHWCOUNTERS
   counters_LIBS=	-L/usr/local/lib -lpperf
   INCLUDES+=		-I/usr/local/include
  endif
 endif
endif
ifeq ($(ARCH),IRIX)
 ifeq ($(UNAME_CPU),IP27)
  CFLAGS+=	-DHWCOUNTERS
  CXXFLAGS+=	-DHWCOUNTERS
  counters_LIBS=	-lperfex
 endif
endif
ifeq ($(ARCH),SOLARIS)
 ifeq ($(UNAME_CPU),sun4u)
  DEVPERFMON=$(shell ls /dev/perfmon 2>/dev/null)
  ifneq ($(DEVPERFMON),)
   LIBPERFMON=($shell ls /usr/local/lib/libperfmon.a 2>/dev/null)
   ifneq ($(LIBPERFMON),)
    CFLAGS+=	-DHWCOUNTERS
    CXXFLAGS+=	-DHWCOUNTERS
    counters_LIBS=	-L/usr/local/lib -lperfmon
    INCLUDES+=		-I/usr/local/include
   endif
  endif
 endif
endif

clean-epilog:
	$(MDBG) $(RM) $(INCDIR)/plain/*
	
install-src: $(HFILES) $(MFILES) $(MHFILES)
	cp $(HFILES) $(INCDIR)/plain
	cp $(MFILES) $(INCDIR)/plain
	cp $(MHFILES) $(INCDIR)/plain

INCLUDES+= \
	-I$(INCDIR)/plain

###########################################################################
#eof


TOPDIR = .
SRCDIR = $(TOPDIR)\..
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

MONET_BUILD  = $(MAKEDIR)
MONET_PREFIX = $(MAKEDIR)

!INCLUDE $(TOPDIR)\rules.msc

all: $(SRCDIR)\Makefile.msc all-msc
	set MONET_BUILD=$(MONET_BUILD)
	set MONET_PREFIX=$(MONET_PREFIX)
	$(MAKE) /nologo /k /f $(SRCDIR)\Makefile.msc all

check: $(SRCDIR)\Makefile.msc RunMtest.bat
	$(MAKE) /nologo /k /i /f $(SRCDIR)\Makefile.msc check
	call RunMtest.bat

install: targetdirs
	set MONET_BUILD=$(MONET_BUILD)
	set MONET_PREFIX=$(MONET_PREFIX)
	$(MAKE) /nologo /k /f $(SRCDIR)\Makefile.msc install

$(SRCDIR)\Makefile.msc: $(SRCDIR)\src\utils\autogen\autogen.py \
	$(SRCDIR)\src\utils\autogen\codegen.py \
	$(SRCDIR)\src\utils\autogen\msc.py
	$(CD) $(SRCDIR) && $(PYTHON) src\utils\autogen\autogen.py

all-msc: config.h

config.h: winconfig.h
	$(INSTALL) winconfig.h config.h

RunMtest.bat: $(SRCDIR)\RunMtest.bat.in
	if exist $(SRCDIR)\RunMtest.bat.in $(CONFIGURE) $(SRCDIR)\RunMtest.bat.in > RunMtest.bat

targetdirs:
	if not exist $(bindir)		$(MKDIR) $(bindir)
	if not exist $(sbindir)		$(MKDIR) $(sbindir)
	if not exist $(libexecdir)	$(MKDIR) $(libexecdir)
	if not exist $(datadir)		$(MKDIR) $(datadir)
	if not exist $(sysconfdir)	$(MKDIR) $(sysconfdir)
	if not exist $(sharedstatedir)	$(MKDIR) $(sharedstatedir)
	if not exist $(localstatedir)	$(MKDIR) $(localstatedir)
	if not exist $(libdir)		$(MKDIR) $(libdir)
	if not exist $(infodir)		$(MKDIR) $(infodir)
	if not exist $(mandir)		$(MKDIR) $(mandir)
	if not exist $(includedir)	$(MKDIR) $(includedir)
	if not exist $(pkgdatadir)	$(MKDIR) $(pkgdatadir)
	if not exist $(pkglibdir)	$(MKDIR) $(pkglibdir)
	if not exist $(pkgincludedir)	$(MKDIR) $(pkgincludedir)

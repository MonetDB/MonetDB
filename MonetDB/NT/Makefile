
TOPDIR = .
SRCDIR = $(TOPDIR)\..
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

!INCLUDE $(TOPDIR)\rules.msc

all: $(SRCDIR)\Makefile.msc all-msc
	$(MAKE) /nologo /k /f $(SRCDIR)\Makefile.msc all

install: targetdirs
	$(MAKE) /nologo /k /f $(SRCDIR)\Makefile.msc install

$(SRCDIR)\Makefile.msc: $(SRCDIR)\src\utils\autogen\autogen.py \
	$(SRCDIR)\src\utils\autogen\codegen.py \
	$(SRCDIR)\src\utils\autogen\msc.py
	$(CD) $(SRCDIR) && $(PYTHON) src\utils\autogen\autogen.py

all-msc: config.h unistd.h sys\socket.h

config.h: winconfig.h
	$(INSTALL) winconfig.h config.h

unistd.h: 
	$(ECHO) #ifndef UNISTD_H > unistd.h
	$(ECHO) #define UNISTD_H >> unistd.h
	$(ECHO) #include "io.h" >> unistd.h
	$(ECHO) #endif >> unistd.h

sys\socket.h: 
	if not exist sys $(MKDIR) sys
	$(ECHO) #ifndef SOCKET_H > sys\socket.h
	$(ECHO) #define SOCKET_H >> sys\socket.h
	$(ECHO) #include "winsock.h" >> sys\socket.h
	$(ECHO) #endif >> sys\socket.h

targetdirs:
	if not exist $(libdir) $(MKDIR) $(libdir)
	if not exist $(bindir) $(MKDIR) $(bindir)
	if not exist $(datadir) $(MKDIR) $(datadir)
	if not exist $(pkglibdir) $(MKDIR) $(pkglibdir)
	if not exist $(pkgbindir) $(MKDIR) $(pkgbindir)
	if not exist $(pkgdatadir) $(MKDIR) $(pkgdatadir)


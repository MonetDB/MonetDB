TOPDIR = .
SRCDIR = $(TOPDIR)\..
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

prefix = $(MAKEDIR)

!INCLUDE "$(TOPDIR)\rules.msc"

UNISTD_H = unistd.h

all: "$(SRCDIR)\Makefile.msc" all-msc $(UNISTD_H) Runmclient.bat RunMtest.bat RunMapprove.bat
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" all

check: "$(SRCDIR)\Makefile.msc"
	$(MAKE) /nologo /i /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" check
	call RunMtest.bat

install: targetdirs clients_config.h all
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" install
	if not "$(MAKEDIR)" == "$(prefix)" $(INSTALL) mclient.bat "$(prefix)"

$(SRCDIR)\Makefile.msc: "$(SRCDIR)\Makefile.ag"
        $(CD) "$(SRCDIR)" && autogen.py

all-msc: clients_config.h

clients_config.h: clients_config.h.in
	$(CONFIGURE) clients_config.h.in > clients_config.h

unistd.h:
	$(ECHO) #ifndef UNISTD_H > unistd.h
	$(ECHO) #define UNISTD_H >> unistd.h
	$(ECHO) #include "io.h" >> unistd.h
	$(ECHO) #define open _open >> unistd.h
	$(ECHO) #define read _read >> unistd.h
	$(ECHO) #define write _write >> unistd.h
	$(ECHO) #define close _close >> unistd.h
	$(ECHO) #define getpid _getpid >> unistd.h
	$(ECHO) #define umask _umask >> unistd.h
	$(ECHO) #endif >> unistd.h

Runmclient.bat: "$(SRCDIR)\Runmclient.bat.in"
	$(CONFIGURE) "$(SRCDIR)\Runmclient.bat.in" > Runmclient.bat

RunMtest.bat: "$(SRCDIR)\RunMtest.bat.in"
        $(CONFIGURE) "$(SRCDIR)\RunMtest.bat.in" > RunMtest.bat

RunMapprove.bat: "$(SRCDIR)\RunMapprove.bat.in"
        $(CONFIGURE) "$(SRCDIR)\RunMapprove.bat.in" > RunMapprove.bat

targetdirs:
	if not exist "$(bindir)"		$(MKDIR) "$(bindir)"
	if not exist "$(sbindir)"		$(MKDIR) "$(sbindir)"
	if not exist "$(libexecdir)"		$(MKDIR) "$(libexecdir)"
	if not exist "$(datadir)"		$(MKDIR) "$(datadir)"
	if not exist "$(sysconfdir)"		$(MKDIR) "$(sysconfdir)"
	if not exist "$(sharedstatedir)"	$(MKDIR) "$(sharedstatedir)"
	if not exist "$(localstatedir)"		$(MKDIR) "$(localstatedir)"
	if not exist "$(libdir)"		$(MKDIR) "$(libdir)"
	if not exist "$(infodir)"		$(MKDIR) "$(infodir)"
	if not exist "$(mandir)"		$(MKDIR) "$(mandir)"
	if not exist "$(includedir)"		$(MKDIR) "$(includedir)"
	if not exist "$(pkgdatadir)"		$(MKDIR) "$(pkgdatadir)"
	if not exist "$(pkglibdir)"		$(MKDIR) "$(pkglibdir)"
	if not exist "$(pkgincludedir)"		$(MKDIR) "$(pkgincludedir)"

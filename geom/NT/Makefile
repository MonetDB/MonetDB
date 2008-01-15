TOPDIR = .
SRCDIR = $(TOPDIR)\..
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

prefix = $(MAKEDIR)

!INCLUDE "$(TOPDIR)\rules.msc"

UNISTD_H = unistd.h

all: "$(SRCDIR)\Makefile.msc" all-msc $(UNISTD_H) RunMserver.bat RunMtest.bat RunMapprove.bat
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" all

all-clear: "$(SRCDIR)\Makefile.msc-clear" all-msc-clear $(UNISTD_H)-clear RunMserver.bat-clear RunMtest.bat-clear RunMapprove.bat-clear
	if exist "src"  RMDIR /S /Q "src"
	if exist "conf" RMDIR /S /Q "conf"

clear-all: all-clear all

check: "$(SRCDIR)\Makefile.msc" RunMtest.bat
	$(MAKE) /nologo /i /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" check
	call RunMtest.bat

install: targetdirs geom_config.h all
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" install

$(SRCDIR)\Makefile.msc: "$(SRCDIR)\Makefile.ag"
	$(CD) "$(SRCDIR)" && autogen.py

$(SRCDIR)\Makefile.msc-clear:
	if exist "$(SRCDIR)\acout.in"			$(DEL) "$(SRCDIR)\acout.in"
	if exist "$(SRCDIR)\doc.lst"			$(DEL) "$(SRCDIR)\doc.lst"
	if exist "$(SRCDIR)\install.lst"		$(DEL) "$(SRCDIR)\install.lst"
	if exist "$(SRCDIR)\Makefile.am"		$(DEL) "$(SRCDIR)\Makefile.am"
	if exist "$(SRCDIR)\Makefile.msc"		$(DEL) "$(SRCDIR)\Makefile.msc"
	if exist "$(SRCDIR)\src\.incs.ag"		$(DEL) "$(SRCDIR)\src\.incs.ag"
	if exist "$(SRCDIR)\src\.incs.in"		$(DEL) "$(SRCDIR)\src\.incs.in"
	if exist "$(SRCDIR)\src\Makefile.am"	$(DEL) "$(SRCDIR)\src\Makefile.am"
	if exist "$(SRCDIR)\src\Makefile.msc"	$(DEL) "$(SRCDIR)\src\Makefile.msc"
	if exist "$(SRCDIR)\conf\Makefile.am"	$(DEL) "$(SRCDIR)\conf\Makefile.am"
	if exist "$(SRCDIR)\conf\Makefile.msc"	$(DEL) "$(SRCDIR)\conf\Makefile.msc"

all-msc: geom_config.h

all-msc-clear: geom_config.h-clear

geom_config.h: geom_config.h.in
	$(CONFIGURE) geom_config.h.in > geom_config.h

geom_config.h-clear:
	if exist "geom_config.h" $(DEL) "geom_config.h"

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

unistd.h-clear:
	if exist unistd.h $(DEL) unistd.h

RunMtest.bat: "$(SRCDIR)\RunMtest.bat.in"
	if exist "$(SRCDIR)\RunMtest.bat.in" $(CONFIGURE) "$(SRCDIR)\RunMtest.bat.in" > RunMtest.bat

RunMtest.bat-clear:
	if exist "RunMtest.bat" $(DEL) "RunMtest.bat"

RunMapprove.bat: "$(SRCDIR)\RunMapprove.bat.in"
	if exist "$(SRCDIR)\RunMapprove.bat.in" $(CONFIGURE) "$(SRCDIR)\RunMapprove.bat.in" > RunMapprove.bat

RunMapprove.bat-clear:
	if exist "RunMapprove.bat" $(DEL) "RunMapprove.bat"

RunMserver.bat: "$(SRCDIR)\RunMserver.bat.in"
	if exist "$(SRCDIR)\RunMserver.bat.in" $(CONFIGURE) "$(SRCDIR)\RunMserver.bat.in" > RunMserver.bat

RunMserver.bat-clear:
	if exist "RunMserver.bat" $(DEL) "RunMserver.bat"

targetdirs:
	if not exist "$(bindir)"			$(MKDIR) "$(bindir)"
	if not exist "$(sbindir)"			$(MKDIR) "$(sbindir)"
	if not exist "$(libexecdir)"		$(MKDIR) "$(libexecdir)"
	if not exist "$(datadir)"			$(MKDIR) "$(datadir)"
	if not exist "$(sysconfdir)"		$(MKDIR) "$(sysconfdir)"
	if not exist "$(sharedstatedir)"	$(MKDIR) "$(sharedstatedir)"
	if not exist "$(localstatedir)"		$(MKDIR) "$(localstatedir)"
	if not exist "$(libdir)"			$(MKDIR) "$(libdir)"
	if not exist "$(infodir)"			$(MKDIR) "$(infodir)"
	if not exist "$(mandir)"			$(MKDIR) "$(mandir)"
	if not exist "$(includedir)"		$(MKDIR) "$(includedir)"
	if not exist "$(pkgdatadir)"		$(MKDIR) "$(pkgdatadir)"
	if not exist "$(pkglibdir)"			$(MKDIR) "$(pkglibdir)"
	if not exist "$(pkglibdir)\Tests"	$(MKDIR) "$(pkglibdir)\Tests"
	if not exist "$(pkgincludedir)"		$(MKDIR) "$(pkgincludedir)"

docs:
	python "$(SRCDIR)\doc\mkdoc.py" "$(SRCDIR)" "$(prefix)"

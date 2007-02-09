
TOPDIR = .
SRCDIR = $(TOPDIR)\..
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

prefix = $(MAKEDIR)

!INCLUDE "$(TOPDIR)\rules.msc"

!IFDEF MONET5
RUNMSERVER5 = RunMserver5.bat
!ELSE
RUNMSERVER5 =
!ENDIF

all: "$(SRCDIR)\Makefile.msc" all-msc unistd.h RunMserver.bat RunMtest.bat RunMapprove.bat $(RUNMSERVER5)
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" all

check: "$(SRCDIR)\Makefile.msc" RunMtest.bat
	$(MAKE) /nologo /i /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" check
	call RunMtest.bat

install: targetdirs all
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" install

$(SRCDIR)\Makefile.msc:
	$(CD) "$(SRCDIR)" && autogen.py

all-msc: sql_config.h

sql_config.h: sql_config.h.in
	$(CONFIGURE) sql_config.h.in > sql_config.h

unistd.h: 
	$(ECHO) #ifndef UNISTD_H > unistd.h
	$(ECHO) #define UNISTD_H >> unistd.h
	$(ECHO) #include "io.h" >> unistd.h
	$(ECHO) #endif >> unistd.h

RunMtest.bat: "$(SRCDIR)\RunMtest.bat.in"
	$(CONFIGURE) "$(SRCDIR)\RunMtest.bat.in" > RunMtest.bat

RunMapprove.bat: "$(SRCDIR)\RunMapprove.bat.in"
	$(CONFIGURE) "$(SRCDIR)\RunMapprove.bat.in" > RunMapprove.bat

RunMserver.bat: "$(SRCDIR)\RunMserver.bat.in"
	$(CONFIGURE) "$(SRCDIR)\RunMserver.bat.in" > RunMserver.bat

RunMserver5.bat: "$(SRCDIR)\RunMserver5.bat.in"
	$(CONFIGURE) "$(SRCDIR)\RunMserver5.bat.in" > RunMserver5.bat

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

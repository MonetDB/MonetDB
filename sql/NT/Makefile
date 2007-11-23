
TOPDIR = .
SRCDIR = $(TOPDIR)\..
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

prefix = $(MAKEDIR)

!INCLUDE "$(TOPDIR)\rules.msc"

!IFDEF HAVE_MONETDB5
RUNMSERVER5 = RunMserver5.bat
M5INSTALLER = install-m5installer
!ELSE
RUNMSERVER5 =
M5INSTALLER =
!ENDIF

!IFDEF HAVE_MONETDB5
M4INSTALLER = install-m4installer
!ELSE
M4INSTALLER =
!ENDIF

all: "$(SRCDIR)\Makefile.msc" all-msc unistd.h RunMserver.bat RunMtest.bat RunMapprove.bat $(RUNMSERVER5)
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" all

check: "$(SRCDIR)\Makefile.msc" RunMtest.bat
	$(MAKE) /nologo /i /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" check
	call RunMtest.bat

install: targetdirs all $(M4INSTALLER) $(M5INSTALLER)
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" install
	if not "$(MAKEDIR)" == "$(prefix)" $(INSTALL) MSQLserver.bat "$(prefix)"

!IFDEF BITS32
install-m4installer:
	$(INSTALL) MonetDB4-SQL\MonetDB4-SQL-Installer.sln "$(prefix)"
	$(INSTALL) MonetDB4-SQL\MonetDB4-SQL-Installer.vdproj "$(prefix)"

install-m5installer:
	$(INSTALL) MonetDB5-SQL\MonetDB5-SQL-Installer.sln "$(prefix)"
	$(INSTALL) MonetDB5-SQL\MonetDB5-SQL-Installer.vdproj "$(prefix)"
!ENDIF

!IFDEF BITS64
install-m4installer:
	$(INSTALL) MonetDB4-SQL64\MonetDB4-SQL-Installer.sln "$(prefix)"
	$(INSTALL) MonetDB4-SQL64\MonetDB4-SQL-Installer.vdproj "$(prefix)"

install-m5installer:
	$(INSTALL) MonetDB5-SQL64\MonetDB5-SQL-Installer.sln "$(prefix)"
	$(INSTALL) MonetDB5-SQL64\MonetDB5-SQL-Installer.vdproj "$(prefix)"
!ENDIF

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

RunMserver5.bat: "$(SRCDIR)\RunMserver5.bat"
	$(INSTALL) "$(SRCDIR)\RunMserver5.bat" .

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

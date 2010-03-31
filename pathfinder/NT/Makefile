TOPDIR = .
SRCDIR = $(TOPDIR)\..
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

prefix = $(MAKEDIR)

!INCLUDE "$(TOPDIR)\..\NT\rules.msc"

INSTALLER = MonetDB4-XQuery
INSTALLER_PREREQ = "$(prefix)\banner.bmp" "$(prefix)\license.rtf" "$(prefix)\monetdb.ico"
!IF $(bits) == 32
IBITS =
!ELSE
IBITS = 64
!ENDIF

UNISTD_H = unistd.h

all: "$(SRCDIR)\Makefile.msc" all-msc $(UNISTD_H) inttypes.h RunMserver.bat RunMtest.bat RunMapprove.bat
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" all

check: "$(SRCDIR)\Makefile.msc" RunMtest.bat
	$(MAKE) /nologo /i /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" check
	call RunMtest.bat

!IFDEF BITS32
install-installer:
	$(INSTALL) ..\NT\MonetDB4-XQuery\MonetDB4-XQuery-Installer.sln "$(prefix)"
	$(INSTALL) ..\NT\MonetDB4-XQuery\MonetDB4-XQuery-Installer.vdproj "$(prefix)"
	-if exist "C:\Program Files (x86)" C:\cygwin\bin\sed.exe -i "s/Program Files/Program Files (x86)/" "$(prefix)\MonetDB4-XQuery-Installer.vdproj"
!ENDIF
!IFDEF BITS64
install-installer:
	$(INSTALL) ..\NT\MonetDB4-XQuery64\MonetDB4-XQuery-Installer.sln "$(prefix)"
	$(INSTALL) ..\NT\MonetDB4-XQuery64\MonetDB4-XQuery-Installer.vdproj "$(prefix)"
!ENDIF

install: targetdirs all install-installer $(INSTALLER_PREREQ)
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" install
	if not "$(MAKEDIR)" == "$(prefix)" $(INSTALL) ..\NT\MXQserver.bat "$(prefix)"
	if not "$(MAKEDIR)" == "$(prefix)" $(INSTALL) ..\NT\pf.bat "$(prefix)"
	$(INSTALL) ..\NT\$(INSTALLER)\$(INSTALLER)$(IBITS).sln "$(prefix)"
	$(INSTALL) ..\NT\$(INSTALLER)\$(INSTALLER)$(IBITS).vdproj "$(prefix)"
	-devenv "$(prefix)\$(INSTALLER)$(IBITS).sln" /build
	if exist "$(prefix)\$(INSTALLER)$(IBITS).msi" del /f "$(prefix)\$(INSTALLER)$(IBITS).msi"
	if exist "$(prefix)\Debug\$(INSTALLER)$(IBITS).msi" $(INSTALL) /Y "$(prefix)"\Debug\$(INSTALLER)$(IBITS).msi "$(prefix)"
	@-c:\cygwin\bin\chmod a+rx "$(prefix)\$(INSTALLER)$(IBITS).msi"

"$(prefix)\banner.bmp":
	$(INSTALL) "$(MONETDB_PREFIX)\banner.bmp" "$(prefix)\banner.bmp"
"$(prefix)\license.rtf":
	$(INSTALL) "$(MONETDB_PREFIX)\license.rtf" "$(prefix)\license.rtf"
"$(prefix)\monetdb.ico":
	$(INSTALL) "$(MONETDB_PREFIX)\monetdb.ico" "$(prefix)\monetdb.ico"

$(SRCDIR)\Makefile.msc: "$(SRCDIR)\Makefile.ag"
        $(CD) "$(SRCDIR)" && autogen.py

all-msc: pf_config.h 

pf_config.h: ..\NT\pf_config.h.in
	$(CONFIGURE) ..\NT\pf_config.h.in > pf_config.h

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

inttypes.h:
	$(ECHO) typedef signed char int8_t; > inttypes.h
	$(ECHO) typedef unsigned char uint8_t; >> inttypes.h
	$(ECHO) typedef short int16_t; >> inttypes.h
	$(ECHO) typedef unsigned short uint16_t; >> inttypes.h
	$(ECHO) typedef int int32_t; >> inttypes.h
	$(ECHO) typedef unsigned int uint32_t; >> inttypes.h

RunMserver.bat: "$(SRCDIR)\RunMserver.bat.in"
	$(CONFIGURE) "$(SRCDIR)\RunMserver.bat.in" > RunMserver.bat

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
	if not exist "$(pkglibdir)\Tests"	$(MKDIR) "$(pkglibdir)\Tests"
	if not exist "$(pkgincludedir)"		$(MKDIR) "$(pkgincludedir)"

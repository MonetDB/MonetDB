
TOPDIR = .
SRCDIR = $(TOPDIR)\..
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

prefix = $(MAKEDIR)

!INCLUDE "$(TOPDIR)\rules.msc"

INSTALLER = MonetDB5-SQL
INSTALLER_PREREQ = "$(prefix)\banner.bmp" "$(prefix)\license.rtf" "$(prefix)\monetdb.ico"
!IF $(bits) == 32
IBITS =
!ELSE
IBITS = 64
!ENDIF

all: "$(SRCDIR)\Makefile.msc" all-msc unistd.h inttypes.h RunMserver.bat RunMtest.bat RunMapprove.bat
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" all

check: "$(SRCDIR)\Makefile.msc" RunMtest.bat
	$(MAKE) /nologo /i /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" check
	call RunMtest.bat

install: targetdirs all install-m5installer $(INSTALLER_PREREQ)
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" install
	if not "$(MAKEDIR)" == "$(prefix)" $(INSTALL) MSQLserver.bat "$(prefix)"
	$(INSTALL) $(INSTALLER)\$(INSTALLER)$(IBITS).sln "$(prefix)"
	$(INSTALL) $(INSTALLER)\$(INSTALLER)$(IBITS).vdproj "$(prefix)"
	-if exist "C:\Program Files (x86)" if "$(bits)" == "32" C:\cygwin\bin\sed.exe -i "s/Program Files/Program Files (x86)/" "$(prefix)\$(INSTALLER)$(IBITS).vdproj"
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

!IFDEF BITS32
install-m5installer:
	$(INSTALL) MonetDB5-SQL\MonetDB5-SQL-Installer.sln "$(prefix)"
	$(INSTALL) MonetDB5-SQL\MonetDB5-SQL-Installer.vdproj "$(prefix)"
	-if exist "C:\Program Files (x86)" C:\cygwin\bin\sed.exe -i "s/Program Files/Program Files (x86)/" "$(prefix)\MonetDB5-SQL-Installer.vdproj"
!ENDIF

!IFDEF BITS64
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

inttypes.h:
	$(ECHO) typedef signed char int8_t; > inttypes.h
	$(ECHO) typedef unsigned char uint8_t; >> inttypes.h
	$(ECHO) typedef short int16_t; >> inttypes.h
	$(ECHO) typedef unsigned short uint16_t; >> inttypes.h
	$(ECHO) typedef int int32_t; >> inttypes.h
	$(ECHO) typedef unsigned int uint32_t; >> inttypes.h

RunMtest.bat: "$(SRCDIR)\RunMtest.bat.in"
	$(CONFIGURE) "$(SRCDIR)\RunMtest.bat.in" > RunMtest.bat

RunMapprove.bat: "$(SRCDIR)\RunMapprove.bat.in"
	$(CONFIGURE) "$(SRCDIR)\RunMapprove.bat.in" > RunMapprove.bat

RunMserver.bat: "$(SRCDIR)\RunMserver.bat.in"
	$(CONFIGURE) "$(SRCDIR)\RunMserver.bat.in" > RunMserver.bat

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

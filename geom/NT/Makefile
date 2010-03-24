TOPDIR = .
SRCDIR = $(TOPDIR)\..
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

prefix = $(MAKEDIR)

!INCLUDE "$(TOPDIR)\..\NT\rules.msc"

!IFDEF HAVE_MONETDB5
M5INSTALLER = install-m5installer
!ELSE
M5INSTALLER =
!ENDIF

all: "$(SRCDIR)\Makefile.msc" all-msc unistd.h inttypes.h RunMserver.bat RunMtest.bat RunMapprove.bat
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" all

check: "$(SRCDIR)\Makefile.msc" RunMtest.bat
	$(MAKE) /nologo /i /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" check
	call RunMtest.bat

install: targetdirs all $(M5INSTALLER)
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" install

!IFDEF BITS32
install-m5installer:
	$(INSTALL) ..\NT\MonetDB5-Geom\MonetDB5-Geom-Module.sln "$(prefix)"
	$(INSTALL) ..\NT\MonetDB5-Geom\MonetDB5-Geom-Module.vdproj "$(prefix)"
	-if exist "C:\Program Files (x86)" C:\cygwin\bin\sed.exe -i "s/Program Files/Program Files (x86)/" "$(prefix)\MonetDB5-Geom-Module.vdproj"
!ENDIF

!IFDEF BITS64
install-m5installer:
	$(INSTALL) ..\NT\MonetDB5-Geom64\MonetDB5-Geom-Module.sln "$(prefix)"
	$(INSTALL) ..\NT\MonetDB5-Geom64\MonetDB5-Geom-Module.vdproj "$(prefix)"
!ENDIF

$(SRCDIR)\Makefile.msc: "$(SRCDIR)\Makefile.ag"
	$(CD) "$(SRCDIR)" && autogen.py

all-msc: geom_config.h

geom_config.h: ..\NT\geom_config.h.in
	$(CONFIGURE) ..\NT\geom_config.h.in > geom_config.h

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

RunMtest.bat: "$(SRCDIR)\RunMtest.bat.in"
	if exist "$(SRCDIR)\RunMtest.bat.in" $(CONFIGURE) "$(SRCDIR)\RunMtest.bat.in" > RunMtest.bat

RunMapprove.bat: "$(SRCDIR)\RunMapprove.bat.in"
	if exist "$(SRCDIR)\RunMapprove.bat.in" $(CONFIGURE) "$(SRCDIR)\RunMapprove.bat.in" > RunMapprove.bat

RunMserver.bat: "$(SRCDIR)\RunMserver.bat.in"
	if exist "$(SRCDIR)\RunMserver.bat.in" $(CONFIGURE) "$(SRCDIR)\RunMserver.bat.in" > RunMserver.bat

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

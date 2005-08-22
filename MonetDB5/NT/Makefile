
TOPDIR = .
SRCDIR = $(TOPDIR)\..
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

prefix = $(MAKEDIR)

!INCLUDE "$(TOPDIR)\rules.msc"

all: "$(SRCDIR)\Makefile.msc" all-msc unistd.h RunMserver.bat
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" all

check: "$(SRCDIR)\Makefile.msc" RunMtest.bat
	$(MAKE) /nologo /i /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" check
	call RunMtest.bat

install: targetdirs all
	$(MAKE) /nologo /f "$(SRCDIR)\Makefile.msc" "prefix=$(prefix)" install

$(SRCDIR)\Makefile.msc: "$(MONETDB_PREFIX)\lib\autogen\autogen.py" \
	"$(MONETDB_PREFIX)\lib\autogen\codegen.py" \
	"$(MONETDB_PREFIX)\lib\autogen\msc.py"
	$(CD) "$(SRCDIR)" && "$(PYTHON)" "$(MONETDB_PREFIX)\lib\autogen\autogen.py"

all-msc: mal_config.h

mal_config.h: winconfig.h sysdefs.h
	$(INSTALL) winconfig.h mal_config.h

sysdefs.h: "$(SRCDIR)\sysdefs.h.in"
	$(CONFIGURE) "$(SRCDIR)\sysdefs.h.in" > sysdefs.h

RunMserver.bat: "$(SRCDIR)\RunMserver.bat.in"
	$(CONFIGURE) "$(SRCDIR)\RunMserver.bat.in" > RunMserver.bat

unistd.h: 
	$(ECHO) #ifndef UNISTD_H > unistd.h
	$(ECHO) #define UNISTD_H >> unistd.h
	$(ECHO) #include "io.h" >> unistd.h
	$(ECHO) #endif >> unistd.h

RunMtest.bat: "$(SRCDIR)\RunMtest.bat.in"
	if exist "$(SRCDIR)\RunMtest.bat.in" $(CONFIGURE) "$(SRCDIR)\RunMtest.bat.in" > RunMtest.bat

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

html:
	python "$(SRCDIR)\doc\mkdoc.py" "$(SRCDIR)" "$(prefix)"

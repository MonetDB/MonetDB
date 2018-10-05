# -*- makefile -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

TOPDIR = .
srcdir = $(TOPDIR)\..

# location of NT directory
NT = $(srcdir)\NT

prefix = $(MAKEDIR)

# ensure "all" is first target
all: _all

!INCLUDE "$(NT)\rules.msc"

_all: update_winconfig_conds_py "$(srcdir)\Makefile.msc" monetdb_config.h unistd.h .monetdb
	$(MAKE) /nologo /f "$(srcdir)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" all

install: targetdirs all
	$(MAKE) /nologo /f "$(srcdir)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" install
	$(INSTALL) .monetdb "$(sysconfdir)"
	$(PYTHON) $(NT)\mksqlwxs.py $(VERSION) "$(MAKE_INCLUDEFILE)" "$(prefix)" > "$(prefix)\MonetDB5-SQL-Installer.wxs"
	$(PYTHON) $(NT)\mkodbcwxs.py $(VERSION) "$(MAKE_INCLUDEFILE)" "$(prefix)" > "$(prefix)\MonetDB-ODBC-Installer.wxs"

$(srcdir)\Makefile.msc: "$(srcdir)\Makefile.ag"
        cd "$(srcdir)"&&set PYTHONPATH=buildtools\autogen&&$(PYTHON) buildtools\autogen\autogen.py

monetdb_config.h: "$(TOPDIR)\winconfig_conds.py" "$(NT)\monetdb_config.h.in"
	$(CONFIGURE) "$(NT)\monetdb_config.h.in" > monetdb_config.h

unistd.h:
	$(ECHO) #ifndef UNISTD_H > unistd.h
	$(ECHO) #define UNISTD_H >> unistd.h
	$(ECHO) #include "io.h" >> unistd.h
	$(ECHO) #ifndef __cplusplus >> unistd.h
	$(ECHO) #define open _open >> unistd.h
	$(ECHO) #define read _read >> unistd.h
	$(ECHO) #define write _write >> unistd.h
	$(ECHO) #define close _close >> unistd.h
	$(ECHO) #define getpid _getpid >> unistd.h
	$(ECHO) #define umask _umask >> unistd.h
	$(ECHO) #define isatty _isatty >> unistd.h
	$(ECHO) #endif >> unistd.h
	$(ECHO) #endif >> unistd.h

.monetdb:
	$(ECHO) user=monetdb> .monetdb
	$(ECHO) password=monetdb>> .monetdb

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


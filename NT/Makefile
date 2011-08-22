# -*- makefile -*-

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

TOPDIR = .
srcdir = $(TOPDIR)\..

prefix = $(MAKEDIR)

# ensure "all" is first target
all: _all

!INCLUDE "$(TOPDIR)\..\NT\rules.msc"

_all: update_winconfig_conds_py "$(srcdir)\Makefile.msc" monetdb_config.h unistd.h inttypes.h .monetdb
	$(MAKE) /nologo /f "$(srcdir)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" all

install: targetdirs all
	$(MAKE) /nologo /f "$(srcdir)\Makefile.msc" "prefix=$(prefix)" "bits=$(bits)" install
	$(INSTALL) .monetdb "$(sysconfdir)"
	$(INSTALL) ..\NT\installer$(bits)\*.sln "$(prefix)"
	$(INSTALL) ..\NT\installer$(bits)\*.vdproj "$(prefix)"
	-if exist "C:\Program Files (x86)" if $(bits)==32 cd "$(prefix)"&&C:\cygwin\bin\sed.exe -i "s/Program Files/Program Files (x86)/" *.vdproj
	if not exist "C:\Program Files (x86)\Microsoft Visual Studio 10.0" if not exist "C:\Program Files\Microsoft Visual Studio 10.0" cd "$(prefix)"&&C:\cygwin\bin\sed.exe -i "s/Format Version 11.00/Format Version 10.00/;s/Visual Studio 2010/Visual Studio 2008/;s/-vs10//;s/_VC100_CRT_x64/_VC90_CRT_x86_x64/;s/_VC100_/_VC90_/" *.vdproj *.sln

$(srcdir)\Makefile.msc: "$(srcdir)\Makefile.ag"
        cd "$(srcdir)"&&set PYTHONPATH=buildtools\autogen&&python buildtools\autogen\autogen.py

monetdb_config.h: "$(TOPDIR)\winconfig_conds.py" "$(srcdir)\NT\monetdb_config.h.in"
	$(CONFIGURE) "$(srcdir)\NT\monetdb_config.h.in" > monetdb_config.h

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
	$(ECHO) #endif >> unistd.h
	$(ECHO) #endif >> unistd.h

inttypes.h:
	$(ECHO) typedef signed char int8_t; > inttypes.h
	$(ECHO) typedef unsigned char uint8_t; >> inttypes.h
	$(ECHO) typedef short int16_t; >> inttypes.h
	$(ECHO) typedef unsigned short uint16_t; >> inttypes.h
	$(ECHO) typedef int int32_t; >> inttypes.h
	$(ECHO) typedef unsigned int uint32_t; >> inttypes.h

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


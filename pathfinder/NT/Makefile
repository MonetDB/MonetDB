#
# Copyright Notice:
# -----------------
# 
# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the ``Pathfinder'' system. The Initial
# Developer of the Original Code is the Database & Information
# Systems Group at the University of Konstanz, Germany. Portions
# created by U Konstanz are Copyright (C) 2000-2004 University
# of Konstanz. All Rights Reserved.
#
# Contributors:
#         Niels Nes <Niels.Nes@cwi.nl>
#

TOPDIR = .
SRCDIR = $(TOPDIR)\..
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

!INCLUDE $(TOPDIR)\rules.msc

all: $(SRCDIR)\Makefile.msc all-msc unistd.h
	$(MAKE) /nologo /f $(SRCDIR)\Makefile.msc all

check: $(SRCDIR)\Makefile.msc RunMtest.bat
	$(MAKE) /nologo /i /f $(SRCDIR)\Makefile.msc check
	call RunMtest.bat

install: targetdirs all
	$(MAKE) /nologo /f $(SRCDIR)\Makefile.msc install

$(SRCDIR)\Makefile.msc: $(MONET_PREFIX)\lib\autogen\autogen.py \
	$(MONET_PREFIX)\lib\autogen\codegen.py \
	$(MONET_PREFIX)\lib\autogen\msc.py
	$(CD) $(SRCDIR) && $(PYTHON) $(MONET_PREFIX)\lib\autogen\autogen.py

all-msc: pf_config.h

pf_config.h: winconfig.h
	$(INSTALL) winconfig.h pf_config.h

unistd.h: 
	$(ECHO) #ifndef UNISTD_H > unistd.h
	$(ECHO) #define UNISTD_H >> unistd.h
	$(ECHO) #include "io.h" >> unistd.h
	$(ECHO) #endif >> unistd.h

RunMtest.bat: $(SRCDIR)\RunMtest.bat.in
	if exist $(SRCDIR)\RunMtest.bat.in $(CONFIGURE) $(SRCDIR)\RunMtest.bat.in > RunMtest.bat

targetdirs:
	if not exist $(bindir)		$(MKDIR) $(bindir)
	if not exist $(sbindir)		$(MKDIR) $(sbindir)
	if not exist $(libexecdir)	$(MKDIR) $(libexecdir)
	if not exist $(datadir)		$(MKDIR) $(datadir)
	if not exist $(sysconfdir)	$(MKDIR) $(sysconfdir)
	if not exist $(sharedstatedir)	$(MKDIR) $(sharedstatedir)
	if not exist $(localstatedir)	$(MKDIR) $(localstatedir)
	if not exist $(libdir)		$(MKDIR) $(libdir)
	if not exist $(infodir)		$(MKDIR) $(infodir)
	if not exist $(mandir)		$(MKDIR) $(mandir)
	if not exist $(includedir)	$(MKDIR) $(includedir)
	if not exist $(pkgdatadir)	$(MKDIR) $(pkgdatadir)
	if not exist $(pkglibdir)	$(MKDIR) $(pkglibdir)
	if not exist $(pkgincludedir)	$(MKDIR) $(pkgincludedir)

html:
	python $(SRCDIR)\doc\mkdoc.py $(SRCDIR) $(TOPDIR)

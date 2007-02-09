## Please run automake on this file to produce a Makefile.in

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

PREFIX=@prefix@
SRCDIR=@srcdir@
DESTDIR=

default: all

@abs_builddir@/autogen:
	mkdir -p $@

@abs_builddir@/%.py:  $(SRCDIR)/%.py @abs_builddir@/autogen
	cp $< $@

all:	@abs_builddir@/setup.py \
	@abs_builddir@/autogen.py \
	@abs_builddir@/autogen/am.py \
	@abs_builddir@/autogen/msc.py \
	@abs_builddir@/autogen/var.py \
	@abs_builddir@/autogen/codegen.py \
	@abs_builddir@/autogen/__init__.py
	@PYTHON@ setup.py build

install: all
	@PYTHON@ setup.py $@ --prefix="$(DESTDIR)$(PREFIX)"

uninstall:
	rm -rf "$(DESTDIR)$(PREFIX)"/"`@PYTHON@ -c 'import distutils.sysconfig; print distutils.sysconfig.get_python_lib(0,0,"")'`"/autogen \
		"$(DESTDIR)$(PREFIX)"/bin/autogen.py

clean:
	@PYTHON@ setup.py $@

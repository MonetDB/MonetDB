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
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2010 MonetDB B.V.
# All Rights Reserved.

## Please run automake on this file to produce a Makefile.in

dist_pkgdata_DATA = rules.mk MonetDB.m4
dist_bin_SCRIPTS = Mdebootstrap Mbootstrap buildtools-config mchangelog Maddlog

MonetDB-release-1.0.tar.gz: MonetDB-release.spec MonetDB-release-1.0/monetdb.repo MonetDB-release-1.0/MonetDB-GPG-KEY
	tar czf MonetDB-release-1.0.tar.gz -C $(srcdir) MonetDB-release.spec MonetDB-release-1.0/monetdb.repo MonetDB-release-1.0/MonetDB-GPG-KEY

srpm: MonetDB-release-1.0.tar.gz
	mkdir -p rpmbuild/BUILD
	mkdir -p rpmbuild/BUILDROOT
	mkdir -p rpmbuild/RPMS
	mkdir -p rpmbuild/SOURCES
	mkdir -p rpmbuild/SPECS
	mkdir -p rpmbuild/SRPMS
	mkdir -p rpmbuild/tmp
	rpmbuild --macros "`rpmbuild --showrc | sed -n 's/macrofiles *: *//p'`:$(srcdir)/.rpmmacros" -ts MonetDB-release-1.0.tar.gz
	mv rpmbuild/SRPMS/*.src.rpm .
	rpm --addsign --define='_signature gpg' --define='_gpg_name 0x0583366F' *.src.rpm

rpm: MonetDB-release-1.0.tar.gz
	mkdir -p rpmbuild/BUILD
	mkdir -p rpmbuild/BUILDROOT
	mkdir -p rpmbuild/RPMS
	mkdir -p rpmbuild/SOURCES
	mkdir -p rpmbuild/SPECS
	mkdir -p rpmbuild/SRPMS
	mkdir -p rpmbuild/tmp
	rpmbuild --macros "`rpmbuild --showrc | sed -n 's/macrofiles *: *//p'`:$(srcdir)/.rpmmacros" -tb MonetDB-release-1.0.tar.gz
	mv rpmbuild/RPMS/noarch/*.noarch.rpm .
	rpm --addsign --define='_signature gpg' --define='_gpg_name 0x0583366F' *.noarch.rpm

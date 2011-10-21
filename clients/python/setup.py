#!/usr/bin/env python

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

from distutils.core import setup

setup(name='python-monetdb',
      version='11.5.7',
      description='Native MonetDB client Python API',
      long_description='''\
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators and SQL frontend.

This package contains the files needed to use MonetDB from a Python
program.
''',
      author='MonetDB BV',
      author_email='info@monetdb.org',
      url='http://www.monetdb.org/',
      packages=['monetdb', 'monetdb.sql'],
      download_url='http://dev.monetdb.org/downloads/sources/Aug2011-SP2/python-monetdb-11.5.7.tar.gz',
     )


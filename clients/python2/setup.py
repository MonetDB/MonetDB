#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import os
from distutils.core import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(name='python-monetdb',
    version='11.24.0',
    description='Native MonetDB client Python API',
    long_description=read('README.rst'),
    author='MonetDB BV',
    author_email='info@monetdb.org',
    url='http://www.monetdb.org/',
    packages=['monetdb', 'monetdb.sql'],
    download_url='<will be filled in before a release>/python2-monetdb-11.24.0.tar.gz',
    classifiers=[
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: Other/Proprietary License",
        "Programming Language :: Python :: 2",
    ]
)



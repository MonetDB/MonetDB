#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

from distutils.core import setup

setup(name = "autogen",
      version = "1.0",
      description = "MonetDB autogen script",
      author = "MonetDB Team, CWI",
      author_email = "info@monetdb.org",
      url = "https://www.monetdb.org/",
      maintainer = "Sjoerd Mullender",
      maintainer_email = "info@monetdb.org",
      license = "Mozilla Public License, v. 2.0",
      packages = ['autogen'],
      scripts = ['autogen.py'])

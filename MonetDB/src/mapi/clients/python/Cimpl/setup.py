#!/usr/bin/env python

# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
#
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
#
# The Original Code is the Monet Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2004 CWI.
# All Rights Reserved.
#
# Contributor(s):
#		Martin Kersten <Martin.Kersten@cwi.nl>
#		Peter Boncz <Peter.Boncz@cwi.nl>
#		Niels Nes <Niels.Nes@cwi.nl>
#		Stefan Manegold  <Stefan.Manegold@cwi.nl>

 
from distutils.core import setup, Extension
import os
 
setup(name="_MapiLib",
      version="0.1",
      description="Interface to MonetDB Mapi",
      author="Sjoerd Mullender",
      author_email="sjoerd@acm.org",
      maintainer="Sjoerd Mullender",
      maintainer_email="sjoerd@users.sourceforge.net",
      license="\"MonetDB license\"",
      platforms=["Unix", "Linux"],
      py_modules=['Mapi', 'MapiLib'],
      ext_modules=[Extension("_MapiLib", ['Mapi_wrap.c'],
                             define_macros = [('SWIG_COBJECT_TYPES', None)],
                             include_dirs = ['/ufs/sjoerd/Monet/include/MonetDB/C'],
                             libraries = ['Mapi'],
                             library_dirs = ['/ufs/sjoerd/Monet/lib'])])

#!/usr/bin/env python
 
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

#!/usr/bin/env python

from distutils.core import setup, Extension
import os

setup(name="_odbc",
      version="0.1",
      description="Interface to MonetDB ODBC",
      author="Sjoerd Mullender",
      author_email="sjoerd@acm.org",
      maintainer="Sjoerd Mullender",
      maintainer_email="sjoerd@users.sourceforge.net",
      license="\"MonetDB license\"",
      platforms=["Unix", "Linux"],
      py_modules=['odbc'],
      ext_modules=[Extension("_odbc", ['odbc_wrap.c'],
                             define_macros = [('SWIG_COBJECT_TYPES', None)],
                             libraries = ['MonetODBC'],
                             library_dirs = ['/ufs/sjoerd/Monet/lib'])])

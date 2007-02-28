How to Prepare a Release
========================

.. This document is written in reStructuredText (see
   http://docutils.sourceforge.net/ for more information).
   Use ``rst2html.py`` to convert this file to HTML.

This document gives step-by-step instructions on how to prepare a
release for the MonetDB suite of programs.

Well before the release
-----------------------

- Get all the bug fixes (and new features for a major release) in long
  before continuing with the rest of the steps.
- Regularly try building the release on Windows during this stage.
  This includes trying to create installers and trying out the
  installers.

Prepartion of a Major Release
-----------------------------

- Create a tag on the head of the current branch to identify the
  branch point (e.g. ``cvs tag MonetDB_1-16_root``).
- Create a new branch for the release on this tag (e.g. ``cvs tag -b
  MonetDB_1-16``).
- Set version numbers in the current branch (e.g. ``vertoo.py -m
  MonetDB set 1.17.1``)
- Create a tag on the head of the current branch for propagation
  purposes (e.g. ``cvs tag MonetDB_1-17-1``).
- Check out the new branch (e.g. ``cvs co -rMonetDB_1-16 MonetDB``).
- Set version numbers in the new stable branch (e.g. ``vertoo.py -m
  MonetDB set 1.16.0``).
- Change defaults in ``buildtools/conf/MonetDB.m4`` for release builds
  (``dft_strict=no``, ``dft_assert=no``, ``dft_optimi=yes``,
  ``dft_netcdf=no``).
- Create a tag on the head of the new branch for propagation purposes
  (e.g. ``cvs tag MonetDB_1-16-1``).

After this, it's **bug fixes only** on the new branch.

Preparation for a Minor Release
-------------------------------

- Propagate changes from the stable branch to the current branch.
- Set version numbers in the stable branch (e.g. ``vertoo.py -m
  MonetDB incr release``).  This should set the release to an even
  number.  (On MonetDB5 it's different: ``vertoo.py -m MonetDB5
  set-part build 1_2``.)
- Update the propagation synchronization tags on the stable branch for
  the files that were changed in the previous step (e.g. ``cvs tag -F
  MonetDB_1-16-1 vertoo.data``).

Releasing
---------

- Wait for the next overnight test to get the daily builds.
- Build Windows installers, making sure they are all compiled with the
  correct options:

  + MonetDB4/SQL using Visual Studio on 32-bit Windows (``nmake``
    options ``NEED_MX=1 HAVE_JAVA=1 HAVE_PYTHON=1 HAVE_PYTHON_SWIG=1
    HAVE_PHP=1 NDEBUG=1``);
  + MonetDB5/SQL using Visual Studio on 32-bit Windows (``nmake``
    options ``NEED_MX=1 HAVE_JAVA=1 HAVE_PYTHON=1 HAVE_PYTHON_SWIG=1
    HAVE_PHP=1 NDEBUG=1 NO_MONETDB4=1 HAVE_MONETDB5=1``);
  + MonetDB4/SQL using Visual Studio on 64-bit Windows (``nmake``
    options ``NEED_MX=1 HAVE_JAVA=1 HAVE_PYTHON=1 HAVE_PYTHON_SWIG=1
    NDEBUG=1``);
  + MonetDB5/SQL using Visual Studio on 64-bit Windows (``nmake``
    options ``NEED_MX=1 HAVE_JAVA=1 HAVE_PYTHON=1 HAVE_PYTHON_SWIG=1
    NDEBUG=1 NO_MONETDB4=1 HAVE_MONETDB5=1``);
  + MonetDB/ODBC using Visual Studio on 32-bit Windows;
  + MonetDB4/XQuery using MinGW on 32-bit Window (Cygwin).

- Copy the daily builds and Windows installers and upload to
  SourceForge using the ``releaseforge`` program.

Post Release
------------

- Set version numbers in the stable branch (e.g. ``vertoo.py -m
  MonetDB incr release``).  This should set the release to an odd
  number.  (On MonetDB5 it's different: ``vertoo.py -m MonetDB5
  set-part build 1_3``.)
- Update the propagation synchronization tags on the stable branch for
  the files that were changed in the previous step (e.g. ``cvs tag -F
  MonetDB_1-16-1 vertoo.data``).

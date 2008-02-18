How to Prepare a Release
========================

.. This document is written in reStructuredText (see
   http://docutils.sourceforge.net/ for more information).
   Use ``rst2html.py`` to convert this file to HTML.

This document gives step-by-step instructions on how to prepare a
release for the MonetDB suite of programs.

Release Etiquette
-----------------

- A release is a major time investment of several people.  Cooperation
  of all developers is essential to live up to our charter: to deliver
  state-of-the-art database technology to the community.
- In particular, a new release should not be worse than any previous one,
  i.e., all features and tests that did work before (and are still
  supported) must still work flawlessly with the new release.
- Strictly adhere to bug fixes and feature updates.  Nothing
  is a bug fix unless it is covered by a bug report.
- Remember that after every release there will be uncountable
  opportunities to get your favorite feature in.
- Refrain from any act that may complicate the process.

Well before the release
-----------------------

- Identify a release manager who has the authority to decide on the
  precise release content and date, rollback updates, block updates,
  etc.  i.e. anything that could put the release in danger.
- Prepare a draft release note to finalize the scope of the release
  and inform those affected/responsible for parts to fix errors and
  prepare/update documentation.
- Decide on and fix the desired version numbers for all packages involved in
  the release.
- All other developers leave the stable branch.
- Get all the bug fixes (and new features for a major release) in long
  before continuing with the rest of the steps.
- Announce freeze dates and planned release dates on the developers
  list.
- Regularly try building the release on Windows during this stage.
  This includes trying to create installers and trying out the
  installers.
- A major (and very time consuming) task of the release manager is to
  continuously remind the developers to indeed care about and fix their
  bugs. This task consists mainly of reading aloud both the nightly testing
  result emails and the daily TestWeb status to each involved developer
  personally.

Preparation of a Major Release
------------------------------

- Create a tag on the head of the current branch to identify the
  branch point (e.g. ``cvs tag MonetDB_1-16_root``).
- Create a new branch for the release on this tag (e.g. ``cvs tag -b
  MonetDB_1-16``).
- Set version numbers in the current branch (e.g. ``vertoo.py -m
  MonetDB set 1.17.0``)
- Create a tag on the head of the current branch for propagation
  purposes (e.g. ``cvs tag MonetDB_1-17_sync``).
- Check out the new branch (e.g. ``cvs co -rMonetDB_1-16 MonetDB``).
- Set version numbers in the new stable branch (e.g. ``vertoo.py -m
  MonetDB set 1.16.0``).
- Change defaults in ``buildtools/conf/MonetDB.m4`` for release builds
  (``dft_strict=no``, ``dft_assert=no``, ``dft_optimi=yes``,
  ``dft_netcdf=no``).
- Create a tag on the head of the new branch for propagation purposes
  (e.g. ``cvs tag MonetDB_1-16_sync``).
- Update the nightly testing setup to use the new branch for nightly
  testing.
- Where necessary, update the "TestWeb" pages of the MonetDB web site to
  point to the new branch's nighly testing results.

After this, it's **bug fixes only** on the new branch.

Preparation for a Minor Release
-------------------------------

- Propagate changes from the stable branch to the current branch.
- Set version numbers in the stable branch (e.g. ``vertoo.py -m
  MonetDB incr release``).  This should set the release to an even
  number.
- Update the propagation synchronization tags on the stable branch for
  the files that were changed in the previous step (e.g. ``cvs tag -F
  MonetDB_1-16_sync vertoo.data``).

Releasing
---------

- Wait for the next overnight test to get the daily builds.
- Create a tag on the tested version of the release branches to mark the
  released code base (e.g. ``cvs tag MonetDB_1-16-0``).
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
  + MonetDB4/XQuery using Intel C++ on 32- & 64-bit Windows.

- Copy the daily builds and Windows installers and upload to
  SourceForge using the ``releaseforge`` program.

Post Release
------------

- Set version numbers in the stable branch (e.g. ``vertoo.py -m
  MonetDB incr release``) and commit (``cvs commit``).  This should
  set the release to an odd number.
- Update the propagation synchronization tags on the stable branch for
  the files that were changed in the previous step (e.g. ``cvs tag -F
  MonetDB_1-16_sync vertoo.data``).


Post Minor Release
------------------

- Roll forward the MonetDB web to reflect the changes.
- Announce the availability of the minor release through the MonetDB
  mail channels.

Post Major Release
------------------

- Roll forward the MonetDB web to reflect the changes.
- Announce the availability of the release through the MonetDB mail
  channels.
- Announce the availability on http://www.dbworld.org/,
  http://www.freshmeat.net/, http://www.hollandopen.nl/,
  http://www.eosj.com/, http://www.freesoftwaremagazine.com/,
  http://www.eweek.com/, http://www.linuxworld.com/,
  http://www.pcmag.com/, http://www.heise.de/ct/,
  http://www.computable.nl/, http://www.dbforums.com/,
  news:comp.databases, Database Magazine (Array Publications), CWI
  announcement, CPI, slashdot, W3C.

.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

***************
MonetDB Release
***************

How to Prepare a Release
========================

.. This document is written in reStructuredText (see
   https://docutils.sourceforge.io/ for more information).
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

``Procedure``
=============

There are several differences between a normal build and a release build:
- The name of the release
- Th monetdb version

``Implementation``
==================

There are 3 different sets of versions:
- The version description
- The monetdb version number
- The monetb libraries version numbers

The version description is "unreleased", unless there is an actual release. Than it contains the name, for example "Nov2019-SP3". The monetdb version number is the version of the entire application, previously managed with vertoo. It contains three parts, a major, minor and release number. The release number is even during development and incremented to even for the actual release version.

``Building a release``
======================

When doing a release build, the only extra thing to do is to add the "-DRELEASE_VERSION=ON" parameter to the cmake command. This will make sure that the build will use the required version string and numbers. After building a successful release the final step is to tag the current version of the code in the release branch. Then you can start the next release by incrementing the "release" number of the monetdb version by 2. Or if necessary, create a new release branch.

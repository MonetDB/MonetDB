.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

.. This document is written in reStructuredText (see
   http://docutils.sourceforge.net/ for more information).
   Use ``rst2html.py`` to convert this file to HTML.

Building MonetDB On Windows
+++++++++++++++++++++++++++

In this document we describe how to build the MonetDB suite of
programs on Windows using the sources from `our source repository`__.
This document is mainly targeted at building on Windows on a 32-bit
architecture, but there are notes throughout about building on Windows
on a 64-bit architecture which is indicated with Windows64.  We have
successfully built on Windows XP, Windows Server, and Windows 7.

.. _MonetDB: https://dev.monetdb.org/hg/MonetDB/

__ MonetDB_

Introduction
============

The MonetDB suite of programs consists of a number of components which
we will describe briefly here.  The section names are the names of the
top-level folders in the Mercurial clone.

Note that in branches up to and including Oct2010 the build process
was different.  This document describes the build process for the
branch this document is part of.  Use the command ``hg branch`` to
find out the name of the branch.

buildtools
----------

The buildtools component contains tools that are used to build the
other components.  This component is required, but not all parts of
this component are required for all configurations.

common
------

Also known as the MonetDB Common component contains some generally
useful libraries.  This component is required.

gdk
---

Also known as the Goblin Database Kernel contains the database kernel,
i.e. the heart of MonetDB.  This component is required.

clients
-------

Also known as the MonetDB Client component contains a library which
forms the basis for communicating with the MonetDB server components,
and some interface programs that use this library to communicate with
the server.  This component is required.

monetdb5
--------

The MonetDB5 Server component is the database server.  It uses MAL
(the MonetDB Algebra Language) as programming interface.  This
component is required.

sql
---

Also known as MonetDB SQL, this component provides an SQL frontend to
MonetDB5.  This component is required if you need SQL support.

tools
-----

The tools component contains two parts.  The mserver part is the
actual database server binary and is required.  The merovingian part
is not used on Windows.

geom
----

The geom component provides a module for the MonetDB SQL frontend.
This component is optional.

testing
-------

The testing component contains some files and programs we use for
testing the MonetDB suite.  This component is optional.

Prerequisites
=============

In order to compile the MonetDB suite of programs, several other
programs and libraries need to be installed.  Some further programs
and libraries can be optionally installed to enable optional features.
The required programs and libraries are listed in this section, the
following section lists the optional programs and libraries.

Chocolatey
----------

Although Chocolatey_ is not a prerequisite per se, it makes
installing and maintaining some of the other prerequisites a lot
easier.  Therefore we recommend installing chocolatey.  Instructions
are on their website__.

We have installed the following programs using Chocolatey_::

  choco install ActivePerl ant cmake ruby
  choco install python3 python3-x86_32

.. _Chocolatey: https://chocolatey.org/

__ Chocolatey_


#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
#]]

set(MONETDB_VERSION_MAJOR "11")
set(MONETDB_VERSION_MINOR "54")
set(MONETDB_VERSION_PATCH "0")

if(RELEASE_VERSION)
  set(MONETDB_RELEASE "unreleased")
endif()
set(MONETDB_VERSION "${MONETDB_VERSION_MAJOR}.${MONETDB_VERSION_MINOR}.${MONETDB_VERSION_PATCH}")

# Version numbers for the shared libraries that we provide.
# The scheme used here comes from libtool but is also usable in the
# current context.
# The libtool scheme uses values <CURRENT>:<REVISION>:<AGE>, but we
# have renamed them here to <MAJOR>, <PATCH>, <MINOR> where <MAJOR> is
# libtool's <CURRENT> - <REVISION>.

# These numbers must be changed according to the following rules:

# X   if there are no code changes, don't change version numbers;
# FIX if there are changes to the code (bug fixes) but no API changes,
#     just increment PATCH;
# NEW if there are only backward compatible API changes (new
#     interfaces), increment MINOR, and set PATCH to 0;
# BRK if there are also incompatible API changes (interfaces removed or
#     changed), increment MAJOR, and set MINOR and PATCH to 0.
#     IMPORTANT: also change debian package names for the relevant
#     library.  This involves renaming the file in debian/ and
#     updating references to the package in debian/control.  The version
#     number should match the ELF SONAME

# version of the GDK library (subdirectory gdk; also includes
# common/options and common/utils)
set(GDK_VERSION_MAJOR "30")
set(GDK_VERSION_MINOR "1")
set(GDK_VERSION_PATCH "1")
set(GDK_VERSION "${GDK_VERSION_MAJOR}.${GDK_VERSION_MINOR}.${GDK_VERSION_PATCH}")

# version of the MAPI library (subdirectory clients/mapilib)
set(MAPI_VERSION_MAJOR "28")
set(MAPI_VERSION_MINOR "0")
set(MAPI_VERSION_PATCH "1")
set(MAPI_VERSION "${MAPI_VERSION_MAJOR}.${MAPI_VERSION_MINOR}.${MAPI_VERSION_PATCH}")

# version of the MONETDB5 library (subdirectory monetdb5, not including
# extras, and tools/utils/msabaoth.[ch])
set(MONETDB5_VERSION_MAJOR "37")
set(MONETDB5_VERSION_MINOR "0")
set(MONETDB5_VERSION_PATCH "2")
set(MONETDB5_VERSION "${MONETDB5_VERSION_MAJOR}.${MONETDB5_VERSION_MINOR}.${MONETDB5_VERSION_PATCH}")

# version of the MONETDBE library (subdirectory tools/monetdbe)
set(MONETDBE_VERSION_MAJOR "27")
set(MONETDBE_VERSION_MINOR "0")
set(MONETDBE_VERSION_PATCH "1")
set(MONETDBE_VERSION "${MONETDBE_VERSION_MAJOR}.${MONETDBE_VERSION_MINOR}.${MONETDBE_VERSION_PATCH}")

# version of the MUTILS library (subdirectory common/utils)
set(MUTILS_VERSION_MAJOR "1")
set(MUTILS_VERSION_MINOR "0")
set(MUTILS_VERSION_PATCH "1")
set(MUTILS_VERSION "${MUTILS_VERSION_MAJOR}.${MUTILS_VERSION_MINOR}.${MUTILS_VERSION_PATCH}")

# version of the SQL library (subdirectory sql)
set(SQL_VERSION_MAJOR "16")
set(SQL_VERSION_MINOR "1")
set(SQL_VERSION_PATCH "1")
set(SQL_VERSION "${SQL_VERSION_MAJOR}.${SQL_VERSION_MINOR}.${SQL_VERSION_PATCH}")

# version of the STREAM library (subdirectory common/stream)
set(STREAM_VERSION_MAJOR "28")
set(STREAM_VERSION_MINOR "0")
set(STREAM_VERSION_PATCH "1")
set(STREAM_VERSION "${STREAM_VERSION_MAJOR}.${STREAM_VERSION_MINOR}.${STREAM_VERSION_PATCH}")

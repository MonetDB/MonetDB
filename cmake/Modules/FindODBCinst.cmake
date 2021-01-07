#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

include(FindPackageHandleStandardArgs)

if(WIN32)
  set(ODBCINST_INCLUDE_DIR "")
  set(ODBCINST_LIBRARIES odbccp32.lib legacy_stdio_definitions.lib)

  # ODBCINST_INCLUDE_DIR is empty, so don't require it
  find_package_handle_standard_args(ODBCinst DEFAULT_MSG
    ODBCINST_LIBRARIES)
else()
  find_package(PkgConfig QUIET)
  pkg_check_modules(PC_ODBCINST QUIET odbcinst)

  find_path(ODBCINST_INCLUDE_DIR NAMES odbcinst.h
    HINTS
    ${PC_ODBCINST_INCLUDEDIR}
    ${PC_ODBCINST_INCLUDE_DIRS}
    )

  find_library(ODBCINST_LIBRARIES NAMES odbcinst
    HINTS
    ${PC_ODBCINST_LIBDIR}
    ${PC_ODBCINST_LIBRARY_DIRS}
    )

  find_package_handle_standard_args(ODBCinst DEFAULT_MSG
    ODBCINST_LIBRARIES ODBCINST_INCLUDE_DIR)
endif()

mark_as_advanced(ODBCINST_INCLUDE_DIR ODBCINST_LIBRARIES)

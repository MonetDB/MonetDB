#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

# Adapted from https://github.com/LuaDist/pcre/blob/master/FindPCRE.cmake
# Copyright (C) 2007-2009 LuaDist.
# Created by Peter Kapec <kapecp@gmail.com>
# Redistribution and use of this file is allowed according to the terms of the MIT license.
# For details see the COPYRIGHT file distributed with LuaDist.

# - Find pcre
# Find the native PCRE headers and libraries.
#
# PCRE_INCLUDE_DIR	- where to find pcre.h, etc.
# PCRE_LIBRARIES	- List of libraries when using pcre.
# PCRE_VERSION	- The version found.
# PCRE_FOUND	- True if pcre found.

find_path(PCRE_INCLUDE_DIR NAMES pcre.h)

find_library(PCRE_LIBRARIES NAMES pcre)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PCRE
  DEFAULT_MSG
  PCRE_LIBRARIES
  PCRE_INCLUDE_DIR)

mark_as_advanced(PCRE_INCLUDE_DIR PCRE_LIBRARIES PCRE_VERSION)

if(PCRE_FOUND)
  file(STRINGS "${PCRE_INCLUDE_DIR}/pcre.h" PCRE_VERSION_LINES REGEX "[ \t]*#define[ \t]+PCRE_(MAJOR|MINOR)")
  string(REGEX REPLACE ".*PCRE_MAJOR *\([0-9]*\).*" "\\1" PCRE_VERSION_MAJOR "${PCRE_VERSION_LINES}")
  string(REGEX REPLACE ".*PCRE_MINOR *\([0-9]*\).*" "\\1" PCRE_VERSION_MINOR "${PCRE_VERSION_LINES}")
  set(PCRE_VERSION "${PCRE_VERSION_MAJOR}.${PCRE_VERSION_MINOR}")

  if(NOT TARGET PCRE::PCRE AND
      (EXISTS "${PCRE_LIBRARIES}"))
    add_library(PCRE::PCRE UNKNOWN IMPORTED)
    set_target_properties(PCRE::PCRE PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${PCRE_INCLUDE_DIR}")

    if(EXISTS "${PCRE_LIBRARIES}")
      set_target_properties(PCRE::PCRE PROPERTIES
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
        IMPORTED_LOCATION "${PCRE_LIBRARIES}")
    endif()
  endif()
endif()

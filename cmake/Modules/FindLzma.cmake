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

find_package(LibLZMA)

if (LIBLZMA_FOUND)
  if(NOT TARGET LibLZMA::LibLZMA)
    add_library(LibLZMA::LibLZMA UNKNOWN IMPORTED)
    set_target_properties(LibLZMA::LibLZMA PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES ${LIBLZMA_INCLUDE_DIR}
      IMPORTED_LINK_INTERFACE_LANGUAGES C
      IMPORTED_LOCATION ${LIBLZMA_LIBRARY})
  endif()
endif ()

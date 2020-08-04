#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

if(GEOS_FOUND)
  add_library(geomlib STATIC)

  target_sources(geomlib
    PRIVATE
    libgeom.c
    libgeom.h)

  target_include_directories(geomlib
    PUBLIC
    $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(geomlib
    PRIVATE
    mutils
    stream
    bat
    Geos::Geos
    $<$<BOOL:${PROJ_FOUND}>:PROJ::PROJ>)

  set_target_properties(geomlib
    PROPERTIES
    POSITION_INDEPENDENT_CODE ON)

  target_compile_definitions(geomlib PRIVATE LIBGEOM)
endif()

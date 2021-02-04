#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

cmake_minimum_required(VERSION 3.12)

set(CMAKE_MODULE_PATH
  "${CMAKE_SOURCE_DIR}/cmake"
  "${CMAKE_SOURCE_DIR}/cmake/Modules"
  CACHE
  INTERNAL
  "Location of custom CMake modules.")

include(CMakeDependentOption REQUIRED)

include(monetdb-options)
include(monetdb-versions)
project(MonetDB VERSION "${MONETDB_VERSION}" LANGUAGES C)
set(CMAKE_PROJECT_HOMEPAGE_URL "https://www.monetdb.org/")
set(C_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 99)

if(${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.13.0")
  cmake_policy(SET CMP0076 OLD)
endif()

# We give precedence to UNIX include directories over
# OS X Frameworks directories
set(CMAKE_FIND_FRAMEWORK LAST)
set(CMAKE_FIND_APPBUNDLE LAST)

include(CheckCSourceCompiles REQUIRED)
include(CheckCCompilerFlag REQUIRED)
include(CheckIncludeFile REQUIRED)
include(CMakePushCheckState REQUIRED)
include(CheckStructHasMember REQUIRED)
include(CheckSymbolExists REQUIRED)
include(CheckFunctionExists REQUIRED)
include(CheckTypeSize REQUIRED)
include(TestBigEndian REQUIRED)

if(WIN32)
  set(CMAKE_REQUIRED_LIBRARIES  ws2_32.lib shell32.lib advapi32.lib)
endif()
test_big_endian(IS_BIG_ENDIAN)

# Include MonetDB specific functions
include(monetdb-functions)
include(monetdb-findpackages)
include(monetdb-toolchain)
monetdb_default_toolchain()
#monetdb_default_compiler_options()

include(monetdb-defines)
monetdb_hg_revision()

# required for symbol existance tests
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  add_definitions(-D_XOPEN_SOURCE -D_GNU_SOURCE)
  set(CMAKE_REQUIRED_DEFINITIONS "${CMAKE_REQUIRED_DEFINITIONS} -D_GNU_SOURCE")
endif()

monetdb_configure_defines()

if(HAVE_NETDB_H)
  set(CMAKE_EXTRA_INCLUDE_FILES "netdb.h" "unistd.h")
endif()
if(NOT HAVE_SYS_SOCKET_H)
  if(HAVE_WINSOCK_H)
    set(CMAKE_EXTRA_INCLUDE_FILES "winsock.h")
  endif()
endif()
set(CMAKE_REQUIRED_INCLUDES "/usr/include")

monetdb_configure_crypto()
monetdb_configure_sizes()

include(GNUInstallDirs)
# The documentation is unclear on what is the best location to install
# the configuration for the exported targets, especially when building
# the packages (deb/rpm). For now set the variable to a reasonable value
set(EXPORT_TARGET_PATH "${CMAKE_INSTALL_DATAROOTDIR}/monetdb/cmake")

# Define the macro variables after configuring the install directories.
# Some variables define additional installation settings.
monetdb_macro_variables()
monetdb_configure_misc()
# Define custom target after the install directories variables.
# Some custom target will need these setting.
include(monetdb-custom-targets)

SET(CMAKE_SKIP_BUILD_RPATH  FALSE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_FULL_LIBDIR};${CMAKE_INSTALL_FULL_LIBDIR}/monetdb5")
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# required for some instalation files
set(PROGRAM_PERMISSIONS_DEFAULT
  OWNER_WRITE
  OWNER_READ
  OWNER_EXECUTE
  GROUP_READ
  GROUP_EXECUTE
  WORLD_READ
  WORLD_EXECUTE)

find_program(BASH NAMES bash DOC "bash interpreter fullpath")

configure_file(monetdb_config.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/monetdb_config.h)
if(WIN32)
  configure_file(unistd.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/unistd.h)
endif()

add_library(monetdb_config_header
  INTERFACE)

target_include_directories(monetdb_config_header
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

install(TARGETS
  monetdb_config_header
  EXPORT monetdb_config_headerTargets
  COMPONENT monetdbdev)

install(EXPORT monetdb_config_headerTargets
  FILE monetdb_config_headerTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT monetdbdev)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/monetdb_config.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT monetdbdev)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  set(CMAKE_MODULE_LINKER_FLAGS "-Wl,-undefined,dynamic_lookup -Wl,-undefined,dynamic_lookup")
endif()

add_subdirectory(common)
add_subdirectory(clients)
add_subdirectory(gdk)
add_subdirectory(misc)
add_subdirectory(monetdb5)
add_subdirectory(sql)
add_subdirectory(geom)
add_subdirectory(tools)
if(TESTING)
  add_subdirectory(testing)
endif()
add_subdirectory(buildtools)

include(CMakePackageConfigHelpers)
configure_package_config_file(${CMAKE_CURRENT_SOURCE_DIR}/Config.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/MonetDBConfig.cmake"
  INSTALL_DESTINATION "${EXPORT_TARGET_PATH}"
  NO_SET_AND_CHECK_MACRO
  NO_CHECK_REQUIRED_COMPONENTS_MACRO)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/MonetDBConfigVersion.cmake"
  VERSION "${MONETDB_VERSION}"
  COMPATIBILITY AnyNewerVersion)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/MonetDBConfig.cmake
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT monetdbdev)

include(monetdb-packages)

include(CTest)
add_subdirectory(ctest)
add_subdirectory(documentation)

if(CMAKE_SUMMARY)
  monetdb_cmake_summary()
endif()


# vim: set ts=2:sw=2:et

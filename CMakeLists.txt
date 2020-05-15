#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
include(CheckIncludeFile REQUIRED)
include(CMakePushCheckState REQUIRED)
include(CheckStructHasMember REQUIRED)
include(CheckSymbolExists REQUIRED)
include(CheckFunctionExists REQUIRED)
include(CheckTypeSize REQUIRED)
include(TestBigEndian REQUIRED)

# Include MonetDB specific functions
if(WIN32)
  set(CMAKE_REQUIRED_LIBRARIES  ws2_32.lib shell32.lib advapi32.lib)
endif()
include(monetdb-functions)
include(monetdb-findpackages)
include(monetdb-defines)
monetdb_hg_revision()

# required for symbol existance tests
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  add_definitions(-D_XOPEN_SOURCE -D_GNU_SOURCE)
  set(CMAKE_REQUIRED_DEFINITIONS "${CMAKE_REQUIRED_DEFINITIONS} -D_GNU_SOURCE")
endif()

monetdb_configure_defines()

# On C99, but we have to calculate the size
check_type_size(size_t SIZEOF_SIZE_T LANGUAGE C)
set(SIZEOF_VOID_P ${CMAKE_SIZEOF_VOID_P})
if(HAVE_NETDB_H)
  set(CMAKE_EXTRA_INCLUDE_FILES "netdb.h" "unistd.h")
endif()
if(HAVE_WINSOCK_H)
  set(CMAKE_EXTRA_INCLUDE_FILES "winsock.h")
endif()
check_type_size(socklen_t HAVE_SOCKLEN_T LANGUAGE C)

set(CMAKE_REQUIRED_INCLUDES "/usr/include")

monetdb_configure_crypto()

check_type_size(ssize_t SIZEOF_SSIZE_T LANGUAGE C)
if(NOT HAVE_SIZEOF_SSIZE_T)
  # Set a default value
  if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(ssize_t "int64_t")
  else()
    set(ssize_t "int32_t")
  endif()
  set(SIZEOF_SSIZE_T ${CMAKE_SIZEOF_VOID_P})
endif()
check_type_size(char SIZEOF_CHAR LANGUAGE C)
check_type_size(short SIZEOF_SHORT LANGUAGE C)
check_type_size(int SIZEOF_INT LANGUAGE C)
check_type_size(long SIZEOF_LONG LANGUAGE C)
check_type_size(wchar_t SIZEOF_WCHAR_T LANGUAGE C)

if(MSVC)
  set(_Noreturn "__declspec(noreturn)")
  # C99 feature not present in MSVC
  set(restrict "__restrict")
  # C99 feature only available on C++ compiler in MSVC
  # https://docs.microsoft.com/en-us/cpp/cpp/inline-functions-cpp?view=vs-2015
  set(inline "__inline")
endif()

include(GNUInstallDirs)

monetdb_macro_variables()
# Define custom target after the install directories variables.
# Some custom target will need these setting.
include(monetdb-custom-targets)

# Used for installing testing python module
execute_process(COMMAND "${Python3_EXECUTABLE}" "-c" "import distutils.sysconfig; print(distutils.sysconfig.get_python_lib(0,0,'${MONETDB_PREFIX}'))"
		RESULT_VARIABLE PY3_LIBDIR_CODE 
		OUTPUT_VARIABLE PYTHON3_SITEDIR 
		OUTPUT_STRIP_TRAILING_WHITESPACE)
if (PY3_LIBDIR_CODE) 
  message(WARNING "Could not determine MonetDB Python3 site-packages instalation directory")
endif()
set(PYTHON3_LIBDIR "${PYTHON3_SITEDIR}")
set(PYTHON "${Python3_EXECUTABLE}")

SET(CMAKE_SKIP_BUILD_RPATH  FALSE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_FULL_LIBDIR}")
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

# Set host information
string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" CMAKE_SYSTEM_PROCESSOR_LOWER)
string(TOLOWER "${CMAKE_SYSTEM_NAME}" CMAKE_SYSTEM_NAME_LOWER)
string(TOLOWER "${CMAKE_C_COMPILER_ID}" CMAKE_C_COMPILER_ID_LOWER)
set("HOST" "${CMAKE_SYSTEM_PROCESSOR_LOWER}-pc-${CMAKE_SYSTEM_NAME_LOWER}-${CMAKE_C_COMPILER_ID_LOWER}")

# Password hash algorithm
set(PASSWORD_BACKEND "SHA512"
  CACHE STRING
  "Password hash algorithm, one of MD5, SHA1, RIPEMD160, SHA224, SHA256, SHA384, SHA512, defaults to SHA512")

if(NOT ${PASSWORD_BACKEND} MATCHES "^MD5|SHA1|RIPEMD160|SHA224|SHA256|SHA384|SHA512$")
  message(FATAL_ERROR "PASSWORD_BACKEND invalid, choose one of MD5, SHA1, RIPEMD160, SHA224, SHA256, SHA384, SHA512")
endif()

find_program(BASH NAMES bash DOC "bash interpreter fullpath")

if(WIN32)
  configure_file(monetdb_config_win32.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb_config.h)
  configure_file(unistd.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/unistd.h)
else()
  configure_file(monetdb_config.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb_config.h)
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
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/cmake
  COMPONENT monetdbdev)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/monetdb_config.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT monetdbdev)

# [TODO]: install created python file
configure_file(create_include_object.py.in
  ${CMAKE_CURRENT_BINARY_DIR}/create_include_object.py
  @ONLY)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  set(CMAKE_MODULE_LINKER_FLAGS "-Wl,-undefined,dynamic_lookup -Wl,-undefined,dynamic_lookup")
endif()

add_subdirectory(common)
add_subdirectory(clients)
add_subdirectory(gdk)
add_subdirectory(monetdb5)
add_subdirectory(sql)
add_subdirectory(geom)
add_subdirectory(tools)
if(TESTING)
  add_subdirectory(testing)
endif()

include(CMakePackageConfigHelpers)
configure_package_config_file(${CMAKE_CURRENT_SOURCE_DIR}/Config.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/MonetDBConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/cmake)"
  NO_SET_AND_CHECK_MACRO
  NO_CHECK_REQUIRED_COMPONENTS_MACRO)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/MonetDBConfigVersion.cmake"
  VERSION "${MONETDB_VERSION}"
  COMPATIBILITY AnyNewerVersion)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/MonetDBConfig.cmake
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/cmake)

include(monetdb-packages)

include(CTest)
add_subdirectory(ctest)

if(CMAKE_SUMMARY)
  monetdb_cmake_summary()
endif()

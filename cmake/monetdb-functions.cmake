#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

function(monetdb_hg_revision)
  # Get the current version control revision
  if(EXISTS "${CMAKE_SOURCE_DIR}/.hg")
    find_package(Hg)
    if(HG_FOUND)
      message("hg found: ${HG_EXECUTABLE}")
    else()
      message(FATAL_ERROR "Failed to find mercurial")
    endif()
    execute_process(COMMAND "${HG_EXECUTABLE}" "id" "-i" WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}" RESULT_VARIABLE HG_RETURN_CODE
      OUTPUT_VARIABLE HG_OUPUT_RES OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(HG_RETURN_CODE EQUAL 0 AND HG_OUPUT_RES)
      set(MERCURIAL_ID "${HG_OUPUT_RES}" PARENT_SCOPE)
    else()
      message(FATAL_ERROR "Failed to find mercurial ID")
    endif()
  elseif(EXISTS "${CMAKE_SOURCE_DIR}/.git")
    find_package(Git)
    if(GIT_FOUND)
      message("git found: ${GIT_EXECUTABLE}")
    else()
      message(FATAL_ERROR "Failed to find git")
    endif()
    execute_process(COMMAND "${GIT_EXECUTABLE}" "rev-parse" "--short" "HEAD" WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
      RESULT_VARIABLE GIT_RETURN_CODE OUTPUT_VARIABLE GIT_OUPUT_RES OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(GIT_RETURN_CODE EQUAL 0 AND GIT_OUPUT_RES)
      set(MERCURIAL_ID "${GIT_OUPUT_RES}" PARENT_SCOPE)
    else()
      message(FATAL_ERROR "Failed to find git ID")
    endif()
  else()
    set(MERCURIAL_ID "Unknown" PARENT_SCOPE)
  endif()
endfunction()

function(MT_addCompilerFlag REGEX_EXPRESSION FLAG_TO_ADD OLD_FLAGS BUILD NEW_FLAGS)
  if(NOT "${OLD_FLAGS}" MATCHES "${REGEX_EXPRESSION}")
    set(${NEW_FLAGS} "${OLD_FLAGS} ${FLAG_TO_ADD}" PARENT_SCOPE)
    message(STATUS "Added ${FLAG_TO_ADD} flag for ${BUILD} build(s)")
  endif()
endfunction()

function(MT_checkCompilerFlag Flag)
  # WARNING this doesn't work with LINK_OPTIONS yet!
  if(NOT "${CMAKE_C_FLAGS}" MATCHES "${Flag}")
    # cmake will get confused if we don't remove - / = ,
    string(REGEX REPLACE "[-/=,]" "" FLAG_TEST "${Flag}")
    cmake_push_check_state()
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${Flag}")
    check_c_source_compiles("int main(int argc,char** argv){(void)argc;(void)argv;return 0;}" COMPILER_${FLAG_TEST})
    cmake_pop_check_state()
    if(COMPILER_${FLAG_TEST})
      set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${Flag}" PARENT_SCOPE)
      message(STATUS "Added ${Flag} flag for all build(s)")
    endif()
  endif()
endfunction()

function(add_option_if_available Flag)
  string(REGEX REPLACE "[-/=,]" "" FLAG_TEST "${Flag}")
  check_c_compiler_flag(${Flag} ${FLAG_TEST}_FOUND)
  set(ISFOUND "${${FLAG_TEST}_FOUND}}")
  if(ISFOUND)
    add_compile_options("${Flag}")
  endif()
endfunction()

function(create_include_object)
  cmake_parse_arguments(
    create_prefix
    "MAL_LANGUAGE;SQL_LANGUAGE"
    "name_module;path"
    "modules_list"
    ${ARGN})
  if((NOT create_prefix_MAL_LANGUAGE) AND
      (NOT create_prefix_SQL_LANGUAGE))
      message(FATAL_ERROR
        "Please set language for ${create_prefix_name_module}_include_object target")
  endif()
  if(create_prefix_MAL_LANGUAGE AND create_prefix_SQL_LANGUAGE)
      message(FATAL_ERROR
        "Please set only one language for ${create_prefix_name_module}_include_object target")
  endif()
  if(create_prefix_MAL_LANGUAGE)
    set(lang_ext "mal")
  endif()
  if(create_prefix_SQL_LANGUAGE)
    set(lang_ext "sql")
  endif()
  if(create_prefix_path)
    set(file_path ${create_prefix_path})
  else()
    set(file_path ${CMAKE_CURRENT_SOURCE_DIR})
  endif()
  set(include_sources "")
  foreach(mod_name IN LISTS create_prefix_modules_list)
    add_custom_command(
      OUTPUT  ${CMAKE_CURRENT_BINARY_DIR}/${mod_name}.${lang_ext}.c
      COMMAND ${Python3_EXECUTABLE} ${PROJECT_BINARY_DIR}/misc/python/create_include_object.py ${mod_name} ${lang_ext} ${file_path}/${mod_name}.${lang_ext} ${CMAKE_CURRENT_BINARY_DIR}/${mod_name}.${lang_ext}.c
      DEPENDS ${file_path}/${mod_name}.${lang_ext}
      )
    list(APPEND include_sources
      ${CMAKE_CURRENT_BINARY_DIR}/${mod_name}.${lang_ext}.c)
  endforeach()
  if(create_prefix_MAL_LANGUAGE)
    set(MONETDB_CURRENT_MAL_SOURCES
      "${include_sources}"
      PARENT_SCOPE)
  endif()
  if(create_prefix_SQL_LANGUAGE)
    set(MONETDB_CURRENT_SQL_SOURCES
      "${include_sources}"
      PARENT_SCOPE)
  endif()
endfunction()

function(monetdb_cmake_summary)
  include(os_release_info)

  message(STATUS "")
  message(STATUS "Summary of cmake configuration of MonetDB")
  message(STATUS "-----------------------------------------")
  message(STATUS "System is big endian: ${IS_BIG_ENDIAN}")
  message(STATUS "Toolchain file: ${CMAKE_TOOLCHAIN_FILE}")
  if(${CMAKE_VERSION} VERSION_LESS "3.14.0")
    message(STATUS "NumPy include dirs: ${NUMPY_INCLUDE_DIRS}")
  else()
    message(STATUS "Numpy target: ")
  endif()
  message(STATUS "System name: ${CMAKE_SYSTEM_NAME}")
  message(STATUS "System version: ${CMAKE_SYSTEM_VERSION}")
  if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    get_os_release_info(LINUX_DISTRO LINUX_DISTRO_VERSION)
    message(STATUS "Linux distro: ${LINUX_DISTRO}")
    message(STATUS "Linux distro version: ${LINUX_DISTRO_VERSION}")
  endif()
  message(STATUS "Iconv library: ${Iconv_FOUND}")
  message(STATUS "Pthread library: ${CMAKE_USE_PTHREADS_INIT}")
  message(STATUS "Pcre library: ${PCRE_FOUND}")
  message(STATUS "Openssl library: ${OPENSSL_FOUND}")
  message(STATUS "Commoncrypto library: ${COMMONCRYPTO_FOUND}")
  message(STATUS "Bz2 library: ${BZIP2_FOUND}")
  message(STATUS "Curl library: ${CURL_FOUND}")
  message(STATUS "Lzma library: ${LIBLZMA_FOUND}")
  message(STATUS "Libxml2 library: ${LIBXML2_FOUND}")
  message(STATUS "Zlib library: ${ZLIB_FOUND}")
  message(STATUS "Lz4 library: ${LZ4_FOUND}")
  message(STATUS "Proj library: ${PROJ_FOUND}")
  message(STATUS "Snappy library: ${SNAPPY_FOUND}")
  message(STATUS "Geos library: ${GEOS_FOUND}")
  message(STATUS "Gdal library: ${GDAL_FOUND}")
  message(STATUS "Fits library: ${CFITSIO_FOUND}")
  message(STATUS "Uuid library: ${HAVE_UUID_GENERATE}")
  message(STATUS "Valgrind library: ${VALGRIND_FOUND}")
  message(STATUS "Kvm library: ${KVM_FOUND}")
  message(STATUS "Netcdf library: ${NETCDF_FOUND}")
  message(STATUS "Readline library: ${READLINE_FOUND}")
  message(STATUS "R library: ${LIBR_FOUND}")
  message(STATUS "ODBC: ${ODBC_FOUND}")
  message(STATUS "Sphinx: ${SPHINX_FOUND}")
  message(STATUS "Createrepo: ${CREATEREPO_FOUND}")
  message(STATUS "Rpmbuild: ${RPMBUILD_FOUND}")
  message(STATUS "Dpkg-buildpackage: ${DPKGBUILDPACKAGE_FOUND}")
  message(STATUS "Reprepro: ${REPREPRO_FOUND}")
  message(STATUS "Semodule: ${SEMODULE_FOUND}")
  message(STATUS "Awk: ${AWK_FOUND}")
  message(STATUS "flags: ${CMAKE_C_FLAGS}")
  message(STATUS "-----------------------------------------")
  message(STATUS "")
endfunction()

# CMake function to test if a variable exists in the cmake code.
function(assert_variable_exists assert_variable_variablename)
  if(NOT ${assert_variable_variablename})
    message(FATAL_ERROR "variable ${assert_variable_variablename} not defined")
    set(DETECT "1")
  else()
    set(DETECT "0")
  endif()

  configure_file(test_cmake_var.c.in
    "${CMAKE_CURRENT_BINARY_DIR}/test_${assert_variable_variablename}_var.c"
    @ONLY)

  add_executable("test_${assert_variable_variablename}_var")
  target_sources("test_${assert_variable_variablename}_var"
    PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/test_${assert_variable_variablename}_var.c")
  add_test("testDetect${assert_variable_variablename}" "test_${assert_variable_variablename}_var")
endfunction()

# CMake function to test if a cmake variable has a corresponding
# legacy variable defined in the monetdb_config.h header file.
function(assert_legacy_variable_exists)
  cmake_parse_arguments(
    assert_variable
    "dummy"
    "variablename;legacy_variablename"
    ""
    ${ARGN})
  if(${${assert_variable_variablename}})
    set(DETECT "0")
    set(UNDETECT "1")
  else()
    set(DETECT "1")
    set(UNDETECT "0")
  endif()
  configure_file(test_detect_legacy_var.c.in
    "${CMAKE_CURRENT_BINARY_DIR}/test_${assert_variable_legacy_variablename}_legacy_var.c"
    @ONLY)
  add_executable("test_${assert_variable_legacy_variablename}_legacy_var")
  target_sources("test_${assert_variable_legacy_variablename}_legacy_var"
    PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/test_${assert_variable_legacy_variablename}_legacy_var.c")
  target_link_libraries("test_${assert_variable_legacy_variablename}_legacy_var"
  PRIVATE
  monetdb_config_header)
  add_test("testDetect${assert_variable_legacy_variablename}Legacy"
    "test_${assert_variable_legacy_variablename}_legacy_var")
endfunction()

# CMake function to test if the package detection gave the
# expected result.
function(assert_package_detected)
  cmake_parse_arguments(
    assert_package
    "dummy"
    "variablename;legacyvariable;detect"
    ""
    ${ARGN})
  if(${assert_package_detect})
    set(DETECT "0")
    set(UNDETECT "1")
  else()
    set(DETECT "1")
    set(UNDETECT "0")
  endif()
  configure_file(test_package_detect.c.in
    "${CMAKE_CURRENT_BINARY_DIR}/test_${assert_package_variablename}_detect_var.c"
    @ONLY)
  add_executable("test_${assert_package_variablename}_detect_var")
  target_sources("test_${assert_package_variablename}_detect_var"
    PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/test_${assert_package_variablename}_detect_var.c")
  target_link_libraries("test_${assert_package_variablename}_detect_var"
  PRIVATE
  monetdb_config_header)
  add_test("testDetect${assert_package_variablename}Detect"
    "test_${assert_package_variablename}_detect_var")
endfunction()

function(monetdb_debian_extra_files)
  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/libmonetdb-client-dev
    COMPONENT clientdev)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/libmonetdb-client${MAPI_VERSION_MAJOR}
    COMPONENT client)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/libmonetdb-client-odbc
    COMPONENT odbc)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/libmonetdb-dev
    COMPONENT monetdb-dev)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/libmonetdb-stream-dev
    COMPONENT streamdev)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/libmonetdb-stream${STREAM_VERSION_MAJOR}
    COMPONENT stream)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/libmonetdb${GDK_VERSION_MAJOR}
    COMPONENT monetdb)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/libmonetdb-dev
    COMPONENT monetdbdev)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/monetdb-client-testing
    COMPONENT clienttest)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/monetdb-testing-python
    COMPONENT pytesting)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/monetdb-testing
    COMPONENT testing)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/monetdb-server-dev
    COMPONENT serverdev)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/monetdb5-sql
    COMPONENT sql)

  install(FILES
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/changelog
    ${CMAKE_SOURCE_DIR}/misc/packages/deb/copyright
    DESTINATION
    ${CMAKE_INSTALL_DATAROOTDIR}/doc/monetdb5-server
    COMPONENT server)
endfunction()

function(find_selinux_types)
  # The execute_process does not handle the single quotes around the awk
  # command well. That is why we run it from the file. Be careful that the
  # awk command is on a single line. Otherwise the output is not on a single
  # line, which is needed to convert it to a cmake list.
  # If the command fails, or awk is not found, we set a default list.
  if(AWK_FOUND)
    execute_process(COMMAND ${AWK_EXECUTABLE} "-f" "${CMAKE_SOURCE_DIR}/misc/selinux/selinux_types.awk" "/etc/selinux/config"
      WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
      RESULT_VARIABLE AWK_RETURN_CODE
      OUTPUT_VARIABLE AWK_OUTPUT_RES
      ERROR_VARIABLE AWK_ERROR_RES
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(AWK_RETURN_CODE EQUAL 0 AND AWK_OUTPUT_RES)
      set(SELINUX_TYPES "${AWK_OUTPUT_RES}" PARENT_SCOPE)
    else()
      message(WARNING "Unable to get selinux types. Using defaults.")
      set(SELINUX_TYPES "mls targeted" PARENT_SCOPE)
    endif()
  else()
    set(SELINUX_TYPES "mls targeted" PARENT_SCOPE)
  endif()
endfunction()

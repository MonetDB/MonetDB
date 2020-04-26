#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

function(monetdb_hg_revision)
  # Get the current version control revision
  if(EXISTS "${CMAKE_SOURCE_DIR}/.hg")
    execute_process(COMMAND "hg" "id" "-i" WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}" RESULT_VARIABLE HG_RETURN_CODE
      OUTPUT_VARIABLE HG_OUPUT_RES OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(HG_RETURN_CODE EQUAL 0 AND HG_OUPUT_RES)
      set(MERCURIAL_ID "${HG_OUPUT_RES}" PARENT_SCOPE)
    else()
      message(FATAL_ERROR "Failed to find mercurial ID")
    endif()
  elseif(EXISTS "${CMAKE_SOURCE_DIR}/.git")
    execute_process(COMMAND "git" "rev-parse" "--short" "HEAD" WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
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
      COMMAND ${Python3_EXECUTABLE} ${PROJECT_BINARY_DIR}/create_include_object.py ${mod_name} ${lang_ext} ${file_path}/${mod_name}.${lang_ext} ${CMAKE_CURRENT_BINARY_DIR}/${mod_name}.${lang_ext}.c
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
  message("Summary of cmake configuration of MonetDB")
  message("-----------------------------------------")
  if(${CMAKE_VERSION} VERSION_LESS "3.14.0")
    message("NumPy include dirs: ${NUMPY_INCLUDE_DIRS}")
  else()
    message("Numpy target: ")
  endif()
  message(STATUS "Geos library: ${GEOS_FOUND}")
  message(STATUS "Gdal library: ${GDAL_FOUND}")
  message("-----------------------------------------")
endfunction()

#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

if(PY3INTEGRATION)
  add_library(pyapi3 MODULE)

  set(include_mal_files
    pyapi3)

  create_include_object(
    MAL_LANGUAGE
    name_module "pyapi3"
    modules_list ${include_mal_files})

  target_sources(pyapi3
    PRIVATE
    pyapi3.c
    unicode3.c
    pytypes3.c
    type_conversion3.c
    formatinput3.c
    connection3.c
    pyloader3.c
    emit3.c
    conversion3.c
    connection.h
    conversion.h
    convert_loops.h
    emit.h
    formatinput.h
    pyapi.h
    pyheader.h
    pytypes.h
    type_conversion.h
    undef.h
    unicode.h
    ${MONETDB_CURRENT_MAL_SOURCES})

  target_include_directories(pyapi3
    PRIVATE
    $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<$<VERSION_LESS:${CMAKE_VERSION},3.14.0>:${NUMPY_INCLUDE_DIRS}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(pyapi3
    PRIVATE
    Python3::Python
    $<$<VERSION_GREATER_EQUAL:${CMAKE_VERSION},3.14.0>:Python3::NumPy>
    mutils
    stream
    gdk
    monetdb5
    monetdb_config_header)

  set_target_properties(pyapi3
    PROPERTIES 
    OUTPUT_NAME 
    _pyapi3)

  install(TARGETS
    pyapi3
    LIBRARY
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT server
    ARCHIVE
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    PUBLIC_HEADER
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

  if(WIN32)
    install(FILES $<TARGET_PDB_FILE:pyapi3> DESTINATION ${LIBDIR}/monetdb5 OPTIONAL)
    install(FILES pyapi_locatepython3.bat DESTINATION ${CMAKE_INSTALL_PREFIX})
  endif()
endif()
# vim: set ts=2:sw=2:et

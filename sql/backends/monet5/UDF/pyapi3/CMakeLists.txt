#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

if(HAVE_LIBPY3)
  add_library(pyapi3 MODULE)

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
    unicode.h)

  target_include_directories(pyapi3
    PRIVATE
    $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlinclude,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<$<VERSION_LESS:${CMAKE_VERSION},3.14.0>:${NUMPY_INCLUDE_DIRS}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(pyapi3
    PRIVATE
    Python3::Python
    $<$<VERSION_GREATER_EQUAL:${CMAKE_VERSION},3.14.0>:Python3::NumPy>
    sql
    mutils
    bat
    monetdb5
    monetdb_config_header)

  target_compile_definitions(pyapi3
    PRIVATE
    LIBPYAPI3)

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
    configure_file(pyapi_locatepython3.bat.in
      ${CMAKE_CURRENT_BINARY_DIR}/pyapi_locatepython3.bat @ONLY)

    install(FILES $<TARGET_PDB_FILE:pyapi3> DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5 OPTIONAL)
    install(FILES
      ${CMAKE_CURRENT_BINARY_DIR}/pyapi_locatepython3.bat
      DESTINATION "."
      COMPONENT server)
  endif()
endif()
# vim: set ts=2:sw=2:et

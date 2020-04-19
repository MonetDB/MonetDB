#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

if(PY2INTEGRATION)
  add_library(pyapi MODULE)

  target_sources(pyapi
    PRIVATE
    pyapi.c pyapi.h unicode.c unicode.h pytypes.c pytypes.h type_conversion.c type_conversion.h formatinput.c
    formatinput.h connection.c connection.h undef.h pyloader.c emit.h emit.c conversion.c conversion.h
    convert_loops.h pyheader.h)

  target_include_directories(pyapi
    PRIVATE
    $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(pyapi
    PRIVATE
    Python2::Python
		Python2::NumPy
    mutils
    stream
    gdk
    monetdb5
    monetdb_config_header)

  set_target_properties(pyapi
    PROPERTIES 
    OUTPUT_NAME 
    _pyapi)

  install(TARGETS
    pyapi
    LIBRARY
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT server
    ARCHIVE
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    PUBLIC_HEADER
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

  if(WIN32)
    install(FILES $<TARGET_PDB_FILE:pyapi> DESTINATION ${LIBDIR}/monetdb5 OPTIONAL)
    install(FILES pyapi_locatepython2.bat DESTINATION ${CMAKE_INSTALL_PREFIX})
  endif()

  install(FILES
    pyapi.mal
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT server)

  install(FILES
    50_pyapi.mal
    DESTINATION
    ${CMAKE_INSTALL_LIBDIR}/monetdb5/autoload
    COMPONENT server)
endif()
# vim: set ts=2:sw=2:et

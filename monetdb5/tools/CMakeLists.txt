#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_library(monetdb5 SHARED
  $<TARGET_OBJECTS:mal>
  $<TARGET_OBJECTS:atoms>
  $<TARGET_OBJECTS:malmodules>
  $<TARGET_OBJECTS:kernel>
  $<TARGET_OBJECTS:optimizer>)

target_link_libraries(monetdb5
    PRIVATE
    mutils
    mcrypt
    stream
    mapi
    msabaoth
    ${SOCKET_LIBRARIES}
    $<$<BOOL:${LibXml2_FOUND}>:LibXml2::LibXml2>
    $<$<BOOL:${PCRE_FOUND}>:PCRE::PCRE>
    $<$<BOOL:${UUID_FOUND}>:UUID::UUID>
    bat)

set_target_properties(monetdb5
  PROPERTIES
  VERSION ${MONETDB5_VERSION}
  SOVERSION ${MONETDB5_VERSION_MAJOR})

if (NOT WIN32)
  if(LibXml2_FOUND)
    set(PKG_LIBXML2 "libxml-2.0")
  endif()

  if(OPENSSL_FOUND)
    # In OpenSSL case, we link only to the crypto library, so it will be
    # always one library to link to
    set(PKG_CRYPTO "libcrypto")
  endif()

  if(PCRE_FOUND)
    set(PKG_PCRE "libpcre")
  endif()

  configure_file(monetdb5.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb5.pc @ONLY)

  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb5.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    COMPONENT serverdev)
endif()

install(TARGETS
  monetdb5
  EXPORT monetdb5Targets
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT server
  NAMELINK_COMPONENT serverdev
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT monetdb5Targets
  FILE monetdb5Targets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT serverdev)

# vim: set ts=2:sw=2:et

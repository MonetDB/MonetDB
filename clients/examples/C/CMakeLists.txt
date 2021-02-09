#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

if(TESTING)
  add_executable(sample0
    sample0.c)

  target_link_libraries(sample0
    PRIVATE mapi)

  add_executable(sample1
    sample1.c)

  target_link_libraries(sample1
    PRIVATE mapi)

  add_executable(sample4
    sample4.c)

  target_link_libraries(sample4
    PRIVATE mapi)

  add_executable(smack00
    smack00.c)

  target_link_libraries(smack00
    PRIVATE mapi)

  add_executable(smack01
    smack01.c)

  target_link_libraries(smack01
    PRIVATE mapi)

  add_executable(streamcat
    streamcat.c)

  target_link_libraries(streamcat
    PRIVATE
    monetdb_config_header
    stream)

  add_executable(bincopydata
    bincopydata.c
    bincopydata.h
    bincopytemporaldata.c
    bincopyuuid.c
  )

  target_link_libraries(bincopydata
    PRIVATE
    monetdb_config_header
    copybinary
  )

  add_executable(bincopyloops
    bincopyloops.c
  )

  target_link_libraries(bincopyloops
    PRIVATE
    monetdb_config_header
    copybinary
  )

  install(TARGETS
    sample0
    sample1
    sample4
    smack00
    smack01
    streamcat
    bincopydata
    RUNTIME
    DESTINATION
    ${CMAKE_INSTALL_BINDIR}
    COMPONENT clienttest)
endif()

# vim: set ts=2:sw=2:et

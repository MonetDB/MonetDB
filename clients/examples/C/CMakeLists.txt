#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
#]]

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

add_executable(testcondvar
  testcondvar.c)

target_link_libraries(testcondvar
  PRIVATE
  monetdb_config_header
  bat
  Threads::Threads
  )

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
  testcondvar
  bincopydata
  RUNTIME
  DESTINATION
  ${CMAKE_INSTALL_BINDIR}
  COMPONENT clienttest)

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:sample0>
    $<TARGET_PDB_FILE:sample1>
    $<TARGET_PDB_FILE:sample4>
    $<TARGET_PDB_FILE:smack00>
    $<TARGET_PDB_FILE:smack01>
    $<TARGET_PDB_FILE:streamcat>
    $<TARGET_PDB_FILE:testcondvar>
    $<TARGET_PDB_FILE:bincopydata>
    $<TARGET_PDB_FILE:bincopyloops>
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    OPTIONAL)
endif()

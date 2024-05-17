#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
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

target_include_directories(streamcat
  PRIVATE
  $<$<BOOL:${Iconv_IS_BUILT_IN}>:${CMAKE_REQUIRED_INCLUDES}>
  ../../mapiclient)

target_link_libraries(streamcat
  PRIVATE
  monetdb_config_header
  stream
  $<$<BOOL:${CURL_FOUND}>:CURL::libcurl>
  $<$<BOOL:${Iconv_FOUND}>:Iconv::Iconv>)

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
  bincopydecimal_impl.h
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

add_executable(murltest
  murltest.c
  testsfile.c
  murltest.h
)

target_link_libraries(murltest
  PRIVATE
  monetdb_config_header
  mapi
  stream
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
  murltest
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
    $<TARGET_PDB_FILE:murltest>
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    OPTIONAL)
endif()

#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
#]]

if(NOT MONETDB_STATIC)
  add_executable(example1 example1.c)
  target_link_libraries(example1
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(run_example1 example1)

  add_executable(example2 example2.c)
  target_link_libraries(example2
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(run_example2 example2)

  add_executable(example_temporal example_temporal.c)
  target_link_libraries(example_temporal
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(run_example_temporal example_temporal)

  add_executable(example_decimals example_decimals.c)
  target_link_libraries(example_decimals
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(run_example_decimals example_decimals)

  add_executable(example_blob example_blob.c)
  target_link_libraries(example_blob
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(run_example_blob example_blob)

  add_executable(example_append example_append.c)
  target_link_libraries(example_append
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(run_example_append example_append)

  add_executable(example_append_raw example_append_raw.c)
  target_link_libraries(example_append_raw
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(run_example_append_raw example_append_raw)

  if(NOT WIN32)
    add_executable(example_backup example_backup.c)
    target_link_libraries(example_backup
      PRIVATE
      monetdb_config_header
      monetdbe)
    add_test(run_example_backup example_backup)
  endif()

  if(NOT WIN32)
    add_executable(example_copy example_copy.c)
    target_link_libraries(example_copy
      PRIVATE
      monetdb_config_header
      monetdbe)
    add_test(run_example_copy example_copy)
  endif()

  add_executable(example_connections example_connections.c)
  target_link_libraries(example_connections
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(run_example_connections example_connections)

  add_executable(example_sessions example_sessions.c)
  target_link_libraries(example_sessions
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(run_example_sessions example_sessions)

  if(WITH_CMOCKA)
    add_executable(cmocka_test cmocka_test.c test_helper.c)
    target_include_directories(cmocka_test PRIVATE "${CMOCKA_INCLUDE_DIR}")
    target_link_libraries(cmocka_test
      PRIVATE
      monetdb_config_header
      monetdbe
      "${CMOCKA_LIBRARY}"
    )
    add_test(run_cmocka_test cmocka_test)
  endif()

  if (TESTING)
    add_executable(example_proxy example_proxy.c)
    target_link_libraries(example_proxy
      PRIVATE
      monetdb_config_header
      monetdbe)

    install(TARGETS
      example_proxy
      DESTINATION ${CMAKE_INSTALL_BINDIR}
      COMPONENT mbeddedtest)

    if(WIN32)
      install(FILES
        $<TARGET_PDB_FILE:example_proxy>
        DESTINATION ${CMAKE_INSTALL_BINDIR}
        OPTIONAL)
    endif()
  endif()
endif()

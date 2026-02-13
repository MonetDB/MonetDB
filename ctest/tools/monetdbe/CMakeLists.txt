#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.
#]]

if(NOT MONETDB_STATIC)
  add_executable(example1 example1.c)
  target_link_libraries(example1
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(NAME run_example1 COMMAND example1)

  add_executable(example2 example2.c)
  target_link_libraries(example2
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(NAME run_example2 COMMAND example2)

  add_executable(example_temporal example_temporal.c)
  target_link_libraries(example_temporal
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(NAME run_example_temporal COMMAND example_temporal)

  add_executable(example_decimals example_decimals.c)
  target_link_libraries(example_decimals
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(NAME run_example_decimals COMMAND example_decimals)

  add_executable(example_blob example_blob.c)
  target_link_libraries(example_blob
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(NAME run_example_blob COMMAND example_blob)

  add_executable(example_append example_append.c)
  target_link_libraries(example_append
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(NAME run_example_append COMMAND example_append)

  add_executable(example_append_raw example_append_raw.c)
  target_link_libraries(example_append_raw
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(NAME run_example_append_raw COMMAND example_append_raw)

  if(NOT WIN32)
    add_executable(example_backup example_backup.c)
    target_link_libraries(example_backup
      PRIVATE
      monetdb_config_header
      monetdbe)
    add_test(NAME run_example_backup COMMAND example_backup)
  endif()

  if(NOT WIN32)
    add_executable(example_copy example_copy.c)
    target_link_libraries(example_copy
      PRIVATE
      monetdb_config_header
      monetdbe)
    add_test(NAME run_example_copy COMMAND example_copy)
  endif()

  add_executable(example_connections example_connections.c)
  target_link_libraries(example_connections
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(NAME run_example_connections COMMAND example_connections)

  add_executable(example_sessions example_sessions.c)
  target_link_libraries(example_sessions
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(NAME run_example_sessions COMMAND example_sessions)

  add_executable(demo_oob_read demo_oob_read.c)
  target_link_libraries(demo_oob_read
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(NAME run_demo_oob_read COMMAND demo_oob_read)

  add_executable(demo_oob_write demo_oob_write.c)
  target_link_libraries(demo_oob_write
    PRIVATE
    monetdb_config_header
    monetdbe)
  add_test(NAME run_demo_oob_write COMMAND demo_oob_write)

  if(WITH_CMOCKA)
    add_executable(cmocka_test cmocka_test.c test_helper.c)
    target_include_directories(cmocka_test PRIVATE "${CMOCKA_INCLUDE_DIR}")
    target_link_libraries(cmocka_test
      PRIVATE
      monetdb_config_header
      monetdbe
      "${CMOCKA_LIBRARY}"
    )
    add_test(NAME run_cmocka_test COMMAND cmocka_test)
  endif()

  if (TESTING)
    add_executable(example_proxy example_proxy.c)
    target_link_libraries(example_proxy
      PRIVATE
      monetdb_config_header
      monetdbe)

    install(TARGETS
      demo_oob_read
      demo_oob_write
      example1
      example2
      example_append
      example_append_raw
      example_blob
      example_connections
      example_decimals
      example_proxy
      example_sessions
      example_temporal
      demo_oob_read
      demo_oob_write
      DESTINATION ${CMAKE_INSTALL_BINDIR}
      COMPONENT mbeddedtest)

    if(WIN32)
      install(FILES
        $<TARGET_PDB_FILE:demo_oob_read>
        $<TARGET_PDB_FILE:demo_oob_write>
        $<TARGET_PDB_FILE:example1>
        $<TARGET_PDB_FILE:example2>
        $<TARGET_PDB_FILE:example_append>
        $<TARGET_PDB_FILE:example_append_raw>
        $<TARGET_PDB_FILE:example_blob>
        $<TARGET_PDB_FILE:example_connections>
        $<TARGET_PDB_FILE:example_decimals>
        $<TARGET_PDB_FILE:example_proxy>
        $<TARGET_PDB_FILE:example_sessions>
        $<TARGET_PDB_FILE:example_temporal>
        DESTINATION ${CMAKE_INSTALL_BINDIR}
        OPTIONAL)
    else()
      install(TARGETS
        example_backup
        example_copy
        DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT mbeddedtest)
    endif()
  endif()
endif()

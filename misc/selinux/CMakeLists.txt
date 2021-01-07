#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

if(SEMODULE_FOUND)
  find_selinux_types()

  configure_file(monetdb.fc.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb.fc
    @ONLY)

  # TODO maybe: %{_unitdir}/monetdbd.service
  configure_file(post_install_script_file.in
    ${CMAKE_CURRENT_BINARY_DIR}/post_install_script_file
    @ONLY)

  # TODO: %{_rundir}
  configure_file(post_uninstall_script_file.in
    ${CMAKE_CURRENT_BINARY_DIR}/post_uninstall_script_file
    @ONLY)

  # We don't want to run the selinux makefile in the source directory, so
  # we copy these files to the build directory. Now the source directory
  # can be read-only.
  file(COPY monetdb.if
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}")
  file(COPY monetdb.te
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}")
  
  # Getting awk to generate the list is tricky. So handle the conversion
  # to a CMake list using cmake code.
  string(REGEX MATCHALL
    "([^\ ]+\ |[^\ ]+$)"
    SELINUX_TYPES_LIST
    "${SELINUX_TYPES}")
  # Use three seperate execute_process function calls. If you use one
  # call with three commands, they will run in parallel and that will fail.
  foreach(SETYPE IN LISTS SELINUX_TYPES_LIST)
    STRING(STRIP "${SETYPE}" SETYPE)
    execute_process(
      COMMAND "make" "NAME=${SETYPE}" "-f" "/usr/share/selinux/devel/Makefile"
      WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    execute_process(
      COMMAND "mv" ${CMAKE_CURRENT_BINARY_DIR}/monetdb.pp ${CMAKE_CURRENT_BINARY_DIR}/monetdb.pp.${SETYPE}
      WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    execute_process(
      COMMAND "make" "NAME=${SETYPE}" "-f" "/usr/share/selinux/devel/Makefile" "clean"
      WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
  endforeach()

  foreach(SETYPE IN LISTS SELINUX_TYPES_LIST)
    STRING(STRIP "${SETYPE}" SETYPE)
    install(FILES
      ${CMAKE_CURRENT_BINARY_DIR}/monetdb.pp.${SETYPE}
      DESTINATION
      ${CMAKE_INSTALL_DATADIR}/selinux/${SETYPE}
      RENAME monetdb.pp
      COMPONENT
      selinux)
  endforeach()

  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb.fc
    monetdb.if
    monetdb.te
    DESTINATION
    ${CMAKE_INSTALL_DATADIR}/doc/MonetDB-selinux
    COMPONENT
    selinux)

endif()

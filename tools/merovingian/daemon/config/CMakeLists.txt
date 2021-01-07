#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

configure_file(monetdbd.in
  ${CMAKE_CURRENT_BINARY_DIR}/monetdbd
  @ONLY)

configure_file(monetdbd.service.in
  ${CMAKE_CURRENT_BINARY_DIR}/monetdbd.service
  @ONLY)

configure_file(.merovingian_properties.in
  ${CMAKE_CURRENT_BINARY_DIR}/.merovingian_properties
  @ONLY)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/monetdbd
	DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/logrotate.d
  COMPONENT sql)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/monetdbd.service
  DESTINATION lib/systemd/system
  COMPONENT sql)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/.merovingian_properties
  .merovingian_lock
	DESTINATION ${CMAKE_INSTALL_LOCALSTATEDIR}/monetdb5/dbfarm
  COMPONENT sql)

install(DIRECTORY 
  DESTINATION ${CMAKE_INSTALL_LOCALSTATEDIR}/log/monetdb
  COMPONENT sql)

install(DIRECTORY 
  DESTINATION ${CMAKE_INSTALL_RUNSTATEDIR}/monetdb
  COMPONENT sql)

# vim: set ts=2:sw=2:et

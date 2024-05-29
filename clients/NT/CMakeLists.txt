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

# These files have to be installed in the root of the project. But you
# cannot use the CMAKE_INSTALL_PREFIX variable, because the WIX
# generator does not allow for absolute install paths. That is why we use
# the "." as a relative path.
if(WIN32)
  configure_file(mclient.bat.in
    ${CMAKE_CURRENT_BINARY_DIR}/mclient.bat @ONLY)
  configure_file(msqldump.bat.in
    ${CMAKE_CURRENT_BINARY_DIR}/msqldump.bat @ONLY)
  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/mclient.bat
    ${CMAKE_CURRENT_BINARY_DIR}/msqldump.bat
    DESTINATION "."
    COMPONENT server)
endif()

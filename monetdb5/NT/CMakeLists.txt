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

if(WIN32)
  configure_file(M5server.bat.in
    ${CMAKE_CURRENT_BINARY_DIR}/M5server.bat @ONLY)
  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/M5server.bat
    DESTINATION "."
    COMPONENT server)
endif()

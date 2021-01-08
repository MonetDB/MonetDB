#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

if(WIN32)
  install(FILES
    banner.bmp
    license.rtf
    monetdb.ico
    website.html
    DESTINATION ${CMAKE_INSTALL_DATADIR}
    COMPONENT server)
endif()

if(DEVELOPMENT AND NOT WIN32)
  install(FILES
    Maddlog
    PERMISSIONS ${PROGRAM_PERMISSIONS_DEFAULT}
    DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()

# vim: set ts=2:sw=2:et

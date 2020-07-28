#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

if(WIN32)
  install(FILES
    banner.bmp
    license.rtf
    monetdb.ico
    website.html
    DESTINATION ${CMAKE_INSTALL_PREFIX}
    COMPONENT server)
endif()

# vim: set ts=2:sw=2:et

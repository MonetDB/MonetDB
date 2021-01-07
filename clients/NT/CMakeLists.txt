#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

# These files have to be installed in the root of the project. But you
# cannot use the CMAKE_INSTALL_PREFIX variable, because the WIX
# generator does not allow for absolute install paths. That is why we use
# the "." as a relative path.
if(WIN32)
  install(FILES
    mclient.bat
    msqldump.bat
    DESTINATION "."
    COMPONENT server)
endif()

# vim: set ts=2:sw=2:et

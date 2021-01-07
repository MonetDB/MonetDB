#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

find_file(SELINUX_MAKEFILE Makefile
  PATHS /usr/share/selinux/devel
  DOC "Manage selinux policy modules"
  NO_DEFAULT_PATH
)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(Semodule
  DEFAULT_MSG
  SELINUX_MAKEFILE
)

mark_as_advanced(SELINUX_MAKEFILE)

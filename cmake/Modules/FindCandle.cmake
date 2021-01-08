#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

find_program(CANDLE_EXECUTABLE NAMES candle
  DOC "Create wix packages"
)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(Candle
  DEFAULT_MSG
  CANDLE_EXECUTABLE
)

mark_as_advanced(CANDLE_EXECUTABLE)

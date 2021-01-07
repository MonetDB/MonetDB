#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

# [TODO]: install created python file
configure_file(create_include_object.py.in
  ${CMAKE_CURRENT_BINARY_DIR}/create_include_object.py
  @ONLY)

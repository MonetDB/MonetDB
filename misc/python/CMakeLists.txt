#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.
#]]

# [TODO]: install created python file
configure_file(create_include_object.py.in
  ${CMAKE_CURRENT_BINARY_DIR}/create_include_object.py
  @ONLY)

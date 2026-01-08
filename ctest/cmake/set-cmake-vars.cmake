#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.
#]]

# Test for the existence of these cmake variables, the source code assumes
# that they are set by the buildsystem.

assert_variable_exists(HAVE_CUDF)

if(HAVE_LIBR)
  assert_variable_exists(RHOME)
endif()

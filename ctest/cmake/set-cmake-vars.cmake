#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
#]]

# Test for the existence of these cmake variables, the source code assumes
# that they are set by the buildsystem.

assert_variable_exists(HAVE_CUDF)

if(HAVE_LIBR)
  assert_variable_exists(RHOME)
endif()

if(DEFINED HAVE_GETOPT_H)
  assert_variable_exists(HAVE_GETOPT)
endif()

#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

# Test for the existance of these cmake variables, the source code assumes
# that they are set by the buildsystem.

assert_variable_exists(DIR_SEP)
assert_variable_exists(PKGCONFIGDIR)
assert_variable_exists(BINDIR)
assert_variable_exists(LOCALSTATEDIR)
assert_variable_exists(DIR_SEP_STR)
assert_variable_exists(PATH_SEP)
assert_variable_exists(SO_PREFIX)
assert_variable_exists(SO_EXT)
assert_variable_exists(LIBDIR)
# assert_variable_exists(ENABLE_STATIC_ANALYSIS)

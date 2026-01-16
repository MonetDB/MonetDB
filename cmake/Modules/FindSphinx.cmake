#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.
#]]

find_program(SPHINX_EXECUTABLE NAMES sphinx-build
  HINTS
  $ENV{SPHINX_DIR}
  PATH_SUFFIXES bin
  DOC "Sphinx documentation generator"
)
 
include(FindPackageHandleStandardArgs)
 
find_package_handle_standard_args(Sphinx DEFAULT_MSG
  SPHINX_EXECUTABLE
)
 
mark_as_advanced(SPHINX_EXECUTABLE)

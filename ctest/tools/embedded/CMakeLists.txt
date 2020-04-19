#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
#]]

add_executable(example1 example1.c)
target_link_libraries(example1
  PRIVATE
    monetdb_config_header
    embedded
    sqlinclude
    gdk
    mapi
    #sql
    )
add_test(run_example1 example1)

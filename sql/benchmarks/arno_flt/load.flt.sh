#!/bin/sh

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

set -x

if [ $# -lt 1 ]; then
	echo "usage load directory-with-data"
	exit 1
fi

mclient <<EOF
bulkload("atom",  "$1/ATOM.flt.tbl",  ",", "\n", 4893);
bulkload("bond",  "$1/BOND.flt.tbl",  ",", "\n", 5243);
bulkload("model", "$1/MODEL.flt.tbl", ",", "\n",  188);
commit;
quit;
EOF


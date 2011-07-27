#!/bin/sh

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

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


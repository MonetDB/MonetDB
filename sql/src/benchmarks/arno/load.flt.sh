#!/bin/sh
set -x

if [ $# -lt 1 ]; then
	echo "usage load directory-with-data"
	exit 1
fi

MapiClient <<EOF
bulkload("atom",  "$1/ATOM.flt.tbl",  ",", "\n", 4893);
bulkload("bond",  "$1/BOND.flt.tbl",  ",", "\n", 5243);
bulkload("model", "$1/MODEL.flt.tbl", ",", "\n",  188);
commit;
quit;
EOF


#!/bin/sh
set -x

if [ $# -lt 1 ]; then
	echo "usage load directory with data"
	exit 1
fi

MapiClient <<EOF
bulkload("atom",  "$1/ATOM.int.tbl",  ",", "\n", 4893);
bulkload("bond",  "$1/BOND.int.tbl",  ",", "\n", 5243);
bulkload("model", "$1/MODEL.int.tbl", ",", "\n",  188);
commit;
quit;
EOF


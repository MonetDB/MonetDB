#!/bin/sh
set -x

if [ $# -lt 1 ]; then
	echo "usage load directory with data"
	exit 1
fi

#Mdestroydb -db sql
#Mcreatedb -db sql
#Mserver -db sql ~/data/sql/Linux/share/sql/sqlserver.mil \
#		~/data/monet/Linux/share/Monet/mapi.mil 

#sql_client < ~/data/sql/src/benchmarks/tpch/c.sql

MapiClient <<EOF
bulkload("customer", "$1/customer.tbl", "|", "|\n", 150000);
bulkload("nation", "$1/nation.tbl", "|", "|\n", 25);
bulkload("orders", "$1/orders.tbl", "|", "|\n", 1500000);
bulkload("partsupp", "$1/partsupp.tbl", "|", "|\n", 800000);
bulkload("part", "$1/part.tbl", "|", "|\n", 200000);
bulkload("region", "$1/region.tbl", "|", "|\n", 5);
bulkload("supplier", "$1/supplier.tbl", "|", "|\n", 10000);
commit;
quit;
EOF

MapiClient -everything <<EOF
bulkload("lineitem", "$1/lineitem.tbl", "|", "|\n", 7000000);
commit;
quit;
EOF

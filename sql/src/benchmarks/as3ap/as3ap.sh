#!/bin/sh
PWD=`pwd`

rm -rf /ufs/niels/data/dbfarm/sql 
Mcreatedb -db sql
Mserver -db sql ../../catalog.mil ../../sql.mil <<EOF 
sql( "$PWD/as3ap-schema.sql", "$PWD/as3ap-schema.mil", 0);
quit;
EOF
echo "commit();quit;" >> as3ap-schema.mil
Mserver -db sql ../../sql.mil as3ap-schema.mil

Mserver -db sql <<EOF
module(ascii_io);

proc merge( BAT[str,BAT] bats, str name ) := {
	var sqlbats := column_name.semijoin(
		column_table.select(table_name.reverse.find(name)));
	var x:= column_bat.reverse().join(sqlbats.join(bats));
	x@batloop(){ \$h.insert(\$t); }
}

merge(load_data( load_format("$PWD/uniques.fmt"), "$PWD/uniques.dat", 100000 ), "uniques");
merge(load_data( load_format("$PWD/hundred.fmt"), "$PWD/hundred.dat", 100000), "hundred");
merge(load_data( load_format("$PWD/tenpct.fmt"), "$PWD/tenpct.dat", 100000), "tenpct");
merge(load_data( load_format("$PWD/updates.fmt"), "$PWD/updates.dat", 100000 ), "updates");

commit();
quit;
EOF

Mserver -db sql ../../sql.mil <<EOF
sql( "$PWD/as3ap-queries.sql", "$PWD/as3ap-queries.mil", 0);
quit;
EOF
echo "commit();quit;" >> as3ap-queries.mil
Mserver -db sql ../../catalog.mil ../../sql.mil < as3ap-queries.mil

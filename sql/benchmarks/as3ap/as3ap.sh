#!/bin/sh
PWD=`pwd`

Mdestroydb -db sql
Mcreatedb -db sql
Mserver -db sql $SQL_PREFIX/lib/MonetDB/sql_server.mil <<EOF 
sql_file( "$PWD/as3ap-schema.sql", "$PWD/as3ap-schema.t" );
quit;
EOF
echo "myc := mvc_create(0);" > as3ap-schema.mil
echo "Output := Stdout;" >> as3ap-schema.mil
cat as3ap-schema.t >> as3ap-schema.mil
rm as3ap-schema.t
echo "commit();quit;" >> as3ap-schema.mil
Mserver -db sql $SQL_PREFIX/lib/MonetDB/sql_server.mil as3ap-schema.mil

Mserver -db sql $SQL_PREFIX/lib/MonetDB/sql_server.mil <<EOF
module(ascii_io);

proc merge( BAT[str,BAT] bats, str name ) := {
	var sqlbats := column_name.semijoin(
		column_table.select(table_name.reverse.find(name)));
	var x:= column_bat.reverse().join(sqlbats.join(bats));
	x@batloop(){ \$h.insert(\$t); }
}

bulkload( "uniques", "$PWD/uniques.dat", ",", "\n", 100000 );
bulkload( "hundred", "$PWD/hundred.dat", ",", "\n", 100000 );
bulkload( "tenpct", "$PWD/tenpct.dat", ",", "\n", 100000 );
bulkload( "updates", "$PWD/updates.dat", ",", "\n", 100000 );

commit();
quit;
EOF

Mserver -db sql $SQL_PREFIX/lib/MonetDB/sql_server.mil <<EOF 
sql_file( "$PWD/as3ap-queries.sql", "$PWD/as3ap-queries.t");
quit;
EOF
echo "myc := mvc_create(0);" > as3ap-queries.mil
echo "Output := Stdout;" >> as3ap-queries.mil
cat as3ap-queries.t >> as3ap-queries.mil
rm as3ap-queries.t
echo "commit();quit;" >> as3ap-queries.mil
Mserver -db sql $SQL_PREFIX/lib/MonetDB/sql_server.mil as3ap-queries.mil

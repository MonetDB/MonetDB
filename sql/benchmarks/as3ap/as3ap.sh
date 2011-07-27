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

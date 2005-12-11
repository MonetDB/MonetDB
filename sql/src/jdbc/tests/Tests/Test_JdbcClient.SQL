#!/bin/sh

PATHS=""
if [ -d $SQL_BUILD/src/jdbc/ ] ; then
	PATHS="${PATHS} `$CYGPATH_WP "${SQL_BUILD}/src/jdbc/"`"
elif [ -d ${TSTBLDBASE}/src/jdbc/ ] ; then
	PATHS="${PATHS} `$CYGPATH_WP "${TSTBLDBASE}/src/jdbc/"`"
elif [ -f ${SQL_PREFIX}/share/MonetDB/lib/] ; then
	PATHS="${PATHS} `$CYGPATH_WP ${SQL_PREFIX}/share/MonetDB/lib/"`"
elif [ -f ${MONETDB_PREFIX}/share/MonetDB/lib/ ] ; then
	PATHS="${PATHS} `$CYGPATH_WP ${MONETDB_PREFIX}/share/MonetDB/lib/"`"
else
	PATHS="${PATHS} `$CYGPATH_WP "${TSTTRGBASE}/share/MonetDB/lib/"`"
fi

JAR="`find ${PATHS} -name 'jdbcclient*.jar'`"

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LC_CTYPE="en_US.UTF-8"
export LC_CTYPE

Mlog -x "java -jar \"$JAR\" -h $HOST -p $SQLPORT -f $RELSRCDIR/../JdbcClient_create_tables.sql"

Mlog -x "java -jar \"$JAR\" -h $HOST -p $SQLPORT -f $RELSRCDIR/../JdbcClient_inserts_selects.sql"

Mlog -x "java -jar \"$JAR\" -h $HOST -p $SQLPORT -d"

rm -f .monetdb

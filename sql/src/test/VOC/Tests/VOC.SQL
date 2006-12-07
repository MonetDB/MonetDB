#!/bin/bash

# necessary for version number
source ${TSTSRCBASE}/src/jdbc/build.properties

JAR="${TSTBLDBASE}/src/jdbc/jdbcclient-${JDBC_MAJOR}.${JDBC_MINOR}.jar"

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

LC_ALL="`locale -a | grep -i en_us | grep -i utf | head -n 1`"
export LC_ALL

Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCcreate_user.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCcreate_user.sql 2>&1

cat << EOF > .monetdb
user=voc
password=voc
EOF

Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCschema.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCschema.sql 2>&1
Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCinsert.sql -Xbatching 400"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCinsert.sql 2>&1
Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCquery.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCquery.sql 2>&1
Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCmanual_examples.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCmanual_examples.sql 2>&1
Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCdrop.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCdrop.sql 2>&1

cat << EOF > .monetdb
user=monetdb
password=monetdb
EOF

Mlog   "$MTIMEOUT java -jar \"$JAR\" -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCdrop_user.sql"
	$MTIMEOUT java -jar  "$JAR"  -h $HOST -p $MAPIPORT -d ${TSTDB} -e -f $RELSRCDIR/../VOCdrop_user.sql 2>&1

rm -f .monetdb

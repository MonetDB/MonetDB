#!/bin/sh

# converts PostgreSQL specific SQL into SQL99 equivalent (if possible)

dirout="sql99"
dirin="sql"
# create a directory to output our converted files to
mkdir $dirout >& /dev/null

files=`cd ${dirin}; ls *.sql`
for file in $files;
do
	sed -r \
		-e 's/\bint8\b/bigint/ig' \
		-e 's/\bint4\b/integer/ig' \
		-e 's/\bint2\b/smallint/ig' \
		-e 's/\s+([^\s]+)::float[248]\b/ cast(\1 as double)/ig' \
		-e 's/\s+([^\s]+)::int2\b/ cast(\1 as smallint)/ig' \
		-e 's/\s+([^\s]+)::int4\b/ cast(\1 as integer)/ig' \
		-e 's/\s+([^\s]+)::int8\b/ cast(\1 as bigint)/ig' \
		-e 's/\s+([^\s]+)::(\w+)\b/ cast(\1 as \2)/ig' \
		${dirin}/${file} > ${dirout}/${file}
done

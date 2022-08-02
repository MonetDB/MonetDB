create table t (c varchar(3));

COPY 1 RECORDS INTO t FROM STDIN USING DELIMITERS '|','
','"' NULL AS '';
"abcd"


drop table t;

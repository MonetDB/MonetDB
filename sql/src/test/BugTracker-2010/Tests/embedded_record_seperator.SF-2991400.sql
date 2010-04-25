
create table ers (c varchar(3));

COPY 1 RECORDS INTO ers FROM STDIN USING DELIMITERS '|',':','"' NULL AS '';
"a:c":

COPY 1 RECORDS INTO ers FROM STDIN USING DELIMITERS '|','\n','"' NULL AS '';
"a
c"

drop table ers;

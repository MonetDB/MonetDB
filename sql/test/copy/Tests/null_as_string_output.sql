start transaction;
-- here we test the NULL as 'string' feature

create table null_as_string (i int, s string, d decimal(5,2));

-- treat empty values as null
copy 4 records into null_as_string from stdin delimiters ',',E'\n' NULL as '';
,,
,zero,0
1,,1
2,two,

copy select * from null_as_string into stdout delimiters ',',E'\n' NULL as '';
delete from null_as_string;

-- default 'NULL' as null
copy 4 records into null_as_string from stdin delimiters ',',E'\n';
NULL,NULL,NULL
NULL,zero,0
1,NULL,1
2,two,NULL

copy select * from null_as_string into stdout delimiters ',',E'\n';
delete from null_as_string;

-- postgres like \N somehow 
copy 4 records into null_as_string from stdin delimiters ',',E'\n' NULL as E'\\N';
\N,\N,\N
\N,zero,0
1,\N,1
2,two,\N

copy select * from null_as_string into stdout delimiters ',',E'\n' NULL as E'\\N';
delete from null_as_string;

drop table null_as_string;
rollback;

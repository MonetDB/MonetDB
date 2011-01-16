
-- here we test the NULL as 'string' feature

create table null_as_string (i int, s string, d decimal(5,2));

-- treat empty values as null
copy 4 records into null_as_string from stdin delimiters ',','\n' NULL as '';
,,
,zero,0
1,,1
2,two,

Select * from null_as_string;
delete from null_as_string;

-- default 'NULL' as null
copy 4 records into null_as_string from stdin delimiters ',','\n';
NULL,NULL,NULL
NULL,zero,0
1,NULL,1
2,two,NULL

Select * from null_as_string;
delete from null_as_string;

-- postgres like \N somehow 
copy 4 records into null_as_string from stdin delimiters ',','\n' NULL as '\\N';
\N,\N,\N
\N,zero,0
1,\N,1
2,two,\N

Select * from null_as_string;
delete from null_as_string;

drop table null_as_string;

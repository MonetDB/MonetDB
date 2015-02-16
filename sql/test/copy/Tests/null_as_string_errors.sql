-- here we check some error conditions for the NULL as 'string' feature

create table null_as_string (i int, s string, d decimal(5,2));

-- treat NULL values as errors when we use empty string as NULL string 
copy 1 records into null_as_string from stdin delimiters ',','\n' NULL as '';
NULL,NULL,NULL

select * from sys.rejects;
call sys.clearrejects();

copy 1 records into null_as_string from stdin delimiters ',','\n' NULL as '';
NULL,zero,0

select * from sys.rejects;
call sys.clearrejects();

-- shouldn't fail because NULL as string is just fine
copy 1 records into null_as_string from stdin delimiters ',','\n' NULL as '';
1,NULL,1

select * from sys.rejects;
call sys.clearrejects();

copy 1 records into null_as_string from stdin delimiters ',','\n' NULL as '';
2,two,NULL

select * from null_as_string;
delete from null_as_string;
select * from sys.rejects;
call sys.clearrejects();

-- treat empty values as errors for the default NULL value 
copy 1 records into null_as_string from stdin delimiters ',','\n' best effort;
,,
select * from sys.rejects;
call sys.clearrejects();

copy 1 records into null_as_string from stdin delimiters ',','\n' best effort;
,zero,0
select * from sys.rejects;
call sys.clearrejects();

-- shouldn't fail because empty strings are just fine
copy 1 records into null_as_string from stdin delimiters ',','\n' best effort;
1,,1
select * from sys.rejects;
call sys.clearrejects();

copy 1 records into null_as_string from stdin delimiters ',','\n' best effort;
2,two,

select * from null_as_string;
delete from null_as_string;
select * from sys.rejects;
call sys.clearrejects();

drop table null_as_string;

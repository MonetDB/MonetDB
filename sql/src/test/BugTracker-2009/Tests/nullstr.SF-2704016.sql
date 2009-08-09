create table testnull (
	node int,
	k varchar(255),
	v varchar(1024)
);
copy 1 records into testnull from stdin using delimiters ',','\n','''';
1,'test','Nullen RA'
select * from testnull;
drop table testnull;

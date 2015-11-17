start transaction;

create table varcharsize5 (id int, varchar106 varchar(5));

insert into varcharsize5 values (1,'不要让早把');

copy 1 records into varcharsize5 from stdin using delimiters ',','\n','"';
"1","不要让早把"

select * from varcharsize5;

rollback;

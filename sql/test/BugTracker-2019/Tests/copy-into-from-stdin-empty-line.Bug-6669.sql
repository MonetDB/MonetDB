start transaction;

create table foo (bar int, baz varchar(10));
copy into foo from stdin delimiters ',', '\n', '''';
5,'aa5aa'
0,'aa0aa'

-- empty line above ends input for COPY INTO
select * from foo;
rollback;

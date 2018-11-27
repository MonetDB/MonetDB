create table foo (s string);
copy 1 records into foo from stdin using delimiters ',',E'\n','"';
"quote: "" another: "" third: """
select * from foo;
drop table foo;

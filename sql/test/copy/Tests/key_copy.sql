create table cik1(i int, primary key(i));
create table cik2(i int, j int, primary key(i,j));

copy 4 records into cik1 from stdin USING DELIMITERS ',','\n','\"' NULL AS '';
0
5
5
9

select * from cik1;

copy 4 records into cik2 from stdin USING DELIMITERS ',','\n','\"' NULL AS '';
0,1
5,1
5,1
9,1

select * from cik2;

drop table cik1;
drop table cik2;

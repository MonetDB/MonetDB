
-- sample value constellationa where multi-column joins drop results
-- (false-negatives) because (sequences of) mkey.bulk_rotate_xor_hash()
-- produce NIL (which then does not match/join)

start transaction;

create table r2 (a bigint, b bigint, x bigint generated always as identity primary key);
insert into r2 (a,b) values (11,21),(12,22),(13,23),(2199023255552,0),(0,2199023255552),(6597069766656,1),(1,6597069766656),(4398046511104,-9223372036854775807),(-9223372036854775807,4398046511104),(1,-9223372036850581504),(-9223372036850581504,1),(-1,9223372036854775807),(9223372036854775807,-1);
create table s2 as (select a, b, -x as y from r2 order by x desc) with data;
select * from r2;
select * from s2;
select * from r2 natural join s2 order by x,y;
select * from s2 natural join r2 order by x,y;

create table r3 (a bigint, b bigint, c bigint, x bigint generated always as identity primary key);
insert into r3 (a,b,c) values (11,21,31),(12,22,32),(13,23,33),(2147483648,0,0),(0,2147483648,0),(0,0,2147483648),(140737488355328,0,0),(0,140737488355328,0),(0,0,140737488355328);
create table s3 as (select a, b, c, -x as y from r3 order by x desc) with data;
select * from r3;
select * from s3;
select * from r3 natural join s3 order by x,y;
select * from s3 natural join r3 order by x,y;

create table r4 (a bigint, b bigint, c bigint, d bigint, x bigint generated always as identity primary key);
insert into r4 (a,b,c,d) values (11,21,31,41),(12,22,32,42),(13,23,33,43),(16777216,0,0,0),(0,16777216,0,0),(0,0,16777216,0),(0,0,0,16777216),(1125899906842624,0,0,0),(0,1125899906842624,0,0),(0,0,1125899906842624,0),(0,0,0,1125899906842624);
create table s4 as (select a, b, c, d, -x as y from r4 order by x desc) with data;
select * from r4;
select * from s4;
select * from r4 natural join s4 order by x,y;
select * from s4 natural join r4 order by x,y;

create table r5 (a bigint, b bigint, c bigint, d bigint, e bigint, x bigint generated always as identity primary key);
insert into r5 (a,b,c,d,e) values (11,21,31,41,51),(12,22,32,42,52),(13,23,33,43,53),(524288,0,0,0,0),(0,524288,0,0,0),(0,0,524288,0,0),(0,0,0,524288,0),(0,0,0,0,524288),(4503599627370496,0,0,0,0),(0,4503599627370496,0,0,0),(0,0,4503599627370496,0,0),(0,0,0,4503599627370496,0),(0,0,0,0,4503599627370496);
create table s5 as (select a, b, c, d, e, -x as y from r5 order by x desc) with data;
select * from r5;
select * from s5;
select * from r5 natural join s5 order by x,y;
select * from s5 natural join r5 order by x,y;

create table r6 (a bigint, b bigint, c bigint, d bigint, e bigint, f bigint, x bigint generated always as identity primary key);
insert into r6 (a,b,c,d,e,f) values (11,21,31,41,51,61),(12,22,32,42,52,62),(13,23,33,43,53,63),(8192,0,0,0,0,0),(0,8192,0,0,0,0),(0,0,8192,0,0,0),(0,0,0,8192,0,0),(0,0,0,0,8192,0),(0,0,0,0,0,8192),(9007199254740992,0,0,0,0,0),(0,9007199254740992,0,0,0,0),(0,0,9007199254740992,0,0,0),(0,0,0,9007199254740992,0,0),(0,0,0,0,9007199254740992,0),(0,0,0,0,0,9007199254740992);
create table s6 as (select a, b, c, d, e, f, -x as y from r6 order by x desc) with data;
select * from r6;
select * from s6;
select * from r6 natural join s6 order by x,y;
select * from s6 natural join r6 order by x,y;

create table r7 (a bigint, b bigint, c bigint, d bigint, e bigint, f bigint, g bigint, x bigint generated always as identity primary key);
insert into r7 (a,b,c,d,e,f,g) values (11,21,31,41,51,61,71),(12,22,32,42,52,62,72),(13,23,33,43,53,63,73),(32768,0,0,0,0,0,0),(0,32768,0,0,0,0,0),(0,0,32768,0,0,0,0),(0,0,0,32768,0,0,0),(0,0,0,0,32768,0,0),(0,0,0,0,0,32768,0),(0,0,0,0,0,0,32768),(36028797018963968,0,0,0,0,0,0),(0,36028797018963968,0,0,0,0,0),(0,0,36028797018963968,0,0,0,0),(0,0,0,36028797018963968,0,0,0),(0,0,0,0,36028797018963968,0,0),(0,0,0,0,0,36028797018963968,0),(0,0,0,0,0,0,36028797018963968);
create table s7 as (select a, b, c, d, e, f, g, -x as y from r7 order by x desc) with data;
select * from r7;
select * from s7;
select * from r7 natural join s7 order by x,y;
select * from s7 natural join r7 order by x,y;

create table r8 (a bigint, b bigint, c bigint, d bigint, e bigint, f bigint, g bigint, h bigint, x bigint generated always as identity primary key);
insert into r8 (a,b,c,d,e,f,g,h) values (11,21,31,41,51,61,71,81),(12,22,32,42,52,62,72,82),(13,23,33,43,53,63,73,83),(128,0,0,0,0,0,0,0),(0,128,0,0,0,0,0,0),(0,0,128,0,0,0,0,0),(0,0,0,128,0,0,0,0),(0,0,0,0,128,0,0,0),(0,0,0,0,0,128,0,0),(0,0,0,0,0,0,128,0),(0,0,0,0,0,0,0,128),(36028797018963968,0,0,0,0,0,0,0),(0,36028797018963968,0,0,0,0,0,0),(0,0,36028797018963968,0,0,0,0,0),(0,0,0,36028797018963968,0,0,0,0),(0,0,0,0,36028797018963968,0,0,0),(0,0,0,0,0,36028797018963968,0,0),(0,0,0,0,0,0,36028797018963968,0),(0,0,0,0,0,0,0,36028797018963968);
create table s8 as (select a, b, c, d, e, f, g, h, -x as y from r8 order by x desc) with data;
select * from r8;
select * from s8;
select * from r8 natural join s8 order by x,y;
select * from s8 natural join r8 order by x,y;

create table r9 (a bigint, b bigint, c bigint, d bigint, e bigint, f bigint, g bigint, h bigint, i bigint, x bigint generated always as identity primary key);
insert into r9 (a,b,c,d,e,f,g,h,i) values (11,21,31,41,51,61,71,81,91),(12,22,32,42,52,62,72,82,92),(13,23,33,43,53,63,73,83,93),(128,0,0,0,0,0,0,0,0),(0,128,0,0,0,0,0,0,0),(0,0,128,0,0,0,0,0,0),(0,0,0,128,0,0,0,0,0),(0,0,0,0,128,0,0,0,0),(0,0,0,0,0,128,0,0,0),(0,0,0,0,0,0,128,0,0),(0,0,0,0,0,0,0,128,0),(0,0,0,0,0,0,0,0,128),(72057594037927936,0,0,0,0,0,0,0,0),(0,72057594037927936,0,0,0,0,0,0,0),(0,0,72057594037927936,0,0,0,0,0,0),(0,0,0,72057594037927936,0,0,0,0,0),(0,0,0,0,72057594037927936,0,0,0,0),(0,0,0,0,0,72057594037927936,0,0,0),(0,0,0,0,0,0,72057594037927936,0,0),(0,0,0,0,0,0,0,72057594037927936,0),(0,0,0,0,0,0,0,0,72057594037927936);
create table s9 as (select a, b, c, d, e, f, g, h, i, -x as y from r9 order by x desc) with data;
select * from r9;
select * from s9;
select * from r9 natural join s9 order by x,y;
select * from s9 natural join r9 order by x,y;

alter table r2 add constraint ab_unique unique (a,b);
insert into r2 (a,b) values (2199023255552,0);
select * from r2;

rollback;


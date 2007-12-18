create table test ( str VARCHAR(20), str2 VARCHAR(20));
insert into test values ('', 'test');
insert into test values ('test', '');
insert into test values ('','');
insert into test values (' Test ','');
select * from test;

select length(str), str from test;

select substring(str from 2 for 8), str2 from test;
select substring(str, 2, 8), str2 from test;
select substring(str from 2 for 1), str2 from test;
select substring(str, 2, 1), str2 from test;
select substring(str from 2), str2 from test;
select substring(str, 2), str2 from test;

select position(str in str2), str, str2 from test;
select locate(str,str2), str, str2 from test;

select ascii(str), str from test;
select code(ascii(str)), str from test;

select left(str,3), str from test;
select right(str,3), str from test;

select lower(str), str from test;
select lcase(str), str from test;
select upper(str), str from test;
select ucase(str), str from test;

select trim(str), str from test;
select ltrim(str), str from test;
select rtrim(str), str from test;

select insert(str,2,4,str2), str, str2 from test;
select replace(str,'t',str), str, str2 from test;

select repeat(str,4), str from test;
select ascii(4), str from test;

select str,str2,soundex(str),soundex(str2), editdistance2(soundex(str),soundex(str2)), editdistance(soundex(str),soundex(str2)), difference(str,str2) from test;

insert into test values ('monetdbiscool', 'coolismonetdb');
select levenshtein(str,str2, 1, 2, 3), str, str2 from test;

insert into test values ('monetdb45is+ cool', '  123123  123123  ');
select qgramnormalize(str) , qgramnormalize(str2), str, str2 from test;


drop table test;

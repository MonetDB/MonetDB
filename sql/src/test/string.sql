create table stringtest ( str VARCHAR(20), str2 VARCHAR(20));
insert into stringtest values ('', 'test');
insert into stringtest values ('test', '');
insert into stringtest values ('','');
insert into stringtest values (' Test ','');
select * from stringtest;

select length(str), str from stringtest;

select substring(str from 2 for 8), str2 from stringtest;
select substring(str, 2, 8), str2 from stringtest;
select substring(str from 2 for 1), str2 from stringtest;
select substring(str, 2, 1), str2 from stringtest;
select substring(str from 2), str2 from stringtest;
select substring(str, 2), str2 from stringtest;

select position(str in str2), str, str2 from stringtest;
select locate(str,str2), str, str2 from stringtest;

select ascii(str), str from stringtest;
select code(ascii(str)), str from stringtest;

select left(str,3), str from stringtest;
select right(str,3), str from stringtest;

select lower(str), str from stringtest;
select lcase(str), str from stringtest;
select upper(str), str from stringtest;
select ucase(str), str from stringtest;

select trim(str), str from stringtest;
select ltrim(str), str from stringtest;
select rtrim(str), str from stringtest;

select insert(str,2,4,str2), str, str2 from stringtest;
select replace(str,'t',str), str, str2 from stringtest;

select repeat(str,4), str from stringtest;
select ascii(4), str from stringtest;

select str,str2,soundex(str),soundex(str2), editdistance2(soundex(str),soundex(str2)), editdistance(soundex(str),soundex(str2)), difference(str,str2) from stringtest;

insert into stringtest values ('monetdbiscool', 'coolismonetdb');
select levenshtein(str,str2, 1, 2, 3), str, str2 from stringtest;

insert into stringtest values ('monetdb45is+ cool', '  123123  123123  ');
select qgramnormalize(str) , qgramnormalize(str2), str, str2 from stringtest;


drop table stringtest;

create table jspath(js json);
insert into jspath values('{"book": [{ "category": "reference", "author": "Nigel Rees", "title": "Sayings of the Century", "price": 8.95 }, { "category": "fiction", "author": "Evelyn Waugh", "title": "Sword of Honour", "price": 12.99 }, { "category": "fiction", "author": "Herman Melville", "title": "Moby Dick", "isbn": "0-553-21311-3", "price": 8.99 }, { "category": "fiction", "author": "J. R. R. Tolkien", "title": "The Lord of the Rings", "isbn": "0-395-19395-8", "price": 22.99 }], "pencil":{ "color": "red", "price": 19.95 }}');

select * from jspath;

select json.filter(js,'.book') from jspath;
select json.filter(js,'.pencil') from jspath;
select json.filter(js,'pencil') from jspath;
select json.filter(js,'..author') from jspath;
select json.filter(js,'..category') from jspath;

select json.filter(js,'.book.[0]') from jspath;
select json.filter(js,'.book.[1]') from jspath;
select json.filter(js,'.book.[2]') from jspath;
select json.filter(js,'.book.[3]') from jspath;
select json.filter(js,'.book.[*].category') from jspath;

declare s json;
set s = '[[{"name":"john"}], {"name":"mary"}]';
select json.filter(s,'..name');
select  json.filter(s,'$.[*].name');
select  json.filter(s,'..name[0]');
select  json.filter(s,'..name[1]');

select json.filter(js,'.book[-1]') from jspath;
select json.filter(js,'.book[4]') from jspath;
select json.filter(js,'$$$') from jspath;
select json.filter(js,'...') from jspath;
select json.filter(js,'[[2]]') from jspath;

drop table jspath;

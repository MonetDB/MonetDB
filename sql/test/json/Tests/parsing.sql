create table jsonparse(j json);

insert into jsonparse values('{}');
insert into jsonparse values('{}  ');
insert into jsonparse values('[]');
insert into jsonparse values('{"null": null}');
insert into jsonparse values('{"f1":1,"f2":2} ');
insert into jsonparse values('[1,2,null,true,false]');
insert into jsonparse values('[1,"hello",2]');

select * from jsonparse;

-- some errors
insert into jsonparse values('{');
insert into jsonparse values('}');
insert into jsonparse values('{}  k');
insert into jsonparse values('{:1}');
insert into jsonparse values('{"k"}');
insert into jsonparse values('{[}]');
insert into jsonparse values('{} }');
insert into jsonparse values('{} }[1]');
insert into jsonparse values('{"f1"::1}');
insert into jsonparse values('{"f1":1,"f2":2 ');

drop table jsonparse;

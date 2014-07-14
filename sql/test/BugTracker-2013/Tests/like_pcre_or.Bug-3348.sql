create table x (a CLOB);
insert into x VALUES ('aapX');
insert into x VALUES ('abc|aapX');
insert into x VALUES ('abc|aap_beer');

select * from x where a like 'abc|aap_%';
select * from x where a like 'abc|aap_%' escape '_';
select * from x where a like 'abc|aap__%' escape '_';
select * from x where a like 'abc|aap!_%' escape '!';

drop table x;

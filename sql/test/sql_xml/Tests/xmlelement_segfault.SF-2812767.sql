start transaction;
create table test (
x varchar(64)
);
select xmlelement(name a, xmlagg(xmlelement(name b, x))) from test;
rollback;

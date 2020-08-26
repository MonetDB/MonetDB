plan select 1 from (values ('one'), ('two')) as l(s), (values ('three'), ('four')) as r(s) where l.s like r.s;
explain select 1 from (values ('one'), ('two')) as l(s), (values ('three'), ('four')) as r(s) where l.s like r.s;
select 1 from (values ('one'), ('two')) as l(s), (values ('three'), ('four')) as r(s) where l.s like r.s;

set optimizer='minimal_pipe';
drop table x;

select 'transient', count(*) from bbp() as bbp where kind like 'tran%';
select 'persistent', count(*) from bbp() as bbp where kind like 'pers%';

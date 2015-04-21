set optimizer='minimal_pipe';
create table x  ( i int, j int );

select 'transient', count(*) from bbp() as bbp where kind like 'tran%';
select 'persistent', count(*) from bbp() as bbp where kind like 'pers%';

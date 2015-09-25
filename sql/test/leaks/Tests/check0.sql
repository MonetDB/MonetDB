set optimizer='minimal_pipe';

select ttype, count from bbp() as bbp 
where kind like 'pers%'
order by ttype, count;

select 'transient', count(*) from bbp() as bbp where kind like 'tran%';
select 'persistent', count(*) from bbp() as bbp where kind like 'pers%';

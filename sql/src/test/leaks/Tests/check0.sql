
select htype, ttype, count from bbp() as bbp 
where kind like 'pers%'
order by htype, ttype, count;

select 'transient', count(*) from bbp() as bbp where kind like 'tran%';
select 'persistent', count(*) from bbp() as bbp where kind like 'pers%';

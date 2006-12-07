
select htype, ttype, count from bbp() as bbp 
where kind = 'pers'
order by htype, ttype, count;

select 'transient', count(*) from bbp() as bbp where kind = 'tran';
select 'persistent', count(*) from bbp() as bbp where kind = 'pers';

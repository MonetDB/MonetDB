START TRANSACTION;
create table turl (val url);
insert into turl values ('https://me@www.monetdb.org:458/Doc/Abc.html?lang=nl&sort=asc#example');
insert into turl values (null);
select * from turl;

select val, sys.getanchor(val) from turl;
select val, sys.getbasename(val) from turl;
select val, sys.getcontext(val) from turl;
select val, sys.getdomain(val) from turl;
select val, sys.getextension(val) from turl;
select val, sys.getfile(val) from turl;
select val, sys.gethost(val) from turl;
select val, sys.getport(val) from turl;
select val, sys.getprotocol(val) from turl;
select val, sys.getquery(val) from turl;
select val, sys.getroboturl(val) from turl;
select val, sys.getuser(val) from turl;

ROLLBACK;

-- following gave a crash when constants optimizer was set.

set optimizer='dictionary_pipe';
select optimizer;
create table x( id serial, a varchar(1000));
drop table x;

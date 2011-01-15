create table get_results (schem string, clust string, real_time bigint);
create procedure save_times (schem string, clust string)
BEGIN
insert into get_results select
schem ,
clust ,
id as real_time
from columns order by id limit 1;
END;

call save_times('triples', 'pso');

select * from get_results;

drop ALL procedure save_times;
drop table get_results;

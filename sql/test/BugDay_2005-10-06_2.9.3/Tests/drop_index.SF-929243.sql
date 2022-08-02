create table t_dropindex(i int);
create index idx_dropindex on t_dropindex(i);
drop index idx_dropindex;

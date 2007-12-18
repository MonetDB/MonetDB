create table atacc1 (test int not null);
alter table atacc1 add constraint "atacc1_pkey" primary key (test);
alter table atacc1 drop constraint "atacc1_pkey";

drop table atacc1;

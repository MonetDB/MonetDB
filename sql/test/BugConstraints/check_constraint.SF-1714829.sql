--[ 1714829 ] CHECK CONSTRAINT...
create table t1 (id int CHECK (f1() = 1));

drop table t1;


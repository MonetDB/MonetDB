start transaction;
create table t3 (id int auto_increment ,text varchar(8) ,primary key (id));
create table t2 (id int auto_increment ,ds int not null ,ra double not null ,primary key (id) ,foreign key (ds) references t3 (id)) ;
create table t1 (id int auto_increment ,runcat int ,ds int not null ,ra double default 0 ,primary key (id) ,foreign key (runcat) references t2 (id) ,foreign key (ds) references t3 (id)) ;
insert into t3 (text) values ('test');
insert into t2 (ds,ra) select id,20 from t3;
--This one does not work:
insert into t1 (runcat,ds,ra) select id,ds,0 from t2;
--This one does work:
insert into t1 (runcat,ds,ra) select id,1,0 from t2;
rollback;

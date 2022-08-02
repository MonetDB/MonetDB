
start transaction;
create table t1 (
	  a int primary key
);

create table t2 (
	  b int primary key
);

create table t3 (
	  a int not null references t1 (a),
	  b int not null references t2 (b)
);

insert into t1 values (1);
insert into t2 values (1);
insert into t3 (b) values (1);

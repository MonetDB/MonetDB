create table facts (a_id bigint, b_id bigint);
insert into facts values(1,1);
insert into facts values(2,1);
insert into facts values(3,1);
insert into facts values(4,1);
insert into facts values(5,1);
insert into facts values(6,1);
insert into facts values(1,2);
insert into facts values(2,2);
insert into facts values(3,2);
insert into facts values(4,2);
insert into facts values(5,2);
insert into facts values(6,2);
insert into facts values(1,3);
insert into facts values(2,3);
insert into facts values(3,3);
insert into facts values(4,3);
insert into facts values(5,3);
insert into facts values(6,3);

create table a (id bigint not null primary key, c_id bigint);
insert into a values(1,1);
insert into a values(2,1);
insert into a values(3,2);
insert into a values(4,2);
insert into a values(5,3);
insert into a values(6,3);

create table b (id bigint not null primary key, name varchar(20));
insert into b values(1,'b1');
insert into b values(2,'b2');
insert into b values(3,'b3');

create table c (id bigint not null primary key, name varchar(20));
insert into c values(1,'c1');
insert into c values(2,'c2');
insert into c values(3,'c3');

-- produces a wrong result:
select b.name, c.id, c.name from facts left join a 
    on a_id = a.id 
	left join b 
		on b_id = b.id 
		left join c 
		on c_id = c.id 
group by b.name, c.name, c.id
order by b.name, c.id, c.name;

alter table c drop constraint c_id_pkey;

-- produces correct result:
select b.name, c.id, c.name from facts left join a on a_id = a.id left join b
on b_id = b.id left join c on c_id = c.id group by b.name, c.name, c.id
order by b.name, c.id, c.name;

drop table facts;
drop table a;
drop table b;
drop table c;

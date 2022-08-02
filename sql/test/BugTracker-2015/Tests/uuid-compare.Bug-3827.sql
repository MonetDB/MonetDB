create table u (u1 uuid, u2 uuid);

select * from u where u1 = u2;
select * from u where u1 <> u2;
select * from u where u1 < u2;
select * from u where u1 <= u2;
select * from u where u1 > u2;
select * from u where u1 >= u2;

drop table u;

start transaction;

create table t_qh ( c_f INTEGER , c_y2 INTEGER , c_i768 INTEGER , c_tqx TEXT , primary key(c_f, c_y2), unique(c_y2) );
insert into t_qh values (1,1,1,'a'), (2,2,2,'b'), (3,3,3,'c');

select ref_1.c_i768 as c0 from t_qh as ref_1 cross join (select ref_2.c_i768 as c0 from t_qh as ref_2 inner join t_qh as
ref_3 on (1=1) where ref_3.c_f <> ref_3.c_y2) as subq_0 where ref_1.c_y2 < ( select ref_1.c_f as c0 from t_qh as ref_4
where (EXISTS ( select distinct ref_5.c_i768 as c0 from t_qh as ref_5)) and (ref_1.c_i768 between ref_4.c_y2 and ref_1.c_y2));
	-- empty

rollback;

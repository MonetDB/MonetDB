start transaction;
create table monet1_test ("Id" bigserial, key int, value varchar(2000));
create table monet2_test ("Id" bigserial, key int, value varchar(2000));

insert into monet1_test select value "Id", value "key", value "value" from generate_series(1,600);
insert into monet2_test select value "Id", value "key", value "value" from generate_series(1,200);

select count(1) from monet2_test t1 inner join monet1_test t2 on exists (select t1.key intersect select t2.key);
select count(1) from monet2_test where exists(select 1 from monet1_test t2 where (t2.key = monet2_test.key) or ((t2.key is null) and (monet2_test.key is null)));
select count(1) from monet2_test t1 inner join monet1_test t2 on ((t1.key = t2.key) or ((t1.key is null) and (t2.key is null)));

MERGE INTO monet2_test
USING monet1_test AS t_1
	ON ("monet2_test"."key" = t_1."key" OR ("monet2_test"."key" IS NULL AND t_1."key" IS NULL))
	WHEN MATCHED THEN
	UPDATE SET "key" = t_1."key", "value" = t_1."value"
	WHEN NOT MATCHED THEN
	INSERT ("Id", "key", "value")
	VALUES (t_1."Id", t_1."key", t_1."value");
rollback;

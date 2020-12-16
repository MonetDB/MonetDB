create table "test2831994" (   
        "id" bigint,
        "value" DECIMAL(5,5),
        "value2" DECIMAL (5, 5)
);
prepare insert into "test2831994"("id","value","value2") values (?, ?, ?);
exec ** (3,0.0,2.34);
select * from test2831994;
drop table test2831994;

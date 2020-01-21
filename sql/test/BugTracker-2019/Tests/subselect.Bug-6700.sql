start transaction;

create table "testTable1" (
	"A" varchar(255),
	"B" varchar(255)
);
insert into "testTable1" values ('Cat1', 'Cat1');
insert into "testTable1" values ('Cat2', 'Cat2');
insert into "testTable1" values ('Cat3', 'Cat1');



create table "testTable2" (
	"A" varchar (255),
	"B" double
);
insert into "testTable2" values ('Cat1', 2);
insert into "testTable2" values ('Cat2', 3);
insert into "testTable2" values ('Cat2', 4);
insert into "testTable2" values (null, null);


select "A", "B", (
      select count(1)
      from "testTable1" "inner"
      where ("inner"."B" = "outer"."A")
) from "testTable1" "outer";
select "A", "B", (
      select count(*)
      from "testTable1" "inner"
      where ("inner"."B" = "outer"."A")
) from "testTable1" "outer";


select "A", "B", (
	select count(1)
	from "testTable1" "inner"
	where ("inner"."B" = "outer"."A") and ("outer"."A" is not null)
) from "testTable1" "outer";
select "A", "B", (
	select count(*)
	from "testTable1" "inner"
	where ("inner"."B" = "outer"."A") and ("outer"."A" is not null)
) from "testTable1" "outer";


select "A", "B", (
	select sum("B") 
	from "testTable2" "inner"
	where (
         "inner"."A" = "outer"."A" or 
         ("inner"."A" is null and  "outer"."A" is null)
      )
) from "testTable2" "outer";


select "A", "B", (
	select sum("B") 
	from "testTable2" "inner"
	where (
         "inner"."A" = "outer"."A" or 
         ("inner"."A" is null and  "outer"."A" is null)
      ) and ("A" = 'Cat7')
) from "testTable2" "outer";


select "A", "B", (
	select sum("B") 
	from "testTable2" "inner"
	where (
         "inner"."A" = "outer"."A" or 
         ("inner"."A" is null and  "outer"."A" is null)
      ) and (true = false)
) from "testTable2" "outer";


rollback;


create table "x"("id" int not null primary key);
create table "y"("id" int not null primary key,"x_id" int,constraint
"x_id_refs_id" FOREIGN KEY("x_id") references "x"("id"));

create index "y_x_id" ON "y"("x_id");
insert into "x" VALUES(1),(2),(3);
insert into "y" VALUES(100,1),(101,2);
update y SET x_id = 3 WHERE id = 101;

drop table "y";
drop table "x";

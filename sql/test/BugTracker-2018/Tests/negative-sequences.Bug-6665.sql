start transaction;
create sequence "other_seq" as integer start with -300 increment by -20 minvalue -580 maxvalue -300;
select get_value_for('sys', 'other_seq');
select next_value_for('sys', 'other_seq');
select next value for "other_seq";

create table "testme" ("col1" int default next value for "other_seq", "col2" int);
insert into "testme" ("col2") values (1);
select get_value_for('sys', 'other_seq');
select next_value_for('sys', 'other_seq');
select next value for "other_seq";

alter sequence "other_seq" restart with (select -400);
select get_value_for('sys', 'other_seq');
select next_value_for('sys', 'other_seq');
select next value for "other_seq";

insert into "testme"("col2") values (2);
select "col1", "col2" from "testme";
select get_value_for('sys', 'other_seq');
select next_value_for('sys', 'other_seq');
select next value for "other_seq";

insert into "testme"("col2") values (3), (4), (5), (6);
select "col1", "col2" from "testme";
select get_value_for('sys', 'other_seq');
select next_value_for('sys', 'other_seq');
select next value for "other_seq";

rollback;

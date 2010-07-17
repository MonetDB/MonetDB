set optimizer = 'minimal_pipe';
create table oblo (a int);
insert into oblo values (4);
insert into oblo values (3);
insert into oblo values (2);
insert into oblo values (1);

   PLAN select * from oblo;
   PLAN select * from oblo OFFSET 2;
   PLAN select * from oblo LIMIT 2;
   PLAN select * from oblo LIMIT 1 OFFSET 2;
   PLAN select * from oblo LIMIT 2 OFFSET 1;
   PLAN select * from oblo ORDER BY a;
   PLAN select * from oblo ORDER BY a OFFSET 2;
   PLAN select * from oblo ORDER BY a LIMIT 2;
   PLAN select * from oblo ORDER BY a LIMIT 2 OFFSET 1;
   PLAN select * from oblo ORDER BY a LIMIT 1 OFFSET 2;

EXPLAIN select * from oblo;
EXPLAIN select * from oblo OFFSET 2;
EXPLAIN select * from oblo LIMIT 2;
EXPLAIN select * from oblo LIMIT 1 OFFSET 2;
EXPLAIN select * from oblo LIMIT 2 OFFSET 1;
EXPLAIN select * from oblo ORDER BY a;
EXPLAIN select * from oblo ORDER BY a OFFSET 2;
EXPLAIN select * from oblo ORDER BY a LIMIT 2;
EXPLAIN select * from oblo ORDER BY a LIMIT 2 OFFSET 1;
EXPLAIN select * from oblo ORDER BY a LIMIT 1 OFFSET 2;

        select * from oblo;
        select * from oblo OFFSET 2;
        select * from oblo LIMIT 2;
        select * from oblo LIMIT 1 OFFSET 2;
        select * from oblo LIMIT 2 OFFSET 1;
        select * from oblo ORDER BY a;
        select * from oblo ORDER BY a OFFSET 2;
        select * from oblo ORDER BY a LIMIT 2;
        select * from oblo ORDER BY a LIMIT 2 OFFSET 1;
        select * from oblo ORDER BY a LIMIT 1 OFFSET 2;

drop table oblo;

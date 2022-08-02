START TRANSACTION;
CREATE TABLE "sys"."unitTestDontDelete" ("A" VARCHAR(255),"B" BIGINT,"C" DOUBLE,"D" TIMESTAMP);
INSERT INTO "sys"."unitTestDontDelete" VALUES (NULL, NULL, NULL, NULL), ('Cat1', 0, 0.5, '2013-06-10 11:10:10.000000'),
('Cat2', 1, 1.5, '2013-06-11 12:11:11.000000'), ('Cat1', 2, 2.5, '2013-06-12 13:12:12.000000'),
('Cat2', 3, 3.5, '2013-06-13 14:13:13.000000'), ('Cat1', 4, 4.5, '2013-06-14 15:14:14.000000'),
('Cat2', 5, 5.5, '2013-06-15 16:15:15.000000'), ('Cat1', 6, 6.5, '2013-06-16 17:16:16.000000'),
('Cat2', 7, 7.5, '2013-06-17 18:17:17.000000'), ('Cat1', 8, 8.5, '2013-06-18 19:18:18.000000');
with "c3_t" as
(
  select
    "c1_t"."A",
    "c1_t"."B",
    "c1_t"."C",
    "c1_t"."D"
  from
    "unitTestDontDelete" as "c1_t"
)
select
  "c3_t"."C",
  (
    select
      count(*)
    from
      "c3_t" as "c4_t"
    where
      "c4_t"."C" >= coalesce("c3_t"."C", 0.0) + 1
  )
  as "c2_f1"
from
  "c3_t";
ROLLBACK;

create table "foo"
(
"key_var" int,
"value_var2" int
);

create table "bar"
(
"key_var" int,
"value_var1" int
);

insert into "foo" values (1, 630);
insert into "foo" values (2, 420);
insert into "bar" values (1, 11);
insert into "bar" values (2, 27);

SELECT t1.key_var, t1.value_var2, t2.key_var, t2.value_var1
FROM 
(
  SELECT t1."key_var" AS "key_var",
  SUM(t2."value_var1") AS "value_var1"
  FROM "bar" t2
  RIGHT OUTER JOIN 
  (
    SELECT t2."key_var" AS "key_var"
    FROM "foo" t2
    GROUP BY t2."key_var"
  ) t1
  ON t1."key_var" = t2."key_var"
  GROUP BY t1."key_var"
) t2
INNER JOIN 
(
  SELECT t3."key_var" AS "key_var",
  SUM(t3."value_var2") AS "value_var2"
  FROM "foo" t3
  GROUP BY t3."key_var"
) t1
ON t2."key_var" = t1."key_var";

DROP TABLE "foo";
DROP TABLE "bar";

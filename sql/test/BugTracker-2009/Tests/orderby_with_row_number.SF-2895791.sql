create table "table1" ("customer" varchar(40), "product" varchar(40), "price" double);
insert into "table1" values ('cust1', 'p1', 100);
insert into "table1" values ('cust1', 'p2', 200);
insert into "table1" values ('cust1', 'p3', 150);
insert into "table1" values ('cust2', 'p1', 300);
insert into "table1" values ('cust2', 'p3', 200);

SELECT "customer", "product", "sumprice", (Row_number() OVER(PARTITION BY "customer" ORDER BY "sumprice")) as "rank" FROM ( SELECT "customer", "product", (Sum("price")) AS "sumprice" FROM "table1" GROUP BY "customer", "product") AS "temp";

/*
Customer product sumprice rank
Cust1 p1 100 1
Cust1 p2 200 2
Cust1 p3 150 3
Cust2 p1 300 1
Cust2 p3 200 2

But it should return:
Customer product sumprice rank
Cust1 p1 100 1
Cust1 p3 150 2
Cust1 p2 200 3
Cust2 p3 200 1
Cust2 p1 200 2 
*/

DROP TABLE "table1";

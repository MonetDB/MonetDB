create table aaa1 (a varchar(100), b varchar(100), c int);
create table aaa2 (a varchar(100), b varchar(100), c decimal(9,6));

insert into aaa1 values ('a1aaaaaaaaaaaaaaaaa', 'b1bbbbbbbbbbbbbbbbbb', 100);
insert into aaa1 values ('a2aaaaaaaaaaaaaaaaa', 'b2bbbbbbbbbbbbbbbbbb', 100);
insert into aaa1 values ('a3aaaaaaaaaaaaaaaaa', 'b3bbbbbbbbbbbbbbbbbb', 100);

insert into aaa2 values ('a1aaaaaaaaaaaaaaaaa', 'b1bbbbbbbbbbbbbbbbbb', 100);
insert into aaa2 values ('a2aaaaaaaaaaaaaaaaa', 'b2bbbbbbbbbbbbbbbbbb', 100);
insert into aaa2 values ('a3aaaaaaaaaaaaaaaaa', 'b3bbbbbbbbbbbbbbbbbb', 100);

SELECT a, CASE WHEN c >=100.000000
            AND c <=200.000000 THEN 'IntValue1' WHEN c>=200.000000
            AND c <=300.000000 THEN 'IntValue2' ELSE 'Out of range' END AS
CaseSentence
FROM aaa1
WHERE
(
   a = 'aaa'
   and
   (
      (
         CASE WHEN c>=100.000000
         AND c<=200.000000 THEN 'IntValue1' WHEN c>=200.000000
         AND c<=300.000000 THEN 'IntValue2' ELSE 'Out of range' END
      )  = 'IntValue1'
   )
)
or
(
   a = 'bbb'
   and
   (
      (
         CASE WHEN c>=100.000000
         AND c<=200.000000 THEN 'IntValue1' WHEN c>=200.000000
         AND c<=300.000000 THEN 'IntValue2' ELSE 'Out of range' END
      )  = 'IntValue2'
   )
);

-- an explicit cast function
SELECT a, CASE WHEN cast (c as double) >=100.000000
            AND cast(c as double) <=200.000000 THEN 'IntValue1' WHEN cast(c as double)>=200.000000
            AND cast(c as double) <=300.000000 THEN 'IntValue2' ELSE 'Out of range' END AS
CaseSentence
FROM aaa1
WHERE
(
   a = 'aaa'
   and
   (
      (
         CASE WHEN cast(c as double)>=100.000000
         AND cast(c as double)<=200.000000 THEN 'IntValue1' WHEN cast(c as double)>=200.000000
         AND cast(c as double)<=300.000000 THEN 'IntValue2' ELSE 'Out of range' END
      )  = 'IntValue1'
   )
)
or
(
   a = 'bbb'
   and
   (
      (
         CASE WHEN cast(c as double)>=100.000000
         AND cast(c as double)<=200.000000 THEN 'IntValue1' WHEN cast(c as double)>=200.000000
         AND cast(c as double)<=300.000000 THEN 'IntValue2' ELSE 'Out of range' END
      )  = 'IntValue2'
   )
);



-- the decimal case version
SELECT a, CASE WHEN c >=100.000000
            AND c <=200.000000 THEN 'IntValue1' WHEN c >=200.000000
            AND c <=300.000000 THEN 'IntValue2' ELSE 'Out of range' END AS
CaseSentence
FROM aaa2
WHERE
(
   a = 'aaa'
   and
   (
      (
         CASE WHEN c >=100.000000
         AND c <=200.000000 THEN 'IntValue1' WHEN c >=200.000000
         AND c <=300.000000 THEN 'IntValue2' ELSE 'Out of range' END
      )  = 'IntValue1'
   )
)
or
(
   a = 'bbb'
   and
   (
      (
         CASE WHEN c >=100.000000
         AND c <=200.000000 THEN 'IntValue1' WHEN c >=200.000000
         AND c <=300.000000 THEN 'IntValue2' ELSE 'Out of range' END
      )  = 'IntValue2'
   )
);

-- The double version
SELECT a, CASE WHEN c >=100.000000
            AND c <=200.000000 THEN 'IntValue1' WHEN c>=200.000000
            AND c <=300.000000 THEN 'IntValue2' ELSE 'Out of range' END AS
CaseSentence
FROM aaa1
WHERE
(
   a = 'aaa'
   and
   (
      (
         CASE WHEN c>=100.000000
         AND c<=200.000000 THEN 'IntValue1' WHEN c>=200.000000
         AND c<=300.000000 THEN 'IntValue2' ELSE 'Out of range' END
      )  = 'IntValue1'
   )
)
or
(
   a = 'bbb'
   and
   (
      (
         CASE WHEN c>=100.000000
         AND c<=200.000000 THEN 'IntValue1' WHEN c>=200.000000
         AND c<=300.000000 THEN 'IntValue2' ELSE 'Out of range' END
      )  = 'IntValue2'
   )
);

drop table aaa1;
drop table aaa2;


START TRANSACTION;
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);

select
          subq_0.c0 as c1
        from
          (select
                ref_0.col0 as c0,
                39 as c1
              from
                tab0 as ref_0
              ) as subq_0
        where subq_0.c1 is NULL; --empty

ROLLBACK;

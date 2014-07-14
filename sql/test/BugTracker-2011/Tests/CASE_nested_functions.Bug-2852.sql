CREATE TABLE t2852 (c INT);
INSERT INTO t2852 VALUES (0);
SELECT
        COALESCE(
            COALESCE("c"  /
              CASE 
              WHEN COALESCE("c" , 0)  = 0 
              THEN
              NULL
              END
            , 0)  - COALESCE("c"  /
                      CASE
                      WHEN "c"  = 0 
                      THEN
                      NULL
                      END
            ,0)
        , 0)
    FROM
      t2852
  ;
DROP TABLE t2852;

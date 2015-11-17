CREATE TABLE foo (
        test1     VARCHAR(48),
        test2     VARCHAR(65535) DEFAULT '',
        test3     VARCHAR(32),
        test4     INTEGER,
        test5 INTEGER
);
 
ALTER TABLE foo ADD COLUMN test6 BOOLEAN;
ALTER TABLE foo ADD COLUMN test7 BOOLEAN;
ALTER TABLE foo ADD COLUMN test8 BOOLEAN;
 
UPDATE foo SET test5 = ROW_NUMBER() OVER (  
   PARTITION BY test1   
   ORDER BY test4  ASC,      
      test6 DESC,      
      test8 DESC,      
      test7 ASC,      
      test3 ASC);

DROP TABLE foo;

CREATE TABLE foo (col INTEGER);
UPDATE foo SET col = ROW_NUMBER() OVER (ORDER BY col);
DROP TABLE foo;

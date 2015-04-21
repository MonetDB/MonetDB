START TRANSACTION;
CREATE TABLE rapi11t (geneid INTEGER,patientid INTEGER,expr_value REAL);
INSERT INTO rapi11t VALUES
(0,0,791922.5),
(0,1,1103684),
(0,2,1956211.62),
(0,3,1073513.12),
(0,4,936948.25),
(0,5,-29069.5605),
(0,6,265815.531),
(0,7,1303931.12),
(0,8,890202.375),
(0,9,1068610.88),
(0,10,1474552.62),
(0,11,-140521.688),
(0,12,105205.953),
(0,13,1233838),
(0,14,874556.875),
(0,15,1006251.25),
(0,16,1744377),
(0,17,1349524),
(0,18,293184.375),
(0,19,891154.75);


CREATE FUNCTION rapi11f(i1 int, i2 int, i3 real) RETURNS TABLE(row_num int, col_num int, val float)  LANGUAGE R {
    library(reshape2)
    x <- data.frame(i1,i2,i3)
    A <- acast(x, list(names(x)[1], names(x)[2]))
    S <- cov(A)
    melt(S)
};

SELECT rapi11f(geneid,patientid,expr_value) FROM rapi11t;

DROP FUNCTION rapi11f;
DROP TABLE rapi11t;

ROLLBACK;

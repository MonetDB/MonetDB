START TRANSACTION;

CREATE TABLE testTableDates (d DATE);
INSERT INTO testTableDates(d) VALUES ('2019-01-01'),('2019-01-02'),('2019-01-03');
CREATE FUNCTION testDateLoopback(din DATE) RETURNS TABLE (dout DATE) LANGUAGE R {
   data.frame(dout=din)
};
SELECT * FROM testDateLoopback( (SELECT * FROM testTableDates) );

ROLLBACK;

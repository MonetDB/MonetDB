CREATE TABLE payments(payment_date DATE, value DECIMAL(7, 2), state CHAR(2));
INSERT INTO payments VALUES ('2011-01-01', 123.45, 'DF');
INSERT INTO payments VALUES ('2011-01-02', 12.33, 'DF');

CREATE FUNCTION amostra(pdate date, st char(2)) RETURNS TABLE(v decimal(7, 2))
BEGIN
   RETURN SELECT value FROM payments WHERE payment_date = pdate AND state = st SAMPLE 400;
END;

SELECT avg(v) FROM amostra('2011-01-01', 'DF');

DROP FUNCTION amostra;
DROP TABLE payments;


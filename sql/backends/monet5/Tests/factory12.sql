--Factory returning table with more than 1 column
CREATE FUNCTION factory12() RETURNS TABLE (aa CLOB, bb DATE) BEGIN
    YIELD TABLE (SELECT 'aa', cast('2015-01-01' AS DATE));
    YIELD TABLE (SELECT 'bb', cast('2016-02-02' AS DATE));
    YIELD TABLE (SELECT 'cc', cast('2017-03-03' AS DATE));
END;

SELECT * FROM factory12();
SELECT * FROM factory12();
SELECT * FROM factory12();

DROP FUNCTION factory12;

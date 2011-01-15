
CREATE FUNCTION fGetDiagChecksum()
RETURNS BIGINT
BEGIN
RETURN (select sum(id)+count(*) from tables);
END;

DROP FUNCTION fGetDiagChecksum;

CREATE FUNCTION fDocColumns(tabname varchar(400))
RETURNS TABLE (enum varchar(64))
BEGIN
    RETURN TABLE( select t.name from sys.tables t where name='romulo');
END; 

drop function fDocColumns;

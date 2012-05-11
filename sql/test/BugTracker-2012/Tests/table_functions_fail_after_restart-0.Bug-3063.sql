CREATE schema ft;
CREATE FUNCTION ft.func()
RETURNS TABLE (sch varchar(100))
RETURN TABLE (SELECT s.name from sys.schemas as s);

select * from ft.func() as ftf;

CREATE function table_id( tname char ) RETURNS INT AS 
	SELECT id from tables where name = tname;

SELECT table_id('tables');
SELECT * from columns where table_id = table_id('tables');

commit;

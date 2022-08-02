CREATE FUNCTION demof1(fin int) RETURNS TABLE (fout int) BEGIN RETURN TABLE(SELECT fin); END;
CREATE FUNCTION demof(fin clob) RETURNS TABLE (fout clob) BEGIN RETURN TABLE(SELECT fin); END;

DROP FUNCTION demof1(int);
SELECT * FROM demof1(1); -- should fail
DROP FUNCTION IF EXISTS demof1(int);

DROP FUNCTION IF EXISTS demof(clob);
DROP FUNCTION demof(clob); -- should fail
SELECT * FROM demof('test'); -- should fail

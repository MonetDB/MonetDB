-- for comparison
CREATE TEMPORARY TABLE orig AS SELECT * FROM sys.comments;

-- Should fail with "!symbol type not found".
-- This means that the parser correctly parsed the statement
-- but the system was unable to handle the resulting AST.
COMMENT ON TABLE sys.comments IS 'For every catalog object, an optional remark';

SELECT remark FROM sys.comments EXCEPT SELECT remark FROM orig;

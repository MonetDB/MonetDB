-- for comparison
CREATE TEMPORARY TABLE orig AS SELECT * FROM sys.comments;

-- Should fail with ERROR = !F00BAR!COMMENT ON 7994
-- This indicates that it was able to resolve the table reference
-- but the rest of the work has not yet been implemented.
COMMENT ON TABLE sys.comments IS 'For every catalog object, an optional remark';

SELECT remark FROM sys.comments EXCEPT SELECT remark FROM orig;

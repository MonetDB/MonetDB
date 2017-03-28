-- for comparison
CREATE TEMPORARY TABLE orig AS SELECT * FROM sys.comments;

-- Should fail with "unexpected COMMENT", which proves that the
-- scanner is aware of the COMMENT keyword.  Otherwise the
-- error would read "unexpected IDENT".
COMMENT ON TABLE sys.comments IS 'For every catalog object, an optional remark';

SELECT remark FROM sys.comments EXCEPT SELECT remark FROM orig;

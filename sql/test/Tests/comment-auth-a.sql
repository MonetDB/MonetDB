
-- can we see the comments set by the super user?
\dn

-- we cannot change comments on objects we don't own
COMMENT ON SCHEMA schema_b IS 'set by user_a';

-- but we can comment on our own stuff
COMMENT ON SCHEMA schema_a IS 'set by user_a';
\dn

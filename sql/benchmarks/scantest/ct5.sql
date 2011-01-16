-- This script creates table t5
--
--  USAGE
--    SQL> @ct5
--

PROMPT Creating table t5

CREATE TABLE t5 (
    Id     NUMBER         NOT NULL,
    v1     FLOAT         NOT NULL,
    v2     FLOAT DEFAULT 1.1,
    v3     FLOAT DEFAULT 1.1,
    v4     FLOAT DEFAULT 1.1,
    v5     FLOAT DEFAULT 1.1
);


-- This script creates table t10
--
--  USAGE
--    SQL> @ct10
--

PROMPT Creating table t10

CREATE TABLE t10 (
    Id     NUMBER         NOT NULL,
    v1     FLOAT         NOT NULL,
    v2     FLOAT DEFAULT 1.1,
    v3     FLOAT DEFAULT 1.1,
    v4     FLOAT DEFAULT 1.1,
    v5     FLOAT DEFAULT 1.1,
    v6     FLOAT DEFAULT 1.1,
    v7     FLOAT DEFAULT 1.1,
    v8     FLOAT DEFAULT 1.1,
    v9     FLOAT DEFAULT 1.1,
    v10    FLOAT DEFAULT 1.1
);


START TRANSACTION;
CREATE TABLE user_record (
    name VARCHAR(64),
    uid VARCHAR(32) PRIMARY KEY
);
CREATE TABLE user_record_insertion (
    name VARCHAR(64),
    uid VARCHAR(32)
);
COMMIT;
START TRANSACTION;
COPY 1 RECORDS
    INTO user_record_insertion
    FROM STDIN
    USING DELIMITERS ',','\n';
Jane Doe,e37722e7
;
INSERT
    INTO user_record (name,uid)
    SELECT name,uid
        FROM user_record_insertion;
COMMIT;
DROP TABLE user_record_insertion;
DROP TABLE user_record;

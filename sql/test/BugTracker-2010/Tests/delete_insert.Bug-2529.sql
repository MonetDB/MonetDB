START TRANSACTION;
CREATE TABLE user_record (
	    name VARCHAR(64),
	    uid VARCHAR(32) PRIMARY KEY
);
CREATE TABLE user_record_insertion (
	    name VARCHAR(64),
	    uid VARCHAR(32)
);
CREATE TABLE user_record_insertion2 (
	    name VARCHAR(64),
	    uid VARCHAR(32)
);
COMMIT;
START TRANSACTION;
DELETE
    FROM user_record_insertion;
COPY 10 RECORDS INTO user_record_insertion FROM STDIN USING DELIMITERS ',','\n';
Steven Teague,d5329b8f
Chrystal Whitman,20cbc561
Elisabeth Luetten,4e0bfbea
Jimmy Roark,6b0e43bd
Vern Marrero,c2d113ad
Shelly Rankin,198118fe
Randall Kaiser,f76bfe86
Rusty Wuerzen,9c29633b
Wilfredo Rosenbaum,e5c469cb
Cecil Herrington,000c14c7

INSERT INTO user_record_insertion2 (name,uid)
SELECT name,uid FROM user_record_insertion;
INSERT INTO user_record (name,uid)
SELECT name,uid FROM user_record_insertion2;
COMMIT;
DROP TABLE user_record_insertion2;
DROP TABLE user_record_insertion;
DROP TABLE user_record;

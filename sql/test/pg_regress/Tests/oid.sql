--
-- OID
--

CREATE TABLE OID_TBL(f1 oid);

INSERT INTO OID_TBL(f1) VALUES ('1234');
INSERT INTO OID_TBL(f1) VALUES (1235);
INSERT INTO OID_TBL(f1) VALUES ('987');
INSERT INTO OID_TBL(f1) VALUES (12345678901);
INSERT INTO OID_TBL(f1) VALUES ('000');
INSERT INTO OID_TBL(f1) VALUES ('    ');  -- in MonetDB this one is accepted
-- leading/trailing hard tab is also allowed
INSERT INTO OID_TBL(f1) VALUES ('5     ');
INSERT INTO OID_TBL(f1) VALUES ('   10  ');
INSERT INTO OID_TBL(f1) VALUES ('	  15 	  ');
INSERT INTO OID_TBL(f1) VALUES (null);

SELECT '' AS ten, OID_TBL.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL;

-- bad inputs
INSERT INTO OID_TBL(f1) VALUES ('asdfasd');
INSERT INTO OID_TBL(f1) VALUES ('99asdfasd');
INSERT INTO OID_TBL(f1) VALUES ('5    d');
INSERT INTO OID_TBL(f1) VALUES ('    5d');
INSERT INTO OID_TBL(f1) VALUES ('5    5');
INSERT INTO OID_TBL(f1) VALUES (-10);   -- negative oids are not allowed in MonetDB, so this should fail
INSERT INTO OID_TBL(f1) VALUES (-1040);   -- negative oids are not allowed in MonetDB, so this should fail
INSERT INTO OID_TBL(f1) VALUES ('-1040');   -- negative oids are not allowed in MonetDB, so this should fail
INSERT INTO OID_TBL(f1) VALUES (' - 500');
INSERT INTO OID_TBL(f1) VALUES ('32958209582039852935');
INSERT INTO OID_TBL(f1) VALUES (32958209582039852935);
INSERT INTO OID_TBL(f1) VALUES ('-23582358720398502385');
INSERT INTO OID_TBL(f1) VALUES (-23582358720398502385);

SELECT '' AS ten, OID_TBL.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL ORDER BY	f1;

DELETE FROM OID_TBL WHERE f1 < '0';

SELECT '' AS one, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 = cast(1234 as oid);
SELECT '' AS one, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 = 1234@0;
SELECT '' AS one, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 = '1234';

SELECT '' AS seven, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 <> cast(1234 as oid);
SELECT '' AS seven, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 <> 1234@0;
SELECT '' AS seven, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 <> '1234';

SELECT '' AS six, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 <= cast(1234 as oid);
SELECT '' AS six, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 <= 1234@0;
SELECT '' AS six, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 <= '1234';

SELECT '' AS five, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 < cast(1234 as oid);
SELECT '' AS five, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 < 1234@0;
SELECT '' AS five, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 < '1234';

SELECT '' AS three, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 >= cast(1234 as oid);
SELECT '' AS three, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 >= 1234@0;
SELECT '' AS three, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 >= '1234';

SELECT '' AS two, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 > cast(1234 as oid);
SELECT '' AS two, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 > 1234@0;
SELECT '' AS two, o.*, cast(f1 as varchar(30)) as oid2str FROM OID_TBL o WHERE o.f1 > '1234';

DROP TABLE OID_TBL;

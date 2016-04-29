CREATE TABLE testUUID (s varchar(36), u UUID);
INSERT INTO testUUID (s, u) VALUES ('ad887b3d-08f7-c308-7285-354a1857cbc8', convert('ad887b3d-08f7-c308-7285-354a1857cbc8', uuid));
INSERT INTO testUUID (s, u) VALUES ('7393ad7e-4fcf-461a-856e-b70027fe1a9e', convert('7393ad7e-4fcf-461a-856e-b70027fe1a9e', uuid));
INSERT INTO testUUID (s, u) VALUES ('c005d6fd-20c3-4d01-91a5-bbe676593530', convert('c005d6fd-20c3-4d01-91a5-bbe676593530', uuid));
SELECT * FROM testUUID ORDER BY s;

SELECT s, u, isaUUID(s) as a_isa_UUID FROM testUUID ORDER BY s;
SELECT s, u, isaUUID(u) as u_isa_UUID FROM testUUID ORDER BY s;
SELECT * FROM testUUID WHERE isaUUID(s) = TRUE ORDER BY s;
SELECT * FROM testUUID WHERE isaUUID(u) = TRUE ORDER BY s;

SELECT MIN(u) AS mn, MAX(u) AS mx, COUNT(u) AS cnt, COUNT(DISTINCT u) AS cnt_d FROM testUUID;
SELECT SUM(u) AS sumu FROM testUUID;
SELECT AVG(u) AS sumu FROM testUUID;

DROP TABLE testUUID;


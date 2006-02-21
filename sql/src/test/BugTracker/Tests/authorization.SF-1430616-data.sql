CREATE USER "voc" WITH PASSWORD 'voc' NAME 'VOC Explorer' SCHEMA "sys";
select * from users;
CREATE SCHEMA "voc" AUTHORIZATION "voc";
ALTER USER "voc" SET SCHEMA "voc";

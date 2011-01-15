START TRANSACTION;

CREATE USER "voc2" WITH PASSWORD 'voc2' NAME 'VOC_EXPLORER' SCHEMA "sys";
CREATE SCHEMA "voc2" AUTHORIZATION "voc2";
ALTER USER "voc2" SET SCHEMA "voc2";

alter user "voc2" with password 'new';

commit;

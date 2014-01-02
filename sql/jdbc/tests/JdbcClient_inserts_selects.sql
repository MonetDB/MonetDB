-- The contents of this file are subject to the MonetDB Public License
-- Version 1.1 (the "License"); you may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
-- http://www.monetdb.org/Legal/MonetDBLicense
--
-- Software distributed under the License is distributed on an "AS IS"
-- basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
-- License for the specific language governing rights and limitations
-- under the License.
--
-- The Original Code is the MonetDB Database System.
--
-- The Initial Developer of the Original Code is CWI.
-- Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
-- Copyright August 2008-2014 MonetDB B.V.
-- All Rights Reserved.

START TRANSACTION;

INSERT INTO allnewtriples VALUES(1, 1, 1, 1, false);
INSERT INTO allnewtriples VALUES(2, 1, 1, 2, false);
INSERT INTO allnewtriples VALUES(3, 1, 2, 1, false);
INSERT INTO allnewtriples VALUES(4, 2, 1, 1, false);
INSERT INTO allnewtriples VALUES(5, 1, 2, 2, false);
INSERT INTO allnewtriples VALUES(6, 2, 2, 1, false);
INSERT INTO allnewtriples VALUES(7, 2, 2, 2, false);

INSERT INTO "foreign" VALUES(1, 1, 1, 1);
INSERT INTO "foreign" VALUES(2, 2, 2, 2);
INSERT INTO "foreign" VALUES(3, 1, 2, 2);
INSERT INTO "foreign" VALUES(4, 2, 2, 1);
INSERT INTO "foreign" VALUES(5, 2, 1, 1);
INSERT INTO "foreign" VALUES(6, 1, 2, 1);
INSERT INTO "foreign" VALUES(7, 1, 1, 2);

SELECT * FROM allnewtriples;

SELECT * FROM "foreign";

COMMIT;

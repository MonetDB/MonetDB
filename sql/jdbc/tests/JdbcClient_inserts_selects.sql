-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

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

-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

START TRANSACTION;

CREATE TABLE allnewtriples (
	id integer NOT NULL,
	subject integer NOT NULL,
	predicate integer NOT NULL,
	object integer NOT NULL,
	explicit boolean NOT NULL,
	PRIMARY KEY(id),
	CONSTRAINT unique_key UNIQUE(subject, predicate, object)
);
CREATE INDEX allnewtriples_subject_idx ON allnewtriples (subject);
CREATE INDEX allnewtriples_predicate_idx ON allnewtriples (predicate);
CREATE INDEX allnewtriples_object_idx ON allnewtriples (object);

CREATE TABLE "foreign" (
	id integer NOT NULL,
	subject integer NOT NULL,
	predicate integer NOT NULL,
	object integer NOT NULL,
	FOREIGN KEY (id) REFERENCES allnewtriples (id),
	FOREIGN KEY (subject, predicate, object) REFERENCES allnewtriples (subject, predicate, object)
);

CREATE TABLE "triples" (
	"id"        int NOT NULL,
	"subject"   int NOT NULL,
	"predicate" int NOT NULL,
	"object"    int NOT NULL,
	"explicit"  boolean     NOT NULL,
	CONSTRAINT "triples_subject_predicate_object_unique" UNIQUE ("subject", "predicate", "object")
);
CREATE INDEX "triples_object_idx" ON "triples" ("object");
CREATE INDEX "triples_predicate_idx" ON "triples" ("predicate");
CREATE INDEX "triples_predicate_object_idx" ON "triples" ("predicate", "object");
CREATE INDEX "triples_subject_idx" ON "triples" ("subject");
CREATE INDEX "triples_subject_object_idx" ON "triples" ("subject", "object");
CREATE INDEX "triples_subject_predicate_idx" ON "triples" ("subject", "predicate");


CREATE View   subject_stats as SELECT "subject",   CAST(COUNT(*) AS BIGINT) AS counts, MIN("subject")   AS min_value, MAX("subject")   AS max_value FROM "triples" GROUP BY "subject"   ORDER BY "subject";
CREATE View predicate_stats as SELECT "predicate", CAST(COUNT(*) AS BIGINT) AS counts, MIN("predicate") AS min_value, MAX("predicate") AS max_value FROM "triples" GROUP BY "predicate" ORDER BY "predicate";
CREATE OR REPLACE View object_stats as SELECT "object", CAST(COUNT(*) AS BIGINT) AS counts, MIN("object") AS min_value, MAX("object")  AS max_value FROM "triples" GROUP BY "object"    ORDER BY "object";


CREATE   MERGE TABLE mt    (id int primary key, nm varchar(123) NOT NULL);
CREATE  REMOTE TABLE remt  (id int primary key, nm varchar(123) NOT NULL) ON 'mapi:monetdb://localhost:42001/mdb3';
CREATE REPLICA TABLE replt (id int primary key, nm varchar(123) NOT NULL);

CREATE GLOBAL TEMP TABLE gtmpt (id int primary key, nm varchar(123) NOT NULL) ON COMMIT PRESERVE ROWS;

COMMIT;

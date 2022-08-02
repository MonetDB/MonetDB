START TRANSACTION;
CREATE TABLE "addedtriples" (
	"id"	mediumint	NOT NULL,
	"subject"	mediumint	NOT NULL,
	"predicate"	mediumint	NOT NULL,
	"object"	mediumint	NOT NULL,
	"explicit"	boolean	NOT NULL
);
CREATE INDEX "addedtriples_subject_predicate_object_idx" ON "addedtriples" ("subject", "predicate", "object");
CREATE TABLE "allinferred" (
	"id"	mediumint	NOT NULL,
	"subject"	mediumint	NOT NULL,
	"predicate"	mediumint	NOT NULL,
	"object"	mediumint	NOT NULL,
	"explicit"	boolean	NOT NULL
);
CREATE TABLE "allnewtriples" (
	"id"	mediumint	NOT NULL,
	"subject"	mediumint	NOT NULL,
	"predicate"	mediumint	NOT NULL,
	"object"	mediumint	NOT NULL,
	"explicit"	boolean	NOT NULL,
	UNIQUE ("subject", "predicate", "object") -- c1017043
);
CREATE INDEX "allnewtriples_object_idx" ON "allnewtriples" ("object");
CREATE INDEX "allnewtriples_predicate_idx" ON "allnewtriples" ("predicate");
CREATE INDEX "allnewtriples_subject_idx" ON "allnewtriples" ("subject");
INSERT INTO "allnewtriples" VALUES (1, 19, 20, 2, false);
INSERT INTO "allnewtriples" VALUES (2, 18, 20, 16, false);
INSERT INTO "allnewtriples" VALUES (3, 20, 20, 2, false);
INSERT INTO "allnewtriples" VALUES (4, 21, 20, 2, false);
INSERT INTO "allnewtriples" VALUES (5, 4, 20, 7, false);
INSERT INTO "allnewtriples" VALUES (6, 5, 20, 7, false);
INSERT INTO "allnewtriples" VALUES (7, 6, 20, 7, false);
INSERT INTO "allnewtriples" VALUES (8, 12, 20, 11, false);
INSERT INTO "allnewtriples" VALUES (9, 13, 20, 11, false);
INSERT INTO "allnewtriples" VALUES (10, 19, 21, 2, false);
INSERT INTO "allnewtriples" VALUES (11, 18, 21, 16, false);
INSERT INTO "allnewtriples" VALUES (12, 20, 21, 16, false);
INSERT INTO "allnewtriples" VALUES (13, 21, 21, 16, false);
INSERT INTO "allnewtriples" VALUES (14, 1, 21, 16, false);
INSERT INTO "allnewtriples" VALUES (15, 22, 21, 17, false);
INSERT INTO "allnewtriples" VALUES (16, 23, 21, 17, false);
INSERT INTO "allnewtriples" VALUES (17, 13, 21, 11, false);
INSERT INTO "allnewtriples" VALUES (18, 8, 18, 27, false);
INSERT INTO "allnewtriples" VALUES (19, 9, 18, 27, false);
INSERT INTO "allnewtriples" VALUES (20, 10, 18, 27, false);
INSERT INTO "allnewtriples" VALUES (21, 29, 18, 2, false);
INSERT INTO "allnewtriples" VALUES (22, 24, 19, 25, false);
INSERT INTO "allnewtriples" VALUES (23, 3, 1, 26, false);
INSERT INTO "allnewtriples" VALUES (24, 26, 18, 16, false);
INSERT INTO "allnewtriples" VALUES (25, 15, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (26, 17, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (27, 7, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (28, 14, 1, 11, false);
INSERT INTO "allnewtriples" VALUES (29, 4, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (30, 5, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (31, 6, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (32, 12, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (33, 13, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (34, 20, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (35, 21, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (36, 18, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (37, 19, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (38, 1, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (39, 22, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (40, 23, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (41, 8, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (42, 9, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (43, 10, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (44, 29, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (45, 24, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (46, 26, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (47, 2, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (48, 16, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (49, 11, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (50, 27, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (51, 25, 1, 2, false);
INSERT INTO "allnewtriples" VALUES (52, 19, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (53, 18, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (54, 20, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (55, 21, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (56, 4, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (57, 5, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (58, 6, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (59, 12, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (60, 13, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (61, 1, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (62, 22, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (63, 23, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (64, 8, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (65, 9, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (66, 10, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (67, 29, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (68, 24, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (69, 3, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (70, 26, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (71, 15, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (72, 17, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (73, 7, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (74, 14, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (75, 2, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (76, 16, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (77, 11, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (78, 27, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (79, 25, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (80, 4, 19, 4, false);
INSERT INTO "allnewtriples" VALUES (81, 5, 19, 5, false);
INSERT INTO "allnewtriples" VALUES (82, 6, 19, 6, false);
INSERT INTO "allnewtriples" VALUES (83, 12, 19, 12, false);
INSERT INTO "allnewtriples" VALUES (84, 13, 19, 13, false);
INSERT INTO "allnewtriples" VALUES (85, 15, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (86, 17, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (87, 7, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (88, 17, 18, 17, false);
INSERT INTO "allnewtriples" VALUES (89, 7, 18, 7, false);
INSERT INTO "allnewtriples" VALUES (90, 3, 1, 16, false);
INSERT INTO "allnewtriples" VALUES (91, 3, 18, 17, false);
INSERT INTO "allnewtriples" VALUES (92, 0, 1, 15, false);
INSERT INTO "allnewtriples" VALUES (93, 20, 19, 20, false);
INSERT INTO "allnewtriples" VALUES (94, 21, 19, 21, false);
INSERT INTO "allnewtriples" VALUES (95, 18, 19, 18, false);
INSERT INTO "allnewtriples" VALUES (96, 19, 19, 19, false);
INSERT INTO "allnewtriples" VALUES (97, 1, 19, 1, false);
INSERT INTO "allnewtriples" VALUES (98, 22, 19, 22, false);
INSERT INTO "allnewtriples" VALUES (99, 23, 19, 23, false);
INSERT INTO "allnewtriples" VALUES (100, 24, 19, 24, false);
INSERT INTO "allnewtriples" VALUES (101, 25, 19, 25, false);
INSERT INTO "allnewtriples" VALUES (102, 8, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (103, 9, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (104, 10, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (105, 29, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (106, 26, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (107, 2, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (108, 16, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (109, 11, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (110, 27, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (111, 3, 18, 15, false);
INSERT INTO "allnewtriples" VALUES (112, 8, 18, 8, false);
INSERT INTO "allnewtriples" VALUES (113, 9, 18, 9, false);
INSERT INTO "allnewtriples" VALUES (114, 10, 18, 10, false);
INSERT INTO "allnewtriples" VALUES (115, 29, 18, 29, false);
INSERT INTO "allnewtriples" VALUES (116, 26, 18, 26, false);
INSERT INTO "allnewtriples" VALUES (117, 2, 18, 2, false);
INSERT INTO "allnewtriples" VALUES (118, 16, 18, 16, false);
INSERT INTO "allnewtriples" VALUES (119, 11, 18, 11, false);
INSERT INTO "allnewtriples" VALUES (120, 27, 18, 27, false);
INSERT INTO "allnewtriples" VALUES (121, 3, 18, 3, false);
CREATE TABLE "class" (
	"id"	mediumint	NOT NULL,

	PRIMARY KEY ("id") -- c1012851
);
INSERT INTO "class" VALUES (15);
INSERT INTO "class" VALUES (17);
INSERT INTO "class" VALUES (7);
INSERT INTO "class" VALUES (8);
INSERT INTO "class" VALUES (9);
INSERT INTO "class" VALUES (10);
INSERT INTO "class" VALUES (29);
INSERT INTO "class" VALUES (26);
INSERT INTO "class" VALUES (2);
INSERT INTO "class" VALUES (16);
INSERT INTO "class" VALUES (11);
INSERT INTO "class" VALUES (27);
INSERT INTO "class" VALUES (3);
CREATE TABLE "depend" (
	"id"	mediumint	NOT NULL,
	"dep1"	mediumint	NOT NULL,
	"dep2"	mediumint	NOT NULL
);
CREATE INDEX "depend_dep1_dep2_idx" ON "depend" ("dep1", "dep2");
CREATE INDEX "depend_id_dep1_dep2_idx" ON "depend" ("id", "dep1", "dep2");
INSERT INTO "depend" VALUES (1, 0, 0);
INSERT INTO "depend" VALUES (2, 0, 0);
INSERT INTO "depend" VALUES (3, 0, 0);
INSERT INTO "depend" VALUES (4, 0, 0);
INSERT INTO "depend" VALUES (5, 0, 0);
INSERT INTO "depend" VALUES (6, 0, 0);
INSERT INTO "depend" VALUES (7, 0, 0);
INSERT INTO "depend" VALUES (8, 0, 0);
INSERT INTO "depend" VALUES (9, 0, 0);
INSERT INTO "depend" VALUES (10, 0, 0);
INSERT INTO "depend" VALUES (11, 0, 0);
INSERT INTO "depend" VALUES (12, 0, 0);
INSERT INTO "depend" VALUES (13, 0, 0);
INSERT INTO "depend" VALUES (14, 0, 0);
INSERT INTO "depend" VALUES (15, 0, 0);
INSERT INTO "depend" VALUES (16, 0, 0);
INSERT INTO "depend" VALUES (17, 0, 0);
INSERT INTO "depend" VALUES (18, 0, 0);
INSERT INTO "depend" VALUES (19, 0, 0);
INSERT INTO "depend" VALUES (20, 0, 0);
INSERT INTO "depend" VALUES (21, 0, 0);
INSERT INTO "depend" VALUES (22, 0, 0);
INSERT INTO "depend" VALUES (23, 0, 0);
INSERT INTO "depend" VALUES (24, 0, 0);
INSERT INTO "depend" VALUES (25, 0, 0);
INSERT INTO "depend" VALUES (26, 0, 0);
INSERT INTO "depend" VALUES (27, 0, 0);
INSERT INTO "depend" VALUES (28, 0, 0);
INSERT INTO "depend" VALUES (29, 0, 0);
INSERT INTO "depend" VALUES (30, 0, 0);
INSERT INTO "depend" VALUES (31, 0, 0);
INSERT INTO "depend" VALUES (32, 0, 0);
INSERT INTO "depend" VALUES (33, 0, 0);
INSERT INTO "depend" VALUES (34, 0, 0);
INSERT INTO "depend" VALUES (35, 0, 0);
INSERT INTO "depend" VALUES (36, 0, 0);
INSERT INTO "depend" VALUES (37, 0, 0);
INSERT INTO "depend" VALUES (38, 0, 0);
INSERT INTO "depend" VALUES (39, 0, 0);
INSERT INTO "depend" VALUES (40, 0, 0);
INSERT INTO "depend" VALUES (41, 0, 0);
INSERT INTO "depend" VALUES (42, 0, 0);
INSERT INTO "depend" VALUES (43, 0, 0);
INSERT INTO "depend" VALUES (44, 0, 0);
INSERT INTO "depend" VALUES (45, 0, 0);
INSERT INTO "depend" VALUES (46, 0, 0);
INSERT INTO "depend" VALUES (47, 0, 0);
INSERT INTO "depend" VALUES (48, 0, 0);
INSERT INTO "depend" VALUES (49, 0, 0);
INSERT INTO "depend" VALUES (50, 0, 0);
INSERT INTO "depend" VALUES (51, 0, 0);
INSERT INTO "depend" VALUES (52, 0, 0);
INSERT INTO "depend" VALUES (53, 0, 0);
INSERT INTO "depend" VALUES (54, 0, 0);
INSERT INTO "depend" VALUES (55, 0, 0);
INSERT INTO "depend" VALUES (56, 0, 0);
INSERT INTO "depend" VALUES (57, 0, 0);
INSERT INTO "depend" VALUES (58, 0, 0);
INSERT INTO "depend" VALUES (59, 0, 0);
INSERT INTO "depend" VALUES (60, 0, 0);
INSERT INTO "depend" VALUES (61, 0, 0);
INSERT INTO "depend" VALUES (62, 0, 0);
INSERT INTO "depend" VALUES (63, 0, 0);
INSERT INTO "depend" VALUES (64, 0, 0);
INSERT INTO "depend" VALUES (65, 0, 0);
INSERT INTO "depend" VALUES (66, 0, 0);
INSERT INTO "depend" VALUES (67, 0, 0);
INSERT INTO "depend" VALUES (68, 0, 0);
INSERT INTO "depend" VALUES (69, 0, 0);
INSERT INTO "depend" VALUES (70, 0, 0);
INSERT INTO "depend" VALUES (71, 0, 0);
INSERT INTO "depend" VALUES (72, 0, 0);
INSERT INTO "depend" VALUES (73, 0, 0);
INSERT INTO "depend" VALUES (74, 0, 0);
INSERT INTO "depend" VALUES (75, 0, 0);
INSERT INTO "depend" VALUES (76, 0, 0);
INSERT INTO "depend" VALUES (77, 0, 0);
INSERT INTO "depend" VALUES (78, 0, 0);
INSERT INTO "depend" VALUES (79, 0, 0);
INSERT INTO "depend" VALUES (80, 0, 0);
INSERT INTO "depend" VALUES (81, 0, 0);
INSERT INTO "depend" VALUES (82, 0, 0);
INSERT INTO "depend" VALUES (83, 0, 0);
INSERT INTO "depend" VALUES (84, 0, 0);
INSERT INTO "depend" VALUES (85, 0, 0);
INSERT INTO "depend" VALUES (86, 0, 0);
INSERT INTO "depend" VALUES (87, 0, 0);
INSERT INTO "depend" VALUES (88, 0, 0);
INSERT INTO "depend" VALUES (89, 0, 0);
INSERT INTO "depend" VALUES (90, 0, 0);
INSERT INTO "depend" VALUES (91, 0, 0);
INSERT INTO "depend" VALUES (92, 0, 0);
INSERT INTO "depend" VALUES (93, 0, 0);
INSERT INTO "depend" VALUES (94, 0, 0);
INSERT INTO "depend" VALUES (95, 0, 0);
INSERT INTO "depend" VALUES (96, 0, 0);
INSERT INTO "depend" VALUES (97, 0, 0);
INSERT INTO "depend" VALUES (98, 0, 0);
INSERT INTO "depend" VALUES (99, 0, 0);
INSERT INTO "depend" VALUES (100, 0, 0);
INSERT INTO "depend" VALUES (101, 0, 0);
INSERT INTO "depend" VALUES (102, 0, 0);
INSERT INTO "depend" VALUES (103, 0, 0);
INSERT INTO "depend" VALUES (104, 0, 0);
INSERT INTO "depend" VALUES (105, 0, 0);
INSERT INTO "depend" VALUES (106, 0, 0);
INSERT INTO "depend" VALUES (107, 0, 0);
INSERT INTO "depend" VALUES (108, 0, 0);
INSERT INTO "depend" VALUES (109, 0, 0);
INSERT INTO "depend" VALUES (110, 0, 0);
INSERT INTO "depend" VALUES (111, 0, 0);
INSERT INTO "depend" VALUES (112, 0, 0);
INSERT INTO "depend" VALUES (113, 0, 0);
INSERT INTO "depend" VALUES (114, 0, 0);
INSERT INTO "depend" VALUES (115, 0, 0);
INSERT INTO "depend" VALUES (116, 0, 0);
INSERT INTO "depend" VALUES (117, 0, 0);
INSERT INTO "depend" VALUES (118, 0, 0);
INSERT INTO "depend" VALUES (119, 0, 0);
INSERT INTO "depend" VALUES (120, 0, 0);
INSERT INTO "depend" VALUES (121, 0, 0);
CREATE TABLE "direct_subclassof" (
	"sub"	mediumint	NOT NULL,
	"super"	mediumint	NOT NULL,

	PRIMARY KEY ("sub", "super") -- c1013868
);
CREATE INDEX "direct_subclassof_super_idx" ON "direct_subclassof" ("super");
INSERT INTO "direct_subclassof" VALUES (8, 27);
INSERT INTO "direct_subclassof" VALUES (9, 27);
INSERT INTO "direct_subclassof" VALUES (10, 27);
INSERT INTO "direct_subclassof" VALUES (29, 2);
INSERT INTO "direct_subclassof" VALUES (26, 16);
INSERT INTO "direct_subclassof" VALUES (17, 15);
INSERT INTO "direct_subclassof" VALUES (7, 15);
INSERT INTO "direct_subclassof" VALUES (3, 17);
INSERT INTO "direct_subclassof" VALUES (2, 15);
INSERT INTO "direct_subclassof" VALUES (16, 15);
INSERT INTO "direct_subclassof" VALUES (11, 15);
INSERT INTO "direct_subclassof" VALUES (27, 15);
CREATE TABLE "direct_subpropertyof" (
	"sub"	mediumint	NOT NULL,
	"super"	mediumint	NOT NULL,

	PRIMARY KEY ("sub", "super") -- c1014770
);
CREATE INDEX "direct_subpropertyof_super_idx" ON "direct_subpropertyof" ("super");
CREATE TABLE "domain" (
	"property"	mediumint	NOT NULL,
	"class"	mediumint	NOT NULL,

	PRIMARY KEY ("class", "property") -- c1016123
);
CREATE INDEX "domain_class_idx" ON "domain" ("class");
INSERT INTO "domain" VALUES (19, 2);
INSERT INTO "domain" VALUES (18, 16);
INSERT INTO "domain" VALUES (20, 2);
INSERT INTO "domain" VALUES (21, 2);
INSERT INTO "domain" VALUES (4, 7);
INSERT INTO "domain" VALUES (5, 7);
INSERT INTO "domain" VALUES (6, 7);
INSERT INTO "domain" VALUES (12, 11);
INSERT INTO "domain" VALUES (13, 11);
CREATE TABLE "expiredtriples" (
	"id"	mediumint	NOT NULL
);
CREATE TABLE "groundedtriples" (
	"id"	mediumint	NOT NULL,

	PRIMARY KEY ("id") -- c1018880
);
CREATE TABLE "inferred" (
	"id"	mediumint	NOT NULL,
	"subject"	mediumint	NOT NULL,
	"predicate"	mediumint	NOT NULL,
	"object"	mediumint	NOT NULL,
	"explicit"	boolean	NOT NULL
);
CREATE TABLE "instanceof" (
	"inst"	mediumint	NOT NULL,
	"class"	mediumint	NOT NULL,

	PRIMARY KEY ("class", "inst") -- c1015221
);
CREATE INDEX "instanceof_class_idx" ON "instanceof" ("class");
CREATE TABLE "literals" (
	"id"	mediumint	NOT NULL,
	"datatype"	mediumint	NOT NULL,
	"language"	varchar(16),
	"labelkey"	character(16)	NOT NULL,
	"label"	varchar(1024)	NOT NULL,

	PRIMARY KEY ("id") -- c1009927
);
CREATE INDEX "literals_labelkey_idx" ON "literals" ("labelkey");
CREATE TABLE "namespaces" (
	"id"	mediumint	NOT NULL,
	"prefix"	varchar(16)	NOT NULL,
	"name"	varchar(1024),
	"userdefined"	boolean	NOT NULL,
	"export"	boolean	NOT NULL,

	PRIMARY KEY ("id"), -- c1008998
	UNIQUE ("prefix") -- c1009050
);
INSERT INTO "namespaces" VALUES (0, '', NULL, false, false);
INSERT INTO "namespaces" VALUES (1, 'ns1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', false, false);
INSERT INTO "namespaces" VALUES (2, 'ns2', 'http://www.w3.org/2000/01/rdf-schema#', false, false);
CREATE TABLE "newgroundedtriples" (
	"id"	mediumint	NOT NULL,

	PRIMARY KEY ("id") -- c1019160
);
CREATE TABLE "newtriples" (
	"id"	mediumint	NOT NULL,
	"subject"	mediumint	NOT NULL,
	"predicate"	mediumint	NOT NULL,
	"object"	mediumint	NOT NULL,
	"explicit"	boolean	NOT NULL
);
CREATE TABLE "noncycles" (
	"sub"	mediumint	NOT NULL,
	"super"	mediumint	NOT NULL,
	UNIQUE ("sub", "super") -- c1075190
);
INSERT INTO "noncycles" VALUES (24, 25);
CREATE TABLE "proper_instanceof" (
	"inst"	mediumint	NOT NULL,
	"class"	mediumint	NOT NULL,

	PRIMARY KEY ("class", "inst") -- c1015672
);
CREATE INDEX "proper_instanceof_class_idx" ON "proper_instanceof" ("class");
CREATE TABLE "property" (
	"id"	mediumint	NOT NULL,

	PRIMARY KEY ("id") -- c1013131
);
INSERT INTO "property" VALUES (4);
INSERT INTO "property" VALUES (5);
INSERT INTO "property" VALUES (6);
INSERT INTO "property" VALUES (12);
INSERT INTO "property" VALUES (13);
INSERT INTO "property" VALUES (20);
INSERT INTO "property" VALUES (21);
INSERT INTO "property" VALUES (18);
INSERT INTO "property" VALUES (19);
INSERT INTO "property" VALUES (1);
INSERT INTO "property" VALUES (22);
INSERT INTO "property" VALUES (23);
INSERT INTO "property" VALUES (24);
INSERT INTO "property" VALUES (25);
CREATE TABLE "range" (
	"property"	mediumint	NOT NULL,
	"class"	mediumint	NOT NULL,

	PRIMARY KEY ("class", "property") -- c1016574
);
CREATE INDEX "range_class_idx" ON "range" ("class");
INSERT INTO "range" VALUES (19, 2);
INSERT INTO "range" VALUES (18, 16);
INSERT INTO "range" VALUES (20, 16);
INSERT INTO "range" VALUES (21, 16);
INSERT INTO "range" VALUES (1, 16);
INSERT INTO "range" VALUES (22, 17);
INSERT INTO "range" VALUES (23, 17);
INSERT INTO "range" VALUES (13, 11);
CREATE TABLE "rawtriples" (
	"id"	mediumint	NOT NULL,
	"subjns"	mediumint	NOT NULL,
	"subjlname"	varchar(255)	NOT NULL,
	"predns"	mediumint	NOT NULL,
	"predlname"	varchar(255)	NOT NULL,
	"objns"	mediumint	NOT NULL,
	"objlname"	varchar(255)	NOT NULL,
	"objlang"	varchar(16),
	"objlabelkey"	character(16),
	"objlabel"	varchar(1024)
);
CREATE TABLE "repinfo" (
	"infokey"	varchar(255)	NOT NULL,
	"infovalue"	varchar(255)	NOT NULL,

	PRIMARY KEY ("infokey") -- c1008145
);
INSERT INTO "repinfo" VALUES ('schemaversion', '1');
INSERT INTO "repinfo" VALUES ('schemacreator', 'org.openrdf.sesame.sailimpl.rdbms.RdfSource');
CREATE TABLE "resources" (
	"id"	mediumint	NOT NULL,
	"namespace"	mediumint	NOT NULL,
	"localname"	varchar(255)	NOT NULL,

	PRIMARY KEY ("id"), -- c1009462
	UNIQUE ("namespace", "localname") -- c1009503
);
INSERT INTO "resources" VALUES (0, 0, '');
INSERT INTO "resources" VALUES (1, 1, 'type');
INSERT INTO "resources" VALUES (2, 1, 'Property');
INSERT INTO "resources" VALUES (3, 1, 'XMLLiteral');
INSERT INTO "resources" VALUES (4, 1, 'subject');
INSERT INTO "resources" VALUES (5, 1, 'predicate');
INSERT INTO "resources" VALUES (6, 1, 'object');
INSERT INTO "resources" VALUES (7, 1, 'Statement');
INSERT INTO "resources" VALUES (8, 1, 'Alt');
INSERT INTO "resources" VALUES (9, 1, 'Bag');
INSERT INTO "resources" VALUES (10, 1, 'Seq');
INSERT INTO "resources" VALUES (11, 1, 'List');
INSERT INTO "resources" VALUES (12, 1, 'first');
INSERT INTO "resources" VALUES (13, 1, 'rest');
INSERT INTO "resources" VALUES (14, 1, 'nil');
INSERT INTO "resources" VALUES (15, 2, 'Resource');
INSERT INTO "resources" VALUES (16, 2, 'Class');
INSERT INTO "resources" VALUES (17, 2, 'Literal');
INSERT INTO "resources" VALUES (18, 2, 'subClassOf');
INSERT INTO "resources" VALUES (19, 2, 'subPropertyOf');
INSERT INTO "resources" VALUES (20, 2, 'domain');
INSERT INTO "resources" VALUES (21, 2, 'range');
INSERT INTO "resources" VALUES (22, 2, 'comment');
INSERT INTO "resources" VALUES (23, 2, 'label');
INSERT INTO "resources" VALUES (24, 2, 'isDefinedBy');
INSERT INTO "resources" VALUES (25, 2, 'seeAlso');
INSERT INTO "resources" VALUES (26, 2, 'Datatype');
INSERT INTO "resources" VALUES (27, 2, 'Container');
INSERT INTO "resources" VALUES (28, 2, 'member');
INSERT INTO "resources" VALUES (29, 2, 'ContainerMembershipProperty');
CREATE TABLE "subclassof" (
	"sub"	mediumint	NOT NULL,
	"super"	mediumint	NOT NULL,

	PRIMARY KEY ("sub", "super") -- c1013417
);
CREATE INDEX "subclassof_super_idx" ON "subclassof" ("super");
INSERT INTO "subclassof" VALUES (8, 27);
INSERT INTO "subclassof" VALUES (9, 27);
INSERT INTO "subclassof" VALUES (10, 27);
INSERT INTO "subclassof" VALUES (29, 2);
INSERT INTO "subclassof" VALUES (26, 16);
INSERT INTO "subclassof" VALUES (15, 15);
INSERT INTO "subclassof" VALUES (17, 15);
INSERT INTO "subclassof" VALUES (7, 15);
INSERT INTO "subclassof" VALUES (17, 17);
INSERT INTO "subclassof" VALUES (7, 7);
INSERT INTO "subclassof" VALUES (3, 17);
INSERT INTO "subclassof" VALUES (8, 15);
INSERT INTO "subclassof" VALUES (9, 15);
INSERT INTO "subclassof" VALUES (10, 15);
INSERT INTO "subclassof" VALUES (29, 15);
INSERT INTO "subclassof" VALUES (26, 15);
INSERT INTO "subclassof" VALUES (2, 15);
INSERT INTO "subclassof" VALUES (16, 15);
INSERT INTO "subclassof" VALUES (11, 15);
INSERT INTO "subclassof" VALUES (27, 15);
INSERT INTO "subclassof" VALUES (3, 15);
INSERT INTO "subclassof" VALUES (8, 8);
INSERT INTO "subclassof" VALUES (9, 9);
INSERT INTO "subclassof" VALUES (10, 10);
INSERT INTO "subclassof" VALUES (29, 29);
INSERT INTO "subclassof" VALUES (26, 26);
INSERT INTO "subclassof" VALUES (2, 2);
INSERT INTO "subclassof" VALUES (16, 16);
INSERT INTO "subclassof" VALUES (11, 11);
INSERT INTO "subclassof" VALUES (27, 27);
INSERT INTO "subclassof" VALUES (3, 3);
CREATE TABLE "subpropertyof" (
	"sub"	mediumint	NOT NULL,
	"super"	mediumint	NOT NULL,

	PRIMARY KEY ("sub", "super") -- c1014319
);
CREATE INDEX "subpropertyof_super_idx" ON "subpropertyof" ("super");
INSERT INTO "subpropertyof" VALUES (24, 25);
INSERT INTO "subpropertyof" VALUES (4, 4);
INSERT INTO "subpropertyof" VALUES (5, 5);
INSERT INTO "subpropertyof" VALUES (6, 6);
INSERT INTO "subpropertyof" VALUES (12, 12);
INSERT INTO "subpropertyof" VALUES (13, 13);
INSERT INTO "subpropertyof" VALUES (20, 20);
INSERT INTO "subpropertyof" VALUES (21, 21);
INSERT INTO "subpropertyof" VALUES (18, 18);
INSERT INTO "subpropertyof" VALUES (19, 19);
INSERT INTO "subpropertyof" VALUES (1, 1);
INSERT INTO "subpropertyof" VALUES (22, 22);
INSERT INTO "subpropertyof" VALUES (23, 23);
INSERT INTO "subpropertyof" VALUES (24, 24);
INSERT INTO "subpropertyof" VALUES (25, 25);
CREATE TABLE "triples" (
	"id"	mediumint	NOT NULL,
	"subject"	mediumint	NOT NULL,
	"predicate"	mediumint	NOT NULL,
	"object"	mediumint	NOT NULL,
	"explicit"	boolean	NOT NULL,
	UNIQUE ("subject", "predicate", "object") -- c1010417
);
CREATE INDEX "triples_object_idx" ON "triples" ("object");
CREATE INDEX "triples_predicate_idx" ON "triples" ("predicate");
CREATE INDEX "triples_predicate_object_idx" ON "triples" ("predicate", "object");
CREATE INDEX "triples_subject_idx" ON "triples" ("subject");
CREATE INDEX "triples_subject_object_idx" ON "triples" ("subject", "object");
CREATE INDEX "triples_subject_predicate_idx" ON "triples" ("subject", "predicate");
INSERT INTO "triples" VALUES (1, 19, 20, 2, false);
INSERT INTO "triples" VALUES (2, 18, 20, 16, false);
INSERT INTO "triples" VALUES (3, 20, 20, 2, false);
INSERT INTO "triples" VALUES (4, 21, 20, 2, false);
INSERT INTO "triples" VALUES (5, 4, 20, 7, false);
INSERT INTO "triples" VALUES (6, 5, 20, 7, false);
INSERT INTO "triples" VALUES (7, 6, 20, 7, false);
INSERT INTO "triples" VALUES (8, 12, 20, 11, false);
INSERT INTO "triples" VALUES (9, 13, 20, 11, false);
INSERT INTO "triples" VALUES (10, 19, 21, 2, false);
INSERT INTO "triples" VALUES (11, 18, 21, 16, false);
INSERT INTO "triples" VALUES (12, 20, 21, 16, false);
INSERT INTO "triples" VALUES (13, 21, 21, 16, false);
INSERT INTO "triples" VALUES (14, 1, 21, 16, false);
INSERT INTO "triples" VALUES (15, 22, 21, 17, false);
INSERT INTO "triples" VALUES (16, 23, 21, 17, false);
INSERT INTO "triples" VALUES (17, 13, 21, 11, false);
INSERT INTO "triples" VALUES (18, 8, 18, 27, false);
INSERT INTO "triples" VALUES (19, 9, 18, 27, false);
INSERT INTO "triples" VALUES (20, 10, 18, 27, false);
INSERT INTO "triples" VALUES (21, 29, 18, 2, false);
INSERT INTO "triples" VALUES (22, 24, 19, 25, false);
INSERT INTO "triples" VALUES (23, 3, 1, 26, false);
INSERT INTO "triples" VALUES (24, 26, 18, 16, false);
INSERT INTO "triples" VALUES (25, 15, 1, 16, false);
INSERT INTO "triples" VALUES (26, 17, 1, 16, false);
INSERT INTO "triples" VALUES (27, 7, 1, 16, false);
INSERT INTO "triples" VALUES (28, 14, 1, 11, false);
INSERT INTO "triples" VALUES (29, 4, 1, 2, false);
INSERT INTO "triples" VALUES (30, 5, 1, 2, false);
INSERT INTO "triples" VALUES (31, 6, 1, 2, false);
INSERT INTO "triples" VALUES (32, 12, 1, 2, false);
INSERT INTO "triples" VALUES (33, 13, 1, 2, false);
INSERT INTO "triples" VALUES (34, 20, 1, 2, false);
INSERT INTO "triples" VALUES (35, 21, 1, 2, false);
INSERT INTO "triples" VALUES (36, 18, 1, 2, false);
INSERT INTO "triples" VALUES (37, 19, 1, 2, false);
INSERT INTO "triples" VALUES (38, 1, 1, 2, false);
INSERT INTO "triples" VALUES (39, 22, 1, 2, false);
INSERT INTO "triples" VALUES (40, 23, 1, 2, false);
INSERT INTO "triples" VALUES (41, 8, 1, 16, false);
INSERT INTO "triples" VALUES (42, 9, 1, 16, false);
INSERT INTO "triples" VALUES (43, 10, 1, 16, false);
INSERT INTO "triples" VALUES (44, 29, 1, 16, false);
INSERT INTO "triples" VALUES (45, 24, 1, 2, false);
INSERT INTO "triples" VALUES (46, 26, 1, 16, false);
INSERT INTO "triples" VALUES (47, 2, 1, 16, false);
INSERT INTO "triples" VALUES (48, 16, 1, 16, false);
INSERT INTO "triples" VALUES (49, 11, 1, 16, false);
INSERT INTO "triples" VALUES (50, 27, 1, 16, false);
INSERT INTO "triples" VALUES (51, 25, 1, 2, false);
INSERT INTO "triples" VALUES (52, 19, 1, 15, false);
INSERT INTO "triples" VALUES (53, 18, 1, 15, false);
INSERT INTO "triples" VALUES (54, 20, 1, 15, false);
INSERT INTO "triples" VALUES (55, 21, 1, 15, false);
INSERT INTO "triples" VALUES (56, 4, 1, 15, false);
INSERT INTO "triples" VALUES (57, 5, 1, 15, false);
INSERT INTO "triples" VALUES (58, 6, 1, 15, false);
INSERT INTO "triples" VALUES (59, 12, 1, 15, false);
INSERT INTO "triples" VALUES (60, 13, 1, 15, false);
INSERT INTO "triples" VALUES (61, 1, 1, 15, false);
INSERT INTO "triples" VALUES (62, 22, 1, 15, false);
INSERT INTO "triples" VALUES (63, 23, 1, 15, false);
INSERT INTO "triples" VALUES (64, 8, 1, 15, false);
INSERT INTO "triples" VALUES (65, 9, 1, 15, false);
INSERT INTO "triples" VALUES (66, 10, 1, 15, false);
INSERT INTO "triples" VALUES (67, 29, 1, 15, false);
INSERT INTO "triples" VALUES (68, 24, 1, 15, false);
INSERT INTO "triples" VALUES (69, 3, 1, 15, false);
INSERT INTO "triples" VALUES (70, 26, 1, 15, false);
INSERT INTO "triples" VALUES (71, 15, 1, 15, false);
INSERT INTO "triples" VALUES (72, 17, 1, 15, false);
INSERT INTO "triples" VALUES (73, 7, 1, 15, false);
INSERT INTO "triples" VALUES (74, 14, 1, 15, false);
INSERT INTO "triples" VALUES (75, 2, 1, 15, false);
INSERT INTO "triples" VALUES (76, 16, 1, 15, false);
INSERT INTO "triples" VALUES (77, 11, 1, 15, false);
INSERT INTO "triples" VALUES (78, 27, 1, 15, false);
INSERT INTO "triples" VALUES (79, 25, 1, 15, false);
INSERT INTO "triples" VALUES (80, 4, 19, 4, false);
INSERT INTO "triples" VALUES (81, 5, 19, 5, false);
INSERT INTO "triples" VALUES (82, 6, 19, 6, false);
INSERT INTO "triples" VALUES (83, 12, 19, 12, false);
INSERT INTO "triples" VALUES (84, 13, 19, 13, false);
INSERT INTO "triples" VALUES (85, 15, 18, 15, false);
INSERT INTO "triples" VALUES (86, 17, 18, 15, false);
INSERT INTO "triples" VALUES (87, 7, 18, 15, false);
INSERT INTO "triples" VALUES (88, 17, 18, 17, false);
INSERT INTO "triples" VALUES (89, 7, 18, 7, false);
INSERT INTO "triples" VALUES (90, 3, 1, 16, false);
INSERT INTO "triples" VALUES (91, 3, 18, 17, false);
INSERT INTO "triples" VALUES (92, 0, 1, 15, false);
INSERT INTO "triples" VALUES (93, 20, 19, 20, false);
INSERT INTO "triples" VALUES (94, 21, 19, 21, false);
INSERT INTO "triples" VALUES (95, 18, 19, 18, false);
INSERT INTO "triples" VALUES (96, 19, 19, 19, false);
INSERT INTO "triples" VALUES (97, 1, 19, 1, false);
INSERT INTO "triples" VALUES (98, 22, 19, 22, false);
INSERT INTO "triples" VALUES (99, 23, 19, 23, false);
INSERT INTO "triples" VALUES (100, 24, 19, 24, false);
INSERT INTO "triples" VALUES (101, 25, 19, 25, false);
INSERT INTO "triples" VALUES (102, 8, 18, 15, false);
INSERT INTO "triples" VALUES (103, 9, 18, 15, false);
INSERT INTO "triples" VALUES (104, 10, 18, 15, false);
INSERT INTO "triples" VALUES (105, 29, 18, 15, false);
INSERT INTO "triples" VALUES (106, 26, 18, 15, false);
INSERT INTO "triples" VALUES (107, 2, 18, 15, false);
INSERT INTO "triples" VALUES (108, 16, 18, 15, false);
INSERT INTO "triples" VALUES (109, 11, 18, 15, false);
INSERT INTO "triples" VALUES (110, 27, 18, 15, false);
INSERT INTO "triples" VALUES (111, 3, 18, 15, false);
INSERT INTO "triples" VALUES (112, 8, 18, 8, false);
INSERT INTO "triples" VALUES (113, 9, 18, 9, false);
INSERT INTO "triples" VALUES (114, 10, 18, 10, false);
INSERT INTO "triples" VALUES (115, 29, 18, 29, false);
INSERT INTO "triples" VALUES (116, 26, 18, 26, false);
INSERT INTO "triples" VALUES (117, 2, 18, 2, false);
INSERT INTO "triples" VALUES (118, 16, 18, 16, false);
INSERT INTO "triples" VALUES (119, 11, 18, 11, false);
INSERT INTO "triples" VALUES (120, 27, 18, 27, false);
INSERT INTO "triples" VALUES (121, 3, 18, 3, false);
COMMIT;

START TRANSACTION;
INSERT INTO newtriples SELECT inf.* FROM inferred inf
LEFT JOIN triples t ON inf.subject = t.subject AND
inf.predicate = t.predicate AND inf.object = t.object
WHERE t.subject IS NULL;
COMMIT;

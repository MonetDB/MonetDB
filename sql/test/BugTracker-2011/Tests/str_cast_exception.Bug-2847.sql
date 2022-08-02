START TRANSACTION;

CREATE SCHEMA "testschema";
CREATE TABLE "testschema"."test" (
        "type"                        CHARACTER LARGE OBJECT NOT NULL,
        "output"                      CHARACTER LARGE OBJECT NOT NULL,
        "output_min_time_value_1"     BIGINT
);
INSERT INTO "testschema"."test" VALUES ('INPUT','see_person','3');
INSERT INTO "testschema"."test" VALUES ('WORKING','greet_person','3');
INSERT INTO "testschema"."test" VALUES ('OUTPUT','greet_person','3');
SELECT "output_min_time_value_1" FROM "testschema"."test" WHERE "type" = 'OUTPUT' AND "output" = 'greet_person' ;

ROLLBACK;

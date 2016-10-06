SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"() WHERE "active";

CALL "sys"."settimeout"(9);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"() WHERE "active";

CALL "sys"."settimeout"(0);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"() WHERE "active";

CALL "sys"."settimeout"(-9);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"() WHERE "active";

CALL "sys"."settimeout"(7, -9);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"() WHERE "active";

CALL "sys"."settimeout"(7, 9);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"() WHERE "active";

CALL "sys"."setsession"(8);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"() WHERE "active";

CALL "sys"."setsession"(-8);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"() WHERE "active";


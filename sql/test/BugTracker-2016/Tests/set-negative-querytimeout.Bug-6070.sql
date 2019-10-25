SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"();

CALL "sys"."settimeout"(9);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"();

CALL "sys"."settimeout"(0);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"();

CALL "sys"."settimeout"(-9);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"();

CALL "sys"."settimeout"(7);
CALL "sys"."sessiontimeout"(0, -9);
CALL "sys"."querytimeout"(0, 0);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"();

CALL "sys"."settimeout"(7);
CALL "sys"."sessiontimeout"(0, 9);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"();

CALL "sys"."setsession"(8);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"();

CALL "sys"."setsession"(-8);
SELECT "querytimeout", "sessiontimeout" FROM "sys"."sessions"();


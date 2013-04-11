START TRANSACTION;

CREATE TABLE "sys"."delete_r" (
    "id"         INTEGER       NOT NULL AUTO_INCREMENT,
    "is_default" BOOLEAN       NOT NULL,
    CONSTRAINT "delete_r_id_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "sys"."delete_rchild" (
    "r_ptr_id" INTEGER       NOT NULL,
    CONSTRAINT "delete_rchild_r_ptr_id_pkey" PRIMARY KEY ("r_ptr_id"),
    CONSTRAINT "delete_rchild_r_ptr_id_fkey" FOREIGN KEY ("r_ptr_id") REFERENCES "sys"."delete_r" ("id")
);

CREATE TABLE "sys"."delete_a" (
    "id"                  INTEGER       NOT NULL AUTO_INCREMENT,
    "name"                VARCHAR(30)   NOT NULL,
    "auto_id"             INTEGER       NOT NULL,
    "auto_nullable_id"    INTEGER,
    "setvalue_id"         INTEGER       NOT NULL,
    "setnull_id"          INTEGER,
    "setdefault_id"       INTEGER       NOT NULL,
    "setdefault_none_id"  INTEGER,
    "cascade_id"          INTEGER       NOT NULL,
    "cascade_nullable_id" INTEGER,
    "protect_id"          INTEGER,
    "donothing_id"        INTEGER,
    "child_id"            INTEGER       NOT NULL,
    "child_setnull_id"    INTEGER,
    "o2o_setnull_id"      INTEGER,
    CONSTRAINT "delete_a_id_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "delete_a_auto_id_fkey" FOREIGN KEY ("auto_id") REFERENCES "sys"."delete_r" ("id"),
    CONSTRAINT "delete_a_auto_nullable_id_fkey" FOREIGN KEY ("auto_nullable_id") REFERENCES "sys"."delete_r" ("id"),
    CONSTRAINT "delete_a_cascade_id_fkey" FOREIGN KEY ("cascade_id") REFERENCES "sys"."delete_r" ("id"),
    CONSTRAINT "delete_a_cascade_nullable_id_fkey" FOREIGN KEY ("cascade_nullable_id") REFERENCES "sys"."delete_r" ("id"),
    CONSTRAINT "delete_a_child_id_fkey" FOREIGN KEY ("child_id") REFERENCES "sys"."delete_rchild" ("r_ptr_id"),
    CONSTRAINT "delete_a_child_setnull_id_fkey" FOREIGN KEY ("child_setnull_id") REFERENCES "sys"."delete_rchild" ("r_ptr_id"),
    CONSTRAINT "delete_a_donothing_id_fkey" FOREIGN KEY ("donothing_id") REFERENCES "sys"."delete_r" ("id"),
    CONSTRAINT "delete_a_o2o_setnull_id_fkey" FOREIGN KEY ("o2o_setnull_id") REFERENCES "sys"."delete_r" ("id"),
    CONSTRAINT "delete_a_protect_id_fkey" FOREIGN KEY ("protect_id") REFERENCES "sys"."delete_r" ("id"),
    CONSTRAINT "delete_a_setdefault_id_fkey" FOREIGN KEY ("setdefault_id") REFERENCES "sys"."delete_r" ("id"),
    CONSTRAINT "delete_a_setdefault_none_id_fkey" FOREIGN KEY ("setdefault_none_id") REFERENCES "sys"."delete_r" ("id"),
    CONSTRAINT "delete_a_setnull_id_fkey" FOREIGN KEY ("setnull_id") REFERENCES "sys"."delete_r" ("id"),
    CONSTRAINT "delete_a_setvalue_id_fkey" FOREIGN KEY ("setvalue_id") REFERENCES "sys"."delete_r" ("id")
);
CREATE INDEX "delete_a_2d7cfe83" ON "sys"."delete_a" ("child_setnull_id");
CREATE INDEX "delete_a_3256f49f" ON "sys"."delete_a" ("setdefault_id");
CREATE INDEX "delete_a_387a994a" ON "sys"."delete_a" ("auto_nullable_id");
CREATE INDEX "delete_a_39102852" ON "sys"."delete_a" ("cascade_nullable_id");
CREATE INDEX "delete_a_39c8e34b" ON "sys"."delete_a" ("donothing_id");
CREATE INDEX "delete_a_705384fd" ON "sys"."delete_a" ("auto_id");
CREATE INDEX "delete_a_afc263df" ON "sys"."delete_a" ("cascade_id");
CREATE INDEX "delete_a_c8a2655b" ON "sys"."delete_a" ("setvalue_id");
CREATE INDEX "delete_a_cc32e934" ON "sys"."delete_a" ("setnull_id");
CREATE INDEX "delete_a_cf9d920d" ON "sys"."delete_a" ("o2o_setnull_id");
CREATE INDEX "delete_a_da61a3df" ON "sys"."delete_a" ("child_id");
CREATE INDEX "delete_a_ea02b18e" ON "sys"."delete_a" ("protect_id");
CREATE INDEX "delete_a_f7f4dcfb" ON "sys"."delete_a" ("setdefault_none_id");

UPDATE "sys"."delete_a" SET "setnull_id" = NULL WHERE "id" IN (1);

DROP TABLE "sys"."delete_a";
DROP TABLE "sys"."delete_rchild";
DROP TABLE "sys"."delete_r";

ROLLBACK;

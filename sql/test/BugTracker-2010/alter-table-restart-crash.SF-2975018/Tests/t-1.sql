--
-- Setup two test tables, one that references the other.
-- 

CREATE TABLE "sf2975018t1" (
"id" int NOT NULL,
CONSTRAINT "sf2975018t1_id_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "sf2975018t2" (
"id" int NOT NULL,
"sf2975018t1_id" int NOT NULL,
CONSTRAINT "sf2975018t2_id_pkey" PRIMARY KEY ("id"),
CONSTRAINT "sf2975018t2_sf2975018t1_id_fkey" FOREIGN KEY ("sf2975018t1_id") REFERENCES "sf2975018t1" ("id")
);
CREATE INDEX "sf2975018t2_sf2975018t1_id" ON "sf2975018t2" ("sf2975018t1_id");

--
-- Drop fkey constraint and index
-- 

DROP INDEX "sf2975018t2_sf2975018t1_id";
ALTER TABLE "sf2975018t2" DROP CONSTRAINT sf2975018t2_sf2975018t1_id_fkey;

--
-- Drop fkey column that links tables.
--

ALTER TABLE "sf2975018t2" DROP COLUMN sf2975018t1_id;

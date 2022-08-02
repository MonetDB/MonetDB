start transaction;

CREATE TABLE "PRIMARY" (
  "ID" integer NOT NULL,
  "LEN" real NOT NULL,
  CONSTRAINT "PK_PRIMARY" PRIMARY KEY ("ID")
);

CREATE TABLE "FOREIGN" (
  "ID" integer NOT NULL,
  "POS" real NOT NULL,
  CONSTRAINT "FK_FOREIGN" FOREIGN KEY ("ID") REFERENCES "PRIMARY" ("ID")
);

alter table "FOREIGN" add column "RelPos" real default NULL;

update "FOREIGN"
	set "RelPos" = "POS" / (
		select "LEN"
		from "PRIMARY"
		where "PRIMARY"."ID" = "FOREIGN"."ID"
	)
	where "POS" between 0 and (
		select "LEN"
		from "PRIMARY"
		where "PRIMARY"."ID" = "FOREIGN"."ID"
	)
;

rollback;

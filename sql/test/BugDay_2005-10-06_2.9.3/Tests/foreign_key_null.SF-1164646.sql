CREATE TABLE "first" (
	"id" int NOT NULL,
	CONSTRAINT "first_id_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "second" (
	"id" int NOT NULL,
	"ref" int,
	CONSTRAINT "second_id_pkey" PRIMARY KEY ("id"),
	CONSTRAINT "second_ref_fkey" FOREIGN KEY ("ref") REFERENCES "first" ("id")
);

insert into "second" values (1, null);

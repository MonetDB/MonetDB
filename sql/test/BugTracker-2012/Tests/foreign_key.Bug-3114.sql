
CREATE TABLE "sys"."first" (
        "id" int        NOT NULL,
        CONSTRAINT "first_id_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "sys"."second" (
        "id"  int       NOT NULL,
        "ref" int,
        CONSTRAINT "second_id_pkey" PRIMARY KEY ("id"),
        CONSTRAINT "second_ref_fkey" FOREIGN KEY
("ref") REFERENCES "sys"."first" ("id")
);

insert into "second" values (100, null);
insert into "second" values (101, 1);

update "second" set ref = 1 WHERE id = 100;
update "second" set ref = null WHERE id = 100;

insert into "first" values (200);
update "second" set ref = 1 WHERE id = 100;

drop table "second";
drop table "first";

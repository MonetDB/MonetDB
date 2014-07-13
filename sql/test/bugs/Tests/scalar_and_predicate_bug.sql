
CREATE TABLE "act" (
  "id" int NOT NULL auto_increment,
  "title" text NOT NULL,
  "is_closed" bool NOT NULL,
  PRIMARY KEY  ("id")
);
CREATE INDEX act_isClosedIndex ON act (is_closed);

CREATE TABLE "entry" (
  "id" int NOT NULL auto_increment,
  "note" text NOT NULL,
  "act_id" int NOT NULL,
  PRIMARY KEY  ("id"),
  CONSTRAINT "entry_act_id_exists" FOREIGN KEY ("act_id") REFERENCES
"act" ("id")
);
insert into act values(100, 'hallo', 1);
insert into entry values(100, 'hallo', 100);

SELECT * FROM entry WHERE ((1) AND (id=100));
select * from entry where ((true) and (id=100));

SELECT * FROM entry WHERE ((0) AND (id=100));
select * from entry where ((false) and (id=100));

drop table entry;
drop table act;

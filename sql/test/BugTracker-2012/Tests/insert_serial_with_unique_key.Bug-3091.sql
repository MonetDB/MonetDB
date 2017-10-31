CREATE SCHEMA "meta";

CREATE TABLE "meta"."program_specs" (
    "id" INT AUTO_INCREMENT PRIMARY KEY,
    "namespace" CLOB NOT NULL,
    "simple_name" CLOB NOT NULL,
    "description" CLOB NOT NULL DEFAULT '',
    "xml_spec" CLOB NOT NULL,
    CONSTRAINT "program_spec_unique_name" UNIQUE ("namespace", "simple_name")
);

INSERT INTO "meta"."program_specs" 
    ("namespace", "simple_name", "description", "xml_spec")
VALUES 
    ( 'example', 'access-control', '', '<?xml version="1.0" encoding="UTF-8"?>'
)
;

DROP TABLE "meta"."program_specs";
DROP SCHEMA "meta" CASCADE;

create table t30(
  a varchar(100),
  b varchar(100),
  CONSTRAINT "t30_unique" UNIQUE ("a", "b")
);
insert into t30(a,b) values('x','y');

create table t31(
  a varchar(100),
  b varchar(100),
  c varchar(100),
  CONSTRAINT "t31_unique" UNIQUE ("a", "b")
);
insert into t31(a,b) values('x','y');

create table t32(
  c varchar(100),
  a varchar(100),
  b varchar(100),
  CONSTRAINT "t32_unique" UNIQUE ("a", "b")
);
insert into t32(c,a,b) values(NULL,'x','y');

drop table t30;
drop table t31;
drop table t32;

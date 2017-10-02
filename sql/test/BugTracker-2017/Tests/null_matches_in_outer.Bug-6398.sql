
create table "E" (
	"intCol" bigint,
	"stringCol" string
);

insert into "E" values (0, 'zero');
insert into "E" values (1, 'one');
insert into "E" values (2, 'two');
insert into "E" values (null, null);

create table "I" (
	"intCol" bigint,
	"stringCol" string
);

insert into "I" values (2, 'due');
insert into "I" values (4, 'quattro');
insert into "I" values (null, 'this is not null');

select * from "E" left outer join "I" on "E"."intCol" = "I"."intCol" or ("E"."intCol" is null and  "I"."intCol" is null);

drop table "E";
drop table "I";

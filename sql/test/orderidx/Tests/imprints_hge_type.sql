create table hge_types (
	"hugeint" hugeint,
	"decimal" decimal,  -- is synonym for decimal(18,3)
	"decimal38" decimal(38),
	"decimal37_22" decimal(37,22),
	"numeric" numeric,  -- is synonym for decimal(18,3)
	"numeric38" numeric(38, 0),  -- is synonym for decimal(38)
	"numeric37_9" numeric(37, 9)  -- is synonym for decimal(37,9)
);

select number, name, type, type_digits, type_scale, "null", "default" from sys._columns where table_id in (select id from sys._tables where name = 'hge_types') order by number;

insert into hge_types ("hugeint") values (12345678900987654321);
insert into hge_types ("decimal") values (123456789012345.678);
insert into hge_types ("numeric") values (123456789012345.678);
insert into hge_types ("decimal38") values (12345678900987654321);
insert into hge_types ("numeric38") values (12345678900987654321);
insert into hge_types ("decimal37_22") values (123456789012345.1234567890123456789012);
insert into hge_types ("numeric37_9") values (1234567890123456789012345.123456789);

insert into hge_types values (123456789012345, 123456789012345, 123456789012345, 123456789012345, 123456789012345, 123456789012345, 123456789012345);
insert into hge_types values (123456789012345, 123456789012345, 123456789012345, 123456789012345, 123456789012345, 123456789012345, 123456789012345);

insert into hge_types select -"hugeint", -"decimal", -"decimal38", -"decimal37_22", -"numeric", -"numeric38", -"numeric37_9" from hge_types;

select * from hge_types order by 1,2,3,4,5,6,7;


-- select 'create imprints index "hge_impidx_'||name||'" on hge_types ("'||name||'");' as stmt from sys._columns where table_id in (select id from sys._tables where name = 'hge_types') order by number;
create imprints index "hge_impidx_hugeint" on hge_types ("hugeint");
create imprints index "hge_impidx_decimal" on hge_types ("decimal");
create imprints index "hge_impidx_decimal38" on hge_types ("decimal38");
create imprints index "hge_impidx_decimal37_22" on hge_types ("decimal37_22");
create imprints index "hge_impidx_numeric" on hge_types ("numeric");
create imprints index "hge_impidx_numeric38" on hge_types ("numeric38");
create imprints index "hge_impidx_numeric37_9" on hge_types ("numeric37_9");

-- dump the table including all indexes defined on it
\D hge_types

select type, name from sys.idxs where table_id in (select id from sys._tables where name = 'hge_types') order by name;

select * from hge_types
 where "hugeint" between 1 and 12345678900987654322
    or "decimal" between 2 and 123456789012345.679
    or "numeric" between 2 and 123456789012345.679
    or "decimal38" between 3 and 123456789009876543212
    or "numeric38" between 3 and 123456789009876543212
    or "decimal37_22" between 4 and 123456789012345.1234567890123456789013
    or "numeric37_9"  between 4 and 1234567890123456789012345.123456790
 order by 1,2,3,4,5,6,7;

select * from hge_types
 where "hugeint" >= -12345678900987654322
    or "decimal" >= -123456789012345.679
    or "numeric" >= -123456789012345.679
    or "decimal38" >= -123456789009876543212
    or "numeric38" >= -123456789009876543212
    or "decimal37_22" >= -123456789012345.1234567890123456789013
    or "numeric37_9"  >= -1234567890123456789012345.123456790
 order by 7 desc, 6 desc, 5 desc, 4 desc, 3 desc, 2 desc, 1 desc;


insert into hge_types select -"hugeint" + 123, -"decimal" + 123, -"decimal38" + 123, -"decimal37_22" + 123, -"numeric" + 123, -"numeric38" + 123, -"numeric37_9" + 123 from hge_types;

select * from hge_types
 where "hugeint" >= -12345678900987654322
    or "decimal" >= -123456789012345.679
    or "numeric" >= -123456789012345.679
    or "decimal38" >= -123456789009876543212
    or "numeric38" >= -123456789009876543212
    or "decimal37_22" >= -123456789012345.1234567890123456789013
    or "numeric37_9"  >= -1234567890123456789012345.123456790
 order by 7 desc, 6 desc, 5 desc, 4 desc, 3 desc, 2 desc, 1 desc;

select type, name from sys.idxs where table_id in (select id from sys._tables where name = 'hge_types') order by name;

drop index "hge_impidx_hugeint";
drop index "hge_impidx_decimal";
drop index "hge_impidx_decimal38";
drop index "hge_impidx_decimal37_22";
drop index "hge_impidx_numeric";
drop index "hge_impidx_numeric38";
drop index "hge_impidx_numeric37_9";

-- dump the table again, now it should not list the indexes anymore
\D hge_types

drop table hge_types;

drop table if exists hge_types cascade;

select type, name from sys.idxs where table_id in (select id from sys._tables where name = 'hge_types') order by name;


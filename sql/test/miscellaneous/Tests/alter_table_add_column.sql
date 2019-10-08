START TRANSACTION;

CREATE SCHEMA "snctest";
CREATE TABLE "snctest"."u_table" ("sys_id" CHAR(32));
ALTER TABLE "snctest"."u_table" ADD "u_flat_string" varchar(40);
ALTER TABLE "snctest"."u_table" ADD "sysc_u_flat_string" varchar(40);
ALTER TABLE "snctest"."u_table" ADD "u_flat_moved" varchar(40);
ALTER TABLE "snctest"."u_table" ADD "sysc_u_flat_moved" varchar(40);
ALTER TABLE "snctest"."u_table" DROP "u_flat_string";
ALTER TABLE "snctest"."u_table" DROP "sysc_u_flat_string";
ALTER TABLE "snctest"."u_table" ADD "u_flat_string" varchar(40);
ALTER TABLE "snctest"."u_table" ADD "sysc_u_flat_string" varchar(40);

SELECT Columns.number, Columns.name, Columns.type, Columns.type_scale FROM sys.columns as Columns
WHERE Columns.table_id = (SELECT Tables.id FROM sys.tables as Tables 
                          WHERE Tables.schema_id = (SELECT Schema.id FROM sys.schemas as Schema WHERE Schema.name = 'snctest') AND Tables.name = 'u_table') 
ORDER BY Columns.number;

ROLLBACK;

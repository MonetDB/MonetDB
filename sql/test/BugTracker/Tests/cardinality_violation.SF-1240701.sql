select * from env() as env where name = ( select 'prefix' from env() as env );
select * from columns where name = (select columns.name from _tables, columns where _tables.id = columns.table_id);

select * from env where name = ( select 'prefix' from env );
select * from columns where name = (select columns.name from ptables, columns where ptables.id = columns.table_id);

select actiontype, propertytype, loanpurpose, count(*) as num_records from hmda_14 group by actiontype, propertytype, loanpurpose;

-- produces timeout/segfault, disabled for now
-- select tables.name, columns.name, location from tables inner join columns on tables.id=columns.table_id left join storage on tables.name=storage.table and columns.name=storage.column where location is null and tables.name like 'hmda%';

drop table hmda_lar_14;
drop table hmda_ins_14;
drop table hmda_14;

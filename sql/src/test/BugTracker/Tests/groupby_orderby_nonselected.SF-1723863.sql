
CREATE TABLE data_1to10 (
     id           INTEGER      NOT NULL,
     col1         INTEGER      NOT NULL,
     col2         VARCHAR(10)  NOT NULL
) ;

select count(*)
from (
select col2, count(*) as row_count
from data_1to10
group by col2
order by row_count desc, col1
) sq;

drop table data_1to10;

start transaction;

create table bug6218 (i int, j int);
copy 8 records into bug6218 from stdin;
0|0
1|0
NULL|1
NULL|1
0|2
1|2
2|0
2|2
select quantile(i,0.5),j from bug6218 group by j order by j;

rollback;

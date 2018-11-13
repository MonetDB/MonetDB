create table merging (aa int, bb clob);
insert into merging values (-100, 1);
create table predata (aa int, bb int);
insert into predata values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

merge into predata using (select aa, bb from merging) sub on predata.bb = sub.bb when matched then delete;
merge into predata using (select aa, bb from merging) sub on predata.bb = sub.bb when matched then update set bb = 1; --nothing updated
update merging set bb = 2 where bb = 1;
merge into predata using (select aa, bb from merging) sub on predata.bb = sub.bb when matched then update set bb = 1;
select aa, bb from predata order by bb;

merge into predata using (select aa, bb from merging) sub on predata.bb = sub.bb when not matched then insert values (5, 5);
select aa, bb from predata order by bb;

merge into predata as othern using (select aa, bb from merging) sub on othern.bb = sub.bb when not matched then insert values (5, 5);

merge into predata as othern using (select aa, bb from merging) sub on predata.bb = sub.bb when not matched then insert values (5, 5); --error, unknown relation predata
merge into predata using (select aa, bb from merging) sub on predata.bb = sub.bb when not matched then insert values (1, 1), (2,2); --error, only one row allowed in insert

drop table merging;
drop table predata;

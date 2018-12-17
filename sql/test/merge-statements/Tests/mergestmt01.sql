create table merging (aa int, bb clob);
create table predata (aa int, bb int);

start transaction;

insert into merging values (-100, 1);
insert into predata values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2);

merge into predata using (select aa, bb from merging) sub on predata.bb = sub.bb
      when matched then delete when not matched then insert values (6, 6);
select aa, bb from predata;

delete from predata;
insert into predata values (15, 3), (3, 1), (2, 1), (5, 3), (4, 1), (6, 3);

merge into predata using (select aa, bb from merging) sub on predata.bb = sub.bb
      when not matched then insert values (null, null) when matched then update set bb = 3;
select aa, bb from predata;

delete from predata;
insert into predata values (15, 3), (3, 1), (2, 1), (5, 3), (8, 2), (NULL, 4);

merge into predata using (select aa, bb from merging) as sub on predata.bb = sub.bb
      when matched then update set bb = predata.bb + 1;
merge into predata othertt using (select aa, bb from merging) as sub on othertt.bb = sub.bb
      when matched then update set bb = othertt.bb + sub.bb;
select aa, bb from predata;

delete from predata;
insert into predata values (15, 1), (3, 1), (6, 3), (8, 2);
insert into merging values (-500, -300);

merge into predata using (select aa, bb from merging) as sub on predata.bb = sub.bb
      when not matched then insert values (sub.aa, 2);
select aa, bb from predata;

insert into merging values (1900, 2);

merge into predata othertt using (select aa, bb from merging) as sub on othertt.bb = sub.bb
      when not matched then insert values (sub.aa + 5, sub.bb * 2);
select aa, bb from predata;

delete from predata;
insert into predata values (2, 2);

merge into predata using (select aa, bb from merging) thee on predata.bb = thee.bb
      when not matched then insert;
select aa, bb from predata;

rollback;

insert into predata values (1, 1);
insert into merging values (1, 1), (2, 1);

merge into predata using (select aa, bb from merging) as sub on predata.bb = sub.bb
      when matched then update set aa = sub.aa; --error, each target row must match one and only one source row
select aa, bb from predata;

merge into predata using (select aa, bb from merging) as sub on predata.bb = sub.bb
      when matched then delete; --error, each target row must match one and only one source row
select aa, bb from predata;

merge into predata othertt using (select aa, bb from merging) as sub on othertt.bb = sub.bb
      when not matched then insert values (othertt.aa, othertt.bb); --error there was no match for the merged table, so it shouldn't appear in the insert clause

merge into predata using (select aa, bb from merging) as sub on predata.bb = sub.bb
      when not matched then insert select 41, -12; --error, not supported

merge into predata using (select aa, bb from merging) as sub on predata.bb = sub.bb
      when not matched then insert select aa, bb from predata; --error, not supported

merge into predata using (select aa, bb from merging) as sub on predata.bb = sub.bb
      when matched then update set bb = bb - 1; --error, bb is ambiguous

drop table merging;
drop table predata;

start transaction;
create merge table uppert (aa int, bb int) partition by range on (aa);
create table subtable1 (aa int, bb int);
create table subtable2 (aa int, bb int);
create table merging (aa int, bb int);

alter table uppert add table subtable1 as partition between '-100' and '0';
alter table uppert add table subtable2 as partition between '1' and '100';
insert into merging values (-100, 1);

merge into uppert using (select aa, bb from merging) sub on uppert.bb = sub.bb
      when matched then delete when not matched then insert values (sub.aa, sub.bb); --TODO, merge statements on merge tables

merge into uppert using (select aa, bb from merging) sub on uppert.bb = sub.bb
      when matched then delete when not matched then insert values (sub.aa, sub.bb); --TODO, merge statements on merge tables

rollback;

create table merging (aa int, bb int);
create table predata (aa int, bb int not null);

merge into predata using (select * from merging) other on predata.bb = other.bb
      when not matched then insert; --ok, no rows inserted
insert into merging;
merge into predata using (select * from merging) other on predata.bb = other.bb
      when not matched then insert; --error, null constraint violation

truncate merging;
alter table predata alter bb set null;
alter table predata add constraint uniquebb unique (bb);

insert into merging values (1,1), (0,1);
insert into predata values (1,1);
merge into predata using (select * from merging) other on predata.aa = other.aa
      when not matched then insert values (other.aa, other.bb); --error, unique index violation

drop table merging;
drop table predata;

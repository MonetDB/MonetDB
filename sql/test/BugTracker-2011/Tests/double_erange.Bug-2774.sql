create table f2774 (f float);
create table d2774 (d double);

insert into f2774 values (1e-310); 
insert into d2774 values (1e-310);
insert into f2774 values (1e-312); 
insert into d2774 values (1e-312);
insert into f2774 values (1e-314); 
insert into d2774 values (1e-314);
insert into f2774 values (1e-316); 
insert into d2774 values (1e-316);
insert into f2774 values (1e-318); 
insert into d2774 values (1e-318);
insert into f2774 values (1e-320); 
insert into d2774 values (1e-320);
insert into f2774 values (1e-322); 
insert into d2774 values (1e-322);
insert into f2774 values (1e-324); 
insert into d2774 values (1e-324);
insert into f2774 values (1e-326); 
insert into d2774 values (1e-326);
insert into f2774 values (1e-328); 
insert into d2774 values (1e-328);
insert into f2774 values (1e-330); 
insert into d2774 values (1e-330);

select * from f2774;
select * from d2774;

drop table f2774;
drop table d2774;

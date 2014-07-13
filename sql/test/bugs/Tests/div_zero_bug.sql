create table div_zero_bug (grp int, value int);

insert into div_zero_bug values( NULL, 1);
insert into div_zero_bug values( NULL, 1);
insert into div_zero_bug values( 0, NULL);
insert into div_zero_bug values( 1, NULL);

select grp, avg(value) from div_zero_bug group by grp;

drop table div_zero_bug;

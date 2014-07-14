create table f2774 (i int, f float);
create table d2774 (i int, d double);

select * from f2774;
select * from d2774;

insert into f2774 values (300,1e-300);
insert into d2774 values (300,1e-300);
insert into f2774 values (302,1e-302);
insert into d2774 values (302,1e-302);
insert into f2774 values (304,1e-304);
insert into d2774 values (304,1e-304);
insert into f2774 values (306,1e-306);
insert into d2774 values (306,1e-306);
insert into f2774 values (307,1e-307);
insert into d2774 values (307,1e-307);
insert into f2774 values (308,1e-308);
insert into d2774 values (308,1e-308);
insert into f2774 values (310,1e-310);
insert into d2774 values (310,1e-310);
insert into f2774 values (312,1e-312);
insert into d2774 values (312,1e-312);
insert into f2774 values (314,1e-314);
insert into d2774 values (314,1e-314);
insert into f2774 values (316,1e-316);
insert into d2774 values (316,1e-316);
insert into f2774 values (318,1e-318);
insert into d2774 values (318,1e-318);
insert into f2774 values (320,1e-320);
insert into d2774 values (320,1e-320);
insert into f2774 values (322,1e-322);
insert into d2774 values (322,1e-322);
insert into f2774 values (323,1e-323);
insert into d2774 values (323,1e-323);
insert into f2774 values (324,1e-324);
insert into d2774 values (324,1e-324);
insert into f2774 values (326,1e-326);
insert into d2774 values (326,1e-326);
insert into f2774 values (328,1e-328);
insert into d2774 values (328,1e-328);
insert into f2774 values (330,1e-330);
insert into d2774 values (330,1e-330);

select * from f2774;
select * from d2774;

delete from f2774;
delete from d2774;

select * from f2774;
select * from d2774;

copy 18 records into f2774 from stdin using delimiters ',','\n';
300,1e-300
302,1e-302
304,1e-304
306,1e-306
307,1e-307
308,1e-308
310,1e-310
312,1e-312
314,1e-314
316,1e-316
318,1e-318
320,1e-320
322,1e-322
323,1e-323
324,1e-324
326,1e-326
328,1e-328
330,1e-330
copy 18 records into d2774 from stdin using delimiters ',','\n';
300,1e-300
302,1e-302
304,1e-304
306,1e-306
307,1e-307
308,1e-308
310,1e-310
312,1e-312
314,1e-314
316,1e-316
318,1e-318
320,1e-320
322,1e-322
323,1e-323
324,1e-324
326,1e-326
328,1e-328
330,1e-330

select * from f2774;
select * from d2774;

delete from f2774;
delete from d2774;

select * from f2774;
select * from d2774;

copy 9 records into f2774 from stdin using delimiters ',','\n';
300,1e-300
302,1e-302
304,1e-304
306,1e-306
307,1e-307
324,1e-324
326,1e-326
328,1e-328
330,1e-330
copy 9 records into d2774 from stdin using delimiters ',','\n';
300,1e-300
302,1e-302
304,1e-304
306,1e-306
307,1e-307
324,1e-324
326,1e-326
328,1e-328
330,1e-330

select * from f2774;
select * from d2774;

drop table f2774;
drop table d2774;

create table urlparts (urlid int);
insert into urlparts values (1);
insert into urlparts values (218);
insert into urlparts values (219);
insert into urlparts values (329);
insert into urlparts values (3857);
insert into urlparts values (3868);

SELECT * FROM urlparts WHERE urlid=218 OR urlid=219 OR
urlid=329 OR urlid=3857 OR urlid=3868 order by urlid;

SELECT * FROM urlparts WHERE urlid=3868 OR urlid=3857
OR urlid=329 OR urlid=219 OR urlid=218 order by urlid;


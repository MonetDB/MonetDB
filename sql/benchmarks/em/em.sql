create table m ( c number  NOT NULL, p double NOT NULL);

insert into m values (1, 0.1);
insert into m values (1, 0.6);
insert into m values (1, 0.7);
insert into m values (1, 0.5);
insert into m values (2, 0.9);
insert into m values (2, 0.7);
insert into m values (2, 0.1);
insert into m values (2, 0.3);

select m.c, m.p/maggr.sp
from m, ( select c, sum(p) from m group by c ) as maggr ( c, sp) 
where m.c = maggr.c;

drop table m;

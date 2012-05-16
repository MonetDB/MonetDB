create table skyboom (i int, ts timestamp);

select i as boom1, ts as boom2 from skyboom
union
select NULL as boom1, NULL as boom2;

drop table skyboom;

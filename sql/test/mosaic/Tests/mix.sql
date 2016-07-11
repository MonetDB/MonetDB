create table mix0(i integer);

declare o integer;
set o = 19531025;

insert into mix0 values
(0), (1), (2), (3), (5), (7), (70), (188), (190), (192), (9999),
(50), (49), (50), (50), (48), (1003), (1002), (1001), (1000), (95), (99), (96),
(98), (97), (94), (90), (93), (91), (92), (1140), (1130), (1120), (1110),
(1100), (1250), (1260), (1270), (1280), (1290), (o), (o), (o), (o), (o),
(o), (o), (o), (o), (o), (o), (o), (o), (o), (o), (o),
(o), (o), (o), (o), (9), (9), (9), (9), (9), (9), (9),
(9), (9), (9), (9), (9), (9), (9), (9), (9), (9), (9),
(9), (9), (9), (9), (9), (9), (9), (9), (9), (9), (9),
(9);

select * from mix0;

-- analyse the impact of the various compression schemes
alter table mix0 set read only;
select * from mosaic_analysis('sys','mix0','i') order by factor desc;

alter table mix0 alter column i set storage 'dictionary';
select * from mosaic_layout('sys','mix0','i') ;


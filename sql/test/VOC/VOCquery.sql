START TRANSACTION;

select boatname from "voyages";
select distinct boatname from "voyages";


select count(*) from craftsmen c, passengers p
where c.trip = p.trip and exists
(select 1 from voyages v
where c.trip = v.trip and v.boatname = 'AMSTERDAM'
and v.departure_harbour ='Texel');

select count(*) from craftsmen c, passengers p
where c.trip = p.trip and exists (select 1) ;

select count(*) from craftsmen c ;
select count(*) from craftsmen c where exists (select 1) ;

commit;

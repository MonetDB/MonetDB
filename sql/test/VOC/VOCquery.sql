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

-- Bug 7066
SELECT number, trip, tonnage, departure_Date, arrival_date,
RANK() OVER ( PARTITION BY trip ORDER BY tonnage ) AS RankAggregation,
CUME_DIST() OVER ( PARTITION BY trip ORDER BY tonnage nulls first ) as CumeDistGroup1,
CUME_DIST() OVER ( PARTITION BY trip ORDER BY tonnage nulls last ) as CumeDistGroup2,
PERCENT_RANK() OVER ( PARTITION BY trip ORDER BY tonnage nulls first ) as PercentRankGroup1,
PERCENT_RANK() OVER ( PARTITION BY trip ORDER BY tonnage nulls last ) as PercentRankGroup2
FROM voyages WHERE particulars LIKE '%_recked%';

SELECT
CUME_DIST() OVER ( ORDER BY tonnage nulls first ) as CumeDistGroup1,
CUME_DIST() OVER ( ORDER BY tonnage nulls last ) as CumeDistGroup2,
PERCENT_RANK() OVER ( ORDER BY tonnage nulls first ) as PercentRankGroup1,
PERCENT_RANK() OVER ( ORDER BY tonnage nulls last ) as PercentRankGroup2
FROM voyages WHERE particulars LIKE '%_recked%';

SELECT
CUME_DIST() OVER ( PARTITION BY trip ) as CumeDistGroup1,
PERCENT_RANK() OVER (PARTITION BY trip ) as PercentRankGroup1
FROM voyages WHERE particulars LIKE '%_recked%';
commit;

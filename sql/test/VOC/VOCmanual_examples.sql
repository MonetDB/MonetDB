CREATE VIEW onboard_people AS
SELECT * FROM (
   SELECT 'craftsmen' AS type, craftsmen.* FROM craftsmen
   UNION ALL
   SELECT 'impotenten' AS type, impotenten.* FROM impotenten
   UNION ALL
   SELECT 'passengers' AS type, passengers.* FROM passengers
   UNION ALL
   SELECT 'seafarers' AS type, seafarers.* FROM seafarers
   UNION ALL
   SELECT 'soldiers' AS type, soldiers.* FROM soldiers
   UNION ALL
   SELECT 'total' AS type, total.* FROM total
) AS onboard_people_table;

SELECT type, COUNT(*) AS total FROM onboard_people GROUP BY type ORDER BY type;

select count(*) from impotenten;

SELECT COUNT(*) FROM voyages WHERE particulars LIKE '%_recked%';

SELECT chamber, CAST(AVG(invoice) AS integer) AS average
FROM invoices
WHERE invoice IS NOT NULL
GROUP BY chamber
ORDER BY average DESC;

CREATE VIEW extended_onboard AS
SELECT number, number_sup, trip, trip_sup, onboard_at_departure, death_at_cape, 
   left_at_cape, onboard_at_cape, death_during_voyage, onboard_at_arrival,
   death_during_voyage - left_at_cape AS death_at_arrival 
FROM onboard_people;


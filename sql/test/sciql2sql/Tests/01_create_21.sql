-- create a nested array
--! CREATE ARRAY experiment2(
--! 	run date DIMENSION[ TIMESTAMP '2010-01-01': INTERVAL'1' day : *],
--! 	payload ARRAY (x integer DIMENSION[4], y integer DIMENSION[4], v float DEFAULT 0.0)
--! );

--! DROP ARRAY experiment2;

-- hour interval calls for overloaded %
-- Nested tables are not allowed in SQL
-- Array domains 
CREATE TABLE experiment2(
    run date CHECK(run >= TIMESTAMP '2010-01-01' ),
    payload float array[4][4] DEFAULT 0.0)
);

DROP TABLE experiment2;


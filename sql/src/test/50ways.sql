drop table s;
drop table sp;
commit;
create table s ( snr int, sname varchar(30) );
create table sp ( snr int, pnr varchar(30) );

select 1;
SELECT DISTINCT S.SNAME
    FROM     S, SP
    WHERE  S.SNR = SP.SNR 
    AND       SP.PNR = 'P2';

select 2; 
SELECT DISTINCT S.SNAME
    FROM     S
    WHERE  S.SNR IN
                  (SELECT SP.SNR
                    FROM     SP
                    WHERE  SP.PNR = 'P2' );

-- any is currently not supported
-- select 3; 
-- SELECT DISTINCT S.SNAME
--    FROM     S
--    WHERE  S.SNR =ANY
--                  (SELECT SP.SNR
--                    FROM    SP
--                    WHERE  SP.PNR = 'P2' );

-- MATCH is currently not supported
-- select 4; 
-- SELECT DISTINCT S.SNAME
--    FROM     S
--    WHERE  S.SNR MATCH
--                  (SELECT SP.SNR
--                    FROM     SP
--                    WHERE  SP.PNR = 'P2' );

-- select 5; 
-- SELECT DISTINCT S.SNAME
--     FROM     S
--     WHERE  S.SNR MATCH UNIQUE
--                   (SELECT SP.SNR
--                     FROM     SP
--                     WHERE  SP.PNR = 'P2' );

-- select 6;
-- SELECT DISTINCT S.SNAME
--     FROM     S
--     WHERE  S.SNR MATCH PARTIAL
--                   (SELECT SP.SNR
--                     FROM     SP
--                     WHERE  SP.PNR = 'P2' );

-- select 7;
-- SELECT DISTINCT S.SNAME
--     FROM     S
--     WHERE  S.SNR MATCH UNIQUE PARTIAL
--                   (SELECT SP.SNR
--                     FROM     SP
--                     WHERE  SP.PNR = 'P2' );

-- select 8;
-- SELECT DISTINCT S.SNAME
--     FROM     S
--     WHERE  S.SNR MATCH FULL
--                   (SELECT SP.SNR
--                     FROM     SP
--                     WHERE  SP.PNR = 'P2' );

-- select 9;
-- SELECT DISTINCT S.SNAME
--     FROM     S
--     WHERE  S.SNR MATCH UNIQUE FULL
--                   (SELECT SP.SNR
--                     FROM     SP
--                     WHERE  SP.PNR = 'P2' );

select 10;
SELECT  DISTINCT S.SNAME
      FROM     S
      WHERE  EXISTS
                    (SELECT  *
                       FROM     SP
                       WHERE  SP.SNR = S.SNR 
                       AND        SP.PNR = 'P2' );

select 11;
SELECT DISTINCT S.SNAME
      FROM   S
     WHERE (SELECT COUNT(*)
                      FROM     SP
                      WHERE  SP.SNR = S.SNR 
                      AND        SP.PNR = 'P2' ) > 0;

select 12;
SELECT DISTINCT S.SNAME
      FROM    S
      WHERE (SELECT COUNT(*)
                       FROM     SP
                       WHERE  SP.SNR = S.SNR 
                       AND        SP.PNR = 'P2' ) = 1;

-- select 13;
-- SELECT DISTINCT S.SNAME
--      FROM     S
--      WHERE  'P2' IN
--                   (SELECT SP.PNR
--                     FROM     SP
--                     WHERE  SP.SNR = S.SNR );

-- select 14;
-- SELECT DISTINCT S.SNAME
--      FROM     S
--      WHERE  'P2' =ANY
--                    (SELECT SP.PNR
--                      FROM     SP
--                      WHERE  SP.SNR = S.SNR );

--select 15;
--SELECT DISTINCT S.SNAME
--      FROM     S
--      WHERE  'P2' MATCH
--                   (SELECT SP.PNR
--                     FROM    SP
--                     WHERE  SP.SNR = S.SNR );

--select 16;
--SELECT DISTINCT S.SNAME
--      FROM     S
--      WHERE  'P2' MATCH UNIQUE
--                   (SELECT SP.PNR
--                     FROM    SP
--                     WHERE  SP.SNR = S.SNR );

--select 17;
--SELECT DISTINCT S.SNAME
--      FROM     S
--      WHERE  'P2' MATCH PARTIAL
--                    (SELECT SP.PNR
--                      FROM     SP
--                      WHERE  SP.SNR = S.SNR );

-- select 18;
-- SELECT DISTINCT S.SNAME
--       FROM     S
--       WHERE  'P2' MATCH UNIQUE PARTIAL
--                    (SELECT  SP.PNR
--                       FROM     SP
--                       WHERE  SP.SNR = S.SNR );

-- select 19;
-- SELECT DISTINCT S.SNAME
--       FROM     S
--       WHERE  'P2' MATCH FULL
--                     (SELECT SP.PNR
--                       FROM     SP
--                       WHERE  SP.SNR = S.SNR );

-- select 20;
-- SELECT DISTINCT S.SNAME
--       FROM     S
--       WHERE  'P2' MATCH UNIQUE FULL
--                    (SELECT SP.PNR
--                      FROM     SP
--                      WHERE  SP.SNR = S.SNR );

select 21;
SELECT  S.SNAME
      FROM     S, SP
      WHERE  S.SNR = SP.SNR
      AND       SP.PNR = 'P2'
      GROUP  BY S.SNAME;

select 22;
SELECT DISTINCT S.SNAME
      FROM     S, SP
      WHERE  S.SNR = SP.SNR
      GROUP   BY S.SNAME, SP.PNR
      HAVING SP.PNR = 'P2';

select 23;
SELECT DISTINCT S.SNAME
      FROM     S, SP
      WHERE  SP.PNR = 'P2'
      GROUP   BY S.SNR, S.SNAME, SP.SNR
      HAVING SP.SNR = S.SNR;

select 24;
SELECT  DISTINCT S.SNAME
      FROM     S, SP
      GROUP   BY S.SNR, S.SNAME, SP.SNR, SP.PNR
      HAVING SP.SNR = S.SNR
      AND        SP.PNR = 'P2';

select 25;
SELECT  DISTINCT S.SNAME
      FROM     S CROSS JOIN SP
      WHERE  S.SNR = SP.SNR
      AND        SP.PNR = 'P2';

select 26;
SELECT  DISTINCT S.SNAME
      FROM     S NATURAL JOIN SP
      WHERE  SP.PNR = 'P2';

select 27;
SELECT  DISTINCT S.SNAME
      FROM     S JOIN SP USING ( SNR ) 
      WHERE  SP.PNR = 'P2';

select 28;
SELECT  DISTINCT S.SNAME
      FROM     S JOIN SP ON S.SNR = SP.SNR
      WHERE  SP.PNR = 'P2';

select 29;
SELECT  DISTINCT S.SNAME
      FROM     S NATURAL LEFT JOIN SP
      WHERE  SP.PNR = 'P2';

select 30;
SELECT  DISTINCT S.SNAME
      FROM     S LEFT JOIN SP USING ( SNR )
      WHERE  SP.PNR = 'P2';

select 31;
SELECT  DISTINCT S.SNAME
      FROM     S LEFT JOIN SP ON S.SNR = SP.SNR
      WHERE  SP.PNR = 'P2';

select 32;
SELECT  DISTINCT S.SNAME
      FROM     S NATURAL RIGHT JOIN SP
      WHERE  SP.PNR = 'P2';

select 33;
SELECT  DISTINCT S.SNAME
      FROM     S RIGHT JOIN SP USING ( SNR )
      WHERE  SP.PNR = 'P2';

select 34;
SELECT  DISTINCT S.SNAME
      FROM     S RIGHT JOIN SP ON S.SNR = SP.SNR
      WHERE  SP.PNR = 'P2';

select 35;
SELECT  DISTINCT S.SNAME
      FROM     S NATURAL FULL JOIN SP
      WHERE  SP.PNR = 'P2';

select 36;
SELECT  DISTINCT S.SNAME
      FROM     S FULL JOIN SP USING ( SNR )
      WHERE  SP.PNR = 'P2';

select 37;
SELECT  DISTINCT S.SNAME
      FROM     S FULL JOIN SP ON S.SNR = SP.SNR
      WHERE  SP.PNR = 'P2';

select 38;
SELECT  DISTINCT S.SNAME
      FROM     S 
                      NATURAL JOIN 
                    (SELECT  SP.SNR 
                      FROM     SP 
                      WHERE  SP.PNR = 'P2' ) AS POINTLESS;

select 39;
SELECT  DISTINCT S.SNAME
      FROM     S 
                      JOIN 
                   (SELECT SP.SNR 
                     FROM     SP 
                     WHERE  SP.PNR = 'P2' ) AS POINTLESS 
                     USING  ( SNR );

--select 40;
--SELECT  DISTINCT S.SNAME
--       FROM     S 
--                       JOIN 
--                    (SELECT SP.SNR 
--                      FROM     SP 
--                      WHERE  SP.PNR = 'P2' ) AS POINTLESS 
--                      ON S.SNR = SP.SNR;

select 41;
SELECT  DISTINCT S.SNAME
      FROM     S 
                      NATURAL LEFT JOIN 
                    (SELECT SP.SNR 
                      FROM     SP 
                      WHERE  SP.PNR = 'P2' ) AS POINTLESS;

select 42;
SELECT  DISTINCT S.SNAME
      FROM     S 
                      LEFT JOIN 
                    (SELECT SP.SNR 
                      FROM     SP 
                      WHERE  SP.PNR = 'P2' ) AS POINTLESS 
                      USING  ( SNR );

-- select 43;
-- SELECT  DISTINCT S.SNAME
--       FROM     S 
--                       LEFT JOIN 
--                    (SELECT SP.SNR 
--                      FROM     SP 
--                      WHERE  SP.PNR = 'P2' ) AS POINTLESS 
--                       ON S.SNR = SP.SNR;

select 44;
SELECT  DISTINCT S.SNAME
      FROM     S 
                      NATURAL RIGHT JOIN 
                    (SELECT SP.SNR 
                      FROM     SP 
                      WHERE  SP.PNR = 'P2' ) AS POINTLESS;

select 45;
SELECT  DISTINCT S.SNAME
      FROM     S 
                      RIGHT JOIN 
                    (SELECT SP.SNR 
                      FROM     SP 
                      WHERE  SP.PNR = 'P2' ) AS POINTLESS 
                      USING  ( SNR );

-- select 46;
-- SELECT  DISTINCT S.SNAME
--       FROM     S 
--                       RIGHT JOIN 
--                     (SELECT SP.SNR 
--                       FROM     SP 
--                       WHERE  SP.PNR = 'P2' ) AS POINTLESS 
--                       ON S.SNR = SP.SNR;

select 47;
SELECT  DISTINCT S.SNAME
      FROM     S 
                      NATURAL FULL JOIN 
                    (SELECT  SP.SNR 
                       FROM     SP 
                       WHERE  SP.PNR = 'P2' ) AS POINTLESS;

select 48;
SELECT  DISTINCT S.SNAME
      FROM     S 
                      FULL JOIN 
                    (SELECT SP.SNR 
                      FROM     SP 
                      WHERE  SP.PNR = 'P2' ) AS POINTLESS 
                      USING  ( SNR );

-- select 49;
-- SELECT  DISTINCT S.SNAME
--       FROM     S 
--                       FULL JOIN 
--                     (SELECT SP.SNR 
--                       FROM     SP 
--                       WHERE  SP.PNR = 'P2' ) AS POINTLESS 
--                       ON S.SNR = SP.SNR;

select 50;
SELECT  DISTINCT S.SNAME
      FROM  (  ( SELECT S.SNR FROM S )
                      INTERSECT 
                      (SELECT SP.SNR FROM SP
                        WHERE SP.PNR = 'P2' ) ) AS POINTLESS
                      NATURAL JOIN S;

select 51;
SELECT  DISTINCT S.SNAME
      FROM  (  ( SELECT * FROM S )
                       INTERSECT CORRESPONDING 
                       (SELECT * FROM SP
                         WHERE SP.PNR = 'P2' ) ) AS POINTLESS
                       NATURAL JOIN S;

select 52;
SELECT  DISTINCT S.SNAME
      FROM  (  ( SELECT * FROM S )
                       INTERSECT CORRESPONDING BY ( SNR )
                       (SELECT * FROM SP
                         WHERE SP.PNR = 'P2' ) ) AS POINTLESS
                       NATURAL JOIN S;

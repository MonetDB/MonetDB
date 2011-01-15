-- Subselection queries on the 3D tomograph.
-- tomo : 256 * 256 * 154 cells
-- Selectivities tested 0.5% 1% 2% 5% 10% 20% 50% 100%

-- 0.5%  
SELECT x, y, z, val FROM tomo 
WHERE x BETWEEN 20 AND 70 AND 
      y BETWEEN 10 AND 40 AND 
      z BETWEEN 30 AND 63;

-- 1%  
SELECT x, y, z, val FROM tomo 
WHERE x BETWEEN 20 AND 70 AND 
      y BETWEEN 10 AND 69 AND 
      z BETWEEN 30 AND 63;

-- 2%  
SELECT x, y, z, val FROM tomo 
WHERE x BETWEEN 20 AND 70 AND 
      y BETWEEN 10 AND 128 AND 
      z BETWEEN 30 AND 63;

-- 5% 
SELECT x, y, z, val FROM tomo 
WHERE x BETWEEN 20 AND 79 AND 
      y BETWEEN 10 AND 128 AND 
      z BETWEEN 30 AND 101;

-- 10% (Only query from paper...)
SELECT x, y, z, val FROM tomo 
WHERE x BETWEEN 20 AND 138 AND 
      y BETWEEN 10 AND 128 AND 
      z BETWEEN 30 AND 101;

-- 20% 
SELECT x, y, z, val FROM tomo 
WHERE x BETWEEN 20 AND 138 AND 
      y BETWEEN 10 AND 246 AND 
      z BETWEEN 30 AND 101;

-- 50%
SELECT x, y, z, val FROM tomo 
WHERE x BETWEEN 10 AND 240 AND 
      y BETWEEN 10 AND 230 AND 
      z BETWEEN 10 AND 109;

-- 100%
SELECT x, y, z, val FROM tomo 
WHERE x BETWEEN 0 AND 255 AND 
      y BETWEEN 0 AND 255 AND 
      z BETWEEN 0 AND 153;


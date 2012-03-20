# For now, disable mitosis for all SciQL GROUP BY queries, because mitosis
# changes the HEAD of a BAT, which in turn breaks the array AGGR
# implementations.  Fixing this might require extension at the MAL or even GDK
# level, postponed ...

set optimizer='no_mitosis_pipe';

CREATE ARRAY ary(x INT DIMENSION[4], y INT DIMENSION[-5], v FLOAT DEFAULT 3.7);

WITH a AS (SELECT * FROM ary[1:3]) SELECT x, y, SUM(v), AVG(v) FROM a GROUP BY a[x-1:x+1][y+1:y-1];

WITH a AS (SELECT * FROM ary[1:3]), b AS(SELECT * FROM ary[*][-1:-3]) SELECT x, y, SUM(v), AVG(v) FROM b GROUP BY b[x-1:x+1][y+1:y-1];

WITH a AS (SELECT * FROM ary[1:3]), b AS(SELECT * FROM ary[*][-1:-3]) SELECT x, y, SUM(v), AVG(v) FROM a GROUP BY b[x-1:x+1][y+1:y-1];
WITH a AS (SELECT * FROM ary[1:3]), b AS(SELECT * FROM ary[*][-1:-3]) SELECT x, y, SUM(v), AVG(v) FROM b GROUP BY a[x-1:x+1][y+1:y-1];

DROP ARRAY ary;


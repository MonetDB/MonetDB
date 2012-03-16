set optimizer='no_mitosis_pipe';

CREATE ARRAY ary(x INT DIMENSION[4], y INT DIMENSION[-5], v FLOAT DEFAULT 3.7);

SELECT  x, SUM(v), COUNT(*) FROM (SELECT * FROM ary[1:3]) AS a GROUP BY x;

# The following query returns wrong results with mitosis enabled.
SELECT  x, SUM(v) FROM (SELECT * FROM ary[1:3]) AS a GROUP BY a[x-1:x+1][y+1:y-1];

# The following requires the detection that [y-1:y+1] requires the reverse of
# the 'step' size of the 'y' dimension.
# Such detection can be implemented for the most static cases, i.e., <column>
# -/+ <atom>
#SELECT  x, SUM(v) FROM (SELECT * FROM ary[1:3]) AS a GROUP BY a[x-1:x+1][y-1:y+1];

DROP ARRAY ary;


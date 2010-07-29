WITH t2 (i) AS (SELECT ROW_NUMBER () OVER (ORDER BY id ASC) AS i FROM tables) select i from t2;

WITH t (i) AS (SELECT ROW_NUMBER () OVER (ORDER BY id ASC) AS i FROM _tables) select i from t;


-- test_aggregate2.ram
SELECT i2,sum(v) AS v FROM 
	(SELECT I0.i0,I0.i1,I0.i2,A.v AS v FROM 
		(SELECT A0.i0,A0.i1,(A0.v+A1.v) AS v FROM 
			(SELECT i0,i1,i0 AS v FROM 
				(SELECT S0.x AS i0,S1.x AS i1 FROM (select * from N where x <10) AS S0,(select * from N where x <10) AS S1 
				) AS base 
			) AS A0,
			(SELECT i0,i1,i1 AS v FROM 
				(SELECT S0.x AS i0,S1.x AS i1 FROM (select * from N where x <10) AS S0,(select * from N where x <10) AS S1 		
				) AS base 
			) AS A1 
				WHERE A0.i0 = A1.i0 AND A0.i1 = A1.i1 
		) AS A,
		(SELECT i0,i1,i2,i0 AS v FROM 
			(SELECT S0.x AS i0,S1.x AS i1,S2.x AS i2 FROM (select * from N where x <10) AS S0,(select * from N where x <10) AS S1,(select * from N where x <5) AS S2 
			) AS base 
		) AS I0,
		(SELECT i0,i1,i2,i1 AS v FROM 
			(SELECT S0.x AS i0,S1.x AS i1,S2.x AS i2 FROM (select * from N where x <10) AS S0,(select * from N where x <10) AS S1,(select * from N where x <5) AS S2 
			) AS base 
		) AS I1 
			WHERE I0.v = A.i0 AND I1.v = A.i1 
	) AS A
GROUP BY i2 ;

-- test_aggregate.ram
SELECT i1,sum(v) AS v FROM 
	(SELECT I0.i0,I0.i1,A.v AS v FROM 
		(SELECT i0,i0 AS v FROM 
			(SELECT S0.x AS i0 FROM (SELECT * from N where x <10) AS S0 
			) AS base 
		) AS A,
		(SELECT i0,i1,i0 AS v FROM 
			(SELECT S0.x AS i0,S1.x AS i1 FROM (SELECT * from N where x <10) AS S0,(SELECT * from N where x <5) AS S1 
			) AS base
		) AS I0 
			WHERE I0.v = A.i0 
	) AS A 
GROUP BY i1 ;

-- test_const.ram
SELECT i0,1 AS v FROM 
	(SELECT S0.x AS i0 FROM (SELECT * from N where x <10) AS S0 
	) AS base ;

-- test_nesting_1.ram
SELECT I0.i0,I0.i1,A.v AS v FROM 
	(SELECT i0,i0 AS v FROM 
		(SELECT S0.x AS i0 FROM (SELECT * from N where x < 10) AS S0 
		) AS base 
	) AS A,
	(SELECT i0,i1,i0 AS v FROM 
		(SELECT S0.x AS i0,S1.x AS i1 FROM (SELECT * from N where x < 10) AS S0,(SELECT * from N where x < 5) AS S1 
		) AS base 
	) AS I0 	
		WHERE I0.v = A.i0 ;

-- test_nesting_2.ram
SELECT i0,i1,i1 AS v FROM 
	(SELECT S0.x AS i0,S1.x AS i1 FROM (SELECT * from N where x < 10) AS S0,(SELECT * from N where x < 5) AS S1 
	) AS base ;

-- test_nesting_3.ram
SELECT I0.i0,A.v AS v FROM (SELECT i0,i0 AS v FROM (SELECT S0.x AS i0 FROM
(SELECT * from N where x < 10) AS S0 ) AS base ) AS A,(SELECT i0,i0 AS v FROM (SELECT S0.x AS i0 FROM
(SELECT * from N where x < 5) AS S0 ) AS base ) AS I0 WHERE I0.v = A.i0 ;

-- test_nesting_7.ram
SELECT i1,sum(v) AS v FROM (SELECT I0.i0,I0.i1,A.v AS v FROM (SELECT i0,i0 AS v
FROM (SELECT S0.x AS i0 FROM (SELECT * from N where x < 10) AS S0 ) AS base ) AS A,(SELECT i0,i1,i0 AS
v FROM (SELECT S0.x AS i0,S1.x AS i1 FROM (SELECT * from N where x < 10) AS S0,(SELECT * from N where x < 5) AS S1 ) AS base
) AS I0 WHERE I0.v = A.i0 ) AS A GROUP BY i1 ;

-- test_nesting_8.ram
SELECT i1,sum(v) AS v FROM (SELECT I0.i0,I0.i1,A.v AS v FROM (SELECT i0,i1,i1 AS
v FROM (SELECT S0.x AS i0,S1.x AS i1 FROM (SELECT * from N where x < 10) AS S0,(SELECT * from N where x < 10) AS S1 ) AS
base ) AS A,(SELECT i0,i1,i0 AS v FROM (SELECT S0.x AS i0,S1.x AS i1 FROM
(SELECT * from N where x < 10) AS S0,(SELECT * from N where x < 5) AS S1 ) AS base ) AS I0,(SELECT i0,i1,i0 AS v FROM
(SELECT S0.x AS i0,S1.x AS i1 FROM (SELECT * from N where x < 10) AS S0,(SELECT * from N where x < 5) AS S1 ) AS base ) AS
I1 WHERE I0.v = A.i0 AND I1.v = A.i1 ) AS A GROUP BY i1 ;

-- test_opt_consttrans.ram
SELECT i0,'X' AS v FROM (SELECT S0.x AS i0 FROM (SELECT * from N where x < 5) AS S0 ) AS base ;

-- test_opt_pushmapdown.ram
SELECT I0.i0,I0.i1,A.v AS v FROM (SELECT A0.i0,(A0.v+A1.v) AS v FROM (SELECT
i0,i0 AS v FROM (SELECT S0.x AS i0 FROM (SELECT * from N where x < 10) AS S0 ) AS base ) AS A0,(SELECT
i0,i0 AS v FROM (SELECT S0.x AS i0 FROM (SELECT * from N where x < 10) AS S0 ) AS base ) AS A1 WHERE
A0.i0 = A1.i0 ) AS A,(SELECT i0,i1,i0 AS v FROM (SELECT S0.x AS i0,S1.x AS i1
FROM (SELECT * from N where x < 10) AS S0,(SELECT * from N where x < 10) AS S1 ) AS base ) AS I0 WHERE I0.v = A.i0 ;

-- test_opt_pushmapup.ram
SELECT A0.i0,(A0.v+A1.v) AS v FROM (SELECT I0.i0,A.v AS v FROM (SELECT i0,i0 AS
v FROM (SELECT S0.x AS i0 FROM (SELECT * from N where x < 10) AS S0 ) AS base ) AS A,(SELECT i0,i0 AS
v FROM (SELECT S0.x AS i0 FROM (SELECT * from N where x < 1) AS S0 ) AS base ) AS I0 WHERE I0.v = A.i0
) AS A0,(SELECT I0.i0,A.v AS v FROM (SELECT i0,i0 AS v FROM (SELECT S0.x AS i0
FROM (SELECT * from N where x < 10) AS S0 ) AS base ) AS A,(SELECT i0,i0 AS v FROM (SELECT S0.x AS i0
FROM (SELECT * from N where x < 1) AS S0 ) AS base ) AS I0 WHERE I0.v = A.i0 ) AS A1 WHERE A0.i0 =
A1.i0 ;

-- test_simple_1.ram
SELECT i0,i0 AS v FROM (SELECT S0.x AS i0 FROM (SELECT * from N where x < 10) AS S0 ) AS base ;

-- test_simple_3.ram
SELECT A0.i0,A0.i1,(A0.v+A1.v) AS v FROM (SELECT i0,i1,i1 AS v FROM (SELECT
S0.x AS i0,S1.x AS i1 FROM (SELECT * from N where x < 10) AS S0,(SELECT * from N where x < 10) AS S1 ) AS base ) AS
A0,(SELECT i0,i1,i0 AS v FROM (SELECT S0.x AS i0,S1.x AS i1 FROM (SELECT * from N where x < 10) AS
S0,(SELECT * from N where x < 10) AS S1 ) AS base ) AS A1 WHERE A0.i0 = A1.i0 AND A0.i1 = A1.i1 ;

-- test_simple_4.ram
SELECT A0.i0,(A0.v+A1.v) AS v FROM (SELECT I0.i0,A.v AS v FROM (SELECT i0,i1,i0
AS v FROM (SELECT S0.x AS i0,S1.x AS i1 FROM (SELECT * from N where x < 10) AS S0,(SELECT * from N where x < 10) AS S1 ) AS
base ) AS A,(SELECT i0,i0 AS v FROM (SELECT S0.x AS i0 FROM (SELECT * from N where x < 10) AS S0 ) AS
base ) AS I0,(SELECT i0,i0 AS v FROM (SELECT S0.x AS i0 FROM (SELECT * from N where x < 10) AS S0 ) AS
base ) AS I1 WHERE I0.v = A.i0 AND I1.v = A.i1 ) AS A0,(SELECT I0.i0,A.v AS v
FROM (SELECT i0,i1,i1 AS v FROM (SELECT S0.x AS i0,S1.x AS i1 FROM (SELECT * from N where x < 10) AS
S0,(SELECT * from N where x < 10) AS S1 ) AS base ) AS A,(SELECT i0,i0 AS v FROM (SELECT S0.x AS i0
FROM (SELECT * from N where x < 10) AS S0 ) AS base ) AS I0,(SELECT i0,i0 AS v FROM (SELECT S0.x AS i0
FROM (SELECT * from N where x < 10) AS S0 ) AS base ) AS I1 WHERE I0.v = A.i0 AND I1.v = A.i1 ) AS A1
WHERE A0.i0 = A1.i0 ;

-- test_simple_5.ram
SELECT I0.i0,I0.i1,A.v AS v FROM (SELECT i0,i0 AS v FROM (SELECT S0.x AS i0 FROM
(SELECT * from N where x < 10) AS S0 ) AS base ) AS A,(SELECT i0,i1,i0 AS v FROM (SELECT S0.x AS
i0,S1.x AS i1 FROM (SELECT * from N where x < 10) AS S0,(SELECT * from N where x < 10) AS S1 ) AS base ) AS I0 WHERE I0.v =
A.i0 ;


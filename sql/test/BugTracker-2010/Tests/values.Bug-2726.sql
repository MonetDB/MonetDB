SELECT t0.a
     FROM (VALUES (1), (3)) AS t0(a),
          (VALUES (1), (3)) AS t1(b)
	    WHERE a = b;

SELECT a
     FROM (VALUES (1), (3)) AS t0(a),
          (VALUES (1), (3)) AS t1(b)
	    WHERE a = b;




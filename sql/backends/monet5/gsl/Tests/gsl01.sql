SELECT sys.chi2prob(20.0, 5.0);

SELECT sys.chi2prob(20.0, NULL);

SELECT sys.chi2prob(NULL, 5.0);

SELECT sys.chi2prob(-1, 5.0);

SELECT sys.chi2prob(20.0, 1.0);

CREATE TABLE chi2(a double, b double);

INSERT INTO chi2 VALUES (20.0, 5.0),
       	    	 	(22.0, 4.0),
			(20.0, 6.0);

SELECT chi2prob(a, b) FROM chi2;

SELECT chi2prob(a, 6.0) FROM chi2;

SELECT chi2prob(19.0, b) FROM chi2;

INSERT INTO chi2 VALUES (20.0, NULL);

SELECT chi2prob(a, b) FROM chi2;

SELECT chi2prob(a, 6.0) FROM chi2;

SELECT chi2prob(19.0, b) FROM chi2;

DELETE FROM chi2;

INSERT INTO chi2 VALUES (20.0, 5.0),
       	    	 	(22.0, 4.0),
			(20.0, 6.0),
                        (NULL, 5.0);

SELECT chi2prob(a, b) FROM chi2;

SELECT chi2prob(a, 6.0) FROM chi2;

SELECT chi2prob(19.0, b) FROM chi2;

DELETE FROM chi2;

INSERT INTO chi2 VALUES (20.0, 5.0),
       	    	 	(22.0, 4.0),
			(20.0, 6.0),
                        (-1, 5.0);

SELECT chi2prob(a, b) FROM chi2;

SELECT chi2prob(a, 6.0) FROM chi2;

SELECT chi2prob(19.0, b) FROM chi2;

DELETE FROM chi2;

INSERT INTO chi2 VALUES (20.0, 5.0),
       	    	 	(22.0, 4.0),
			(20.0, 6.0),
                        (20.0, 1.0);

SELECT chi2prob(a, b) FROM chi2;

SELECT chi2prob(a, 6.0) FROM chi2;

SELECT chi2prob(19.0, b) FROM chi2;


CREATE TABLE RX (
	    u int NOT NULL,
	    v int NOT NULL,
	    r int NOT NULL
);

CREATE TABLE trans (
	    s int NOT NULL, 
	    t int NOT NULL, 
	    comp int NOT NULL         
);

CREATE INDEX trans_st_idx ON trans (s, t);


INSERT INTO trans(s, t, comp) VALUES
(1, 2, 31),
(1, 16, 31),
(1, 3, 255),
(255, 3, 255);

INSERT INTO RX (u, v, r) VALUES
(0, 1, 1),
(1, 2, 3),
(2, 4, 3),
(1, 4, 16),
(1, 3, 2),
(3, 2, 255);


SELECT TR.x, TR.z, comp
FROM 
(SELECT TR1.u as x, TR1.v as y, TR2.v as z, TR1.r as rxy, TR2.r as ryz
	    FROM
	        RX as TR1 JOIN RX as TR2 
		    ON (TR1.v = TR2.u AND TR1.u <> TR2.v)
	) as TR                         
	    JOIN 
	 trans
	ON (TR.rxy = s AND TR.ryz = t)
 order by TR.x, TR.z;

Drop index trans_st_idx;
Drop table trans;
Drop table rx;

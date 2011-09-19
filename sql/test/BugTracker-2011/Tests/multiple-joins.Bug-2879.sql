START TRANSACTION;

CREATE TABLE triples (
  s varchar(255),
  p varchar(255),
  o varchar(255),
  PRIMARY KEY (s, p, o)
);

INSERT INTO triples (s, p, o) VALUES
('<http://example/a>', '<http://example/q1>', '<http://example/z11>'),
('<http://example/a>', '<http://example/q1>', '<http://example/z12>'),
('<http://example/a>', '<http://example/q2>', '<http://example/z21>'),
('<http://example/a>', '<http://example/q2>', '<http://example/z22>'),
('<http://example/b>', '<http://example/q2>', '<http://example/y21>'),
('<http://example/b>', '<http://example/q2>', '<http://example/y22>'),
('<http://example/c>', '<http://example/r>', '<http://example/rr>'),
('<http://example/x>', '<http://example/p>', '<http://example/a>'),
('<http://example/x>', '<http://example/p>', '<http://example/b>'),
('<http://example/x>', '<http://example/p>', '<http://example/c>');

SELECT M_1.VC_4 AS V_1, M_1.VC_3 AS V_2, M_1.VC_1 AS V_3, M_1.VC_2 AS V_4
FROM
  ( SELECT COALESCE(T_2.o, T_4.o) AS VC_1, COALESCE(T_3.o, T_5.o) AS VC_2,
T_1.o AS VC_3, T_1.s AS VC_4
      FROM
        ( SELECT *
          FROM Triples AS T_1
          WHERE ( T_1.p = '<http://example/p>' )
        ) AS T_1
      LEFT OUTER JOIN
          Triples AS T_2
        INNER JOIN
          Triples AS T_3
        ON ( T_2.p = '<http://example/q1>'
         AND T_3.p = '<http://example/q2>'
         AND T_2.s = T_3.s )
      ON ( T_1.o = T_2.s )
      LEFT OUTER JOIN
          Triples AS T_4
        INNER JOIN
          Triples AS T_5
        ON ( T_4.p = '<http://example/q2>'
         AND T_5.p = '<http://example/q2>'
         AND T_4.s = T_5.s )
      ON ( T_1.o = T_4.s
       AND ( ( T_2.o IS NULL ) OR ( T_2.o = T_4.o ) )
       AND ( ( T_3.o IS NULL ) OR ( T_3.o = T_5.o ) ) )
    ) AS M_1
ORDER BY V_1, V_2, V_3, V_4;

ROLLBACK;

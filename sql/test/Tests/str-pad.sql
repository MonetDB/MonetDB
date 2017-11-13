SELECT '#' || lpad('hi', 7) || '#';
SELECT '#' || lpad('hixyäbcdef', 7) || '#';
SELECT '#' || lpad('hi', 7, 'xya') || '#';
SELECT '#' || lpad('hi', 7, 'xyä') || '#';
SELECT '#' || lpad('hi', 7, 'xy颖äbcdef') || '#';
SELECT '#' || lpad('hi颖xyäbcdef', 7, 'lmn') || '#';

SELECT '#' || rpad('hi', 7) || '#';
SELECT '#' || rpad('hixyäbcdef', 7) || '#';
SELECT '#' || rpad('hi', 7, 'xya') || '#';
SELECT '#' || rpad('hi', 7, 'xyä') || '#';
SELECT '#' || rpad('hi', 7, 'xy颖äbcdef') || '#';
SELECT '#' || rpad('hi颖xyäbcdef', 7, 'lmn') || '#';

CREATE TABLE p (s VARCHAR(20), n int);
INSERT INTO p VALUES ('hi', 10), ('hixyäbcdef', 7);
SELECT '#' || lpad(s, 5) || '#' FROM p;
SELECT '#' || lpad(s, n) || '#' FROM p;

SELECT '#' || rpad(s, 5) || '#' FROM p;
SELECT '#' || rpad(s, n) || '#' FROM p;

CREATE TABLE p2 (s VARCHAR(20), n int, s2 VARCHAR(10));
INSERT INTO p2 VALUES ('hi', 6, 'xya'), ('hi', 7, 'xyä'), ('hi', 8, 'xy颖äbcdef'), ('hi颖xyäbcdef', 9, 'lmn');
SELECT '#' || lpad(s, 10, 'x') || '#' FROM p2;
SELECT '#' || lpad(s, n, 'x') || '#' FROM p2;
SELECT '#' || lpad(s, 10, s2) || '#' FROM p2;
SELECT '#' || lpad(s, n, s2) || '#' FROM p2;

SELECT '#' || rpad(s, 10, 'x') || '#' FROM p2;
SELECT '#' || rpad(s, n, 'x') || '#' FROM p2;
SELECT '#' || rpad(s, 10, s2) || '#' FROM p2;
SELECT '#' || rpad(s, n, s2) || '#' FROM p2;

DROP TABLE p;
DROP TABLE p2;

-- see bug 6414
SELECT '#' || rpad('hi颖xyäbcdef', 0) || '#';
SELECT '#' || lpad('hi颖xyäbcdef', 0) || '#';
SELECT '#' || rpad('hi颖xyäbcdef', 0, 'junk') || '#';
SELECT '#' || lpad('hi颖xyäbcdef', 0, 'junk') || '#';

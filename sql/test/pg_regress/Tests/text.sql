--
-- TEXT
--

SELECT cast ('this is a text string' AS text) = cast('this is a text string' AS text) AS "true";

SELECT cast ('this is a text string' AS text) = cast('this is a text strin' AS text) AS "false";

CREATE TABLE TEXT_TBL (f1 text);

INSERT INTO TEXT_TBL VALUES ('doh!');
INSERT INTO TEXT_TBL VALUES ('hi de ho neighbor');

SELECT '' AS two, * FROM TEXT_TBL;

-- cleanup
DROP TABLE TEXT_TBL;

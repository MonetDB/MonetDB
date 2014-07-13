-- http://michael.otacoo.com/postgresql-2/postgres-9-3-feature-highlight-json-operators/
CREATE TABLE jsonTbl (a int, b json);

INSERT INTO jsonTbl  VALUES (1, '{"f1":1,"f2":true,"f3":"Hi I''m \\"Daisy\\""}');
INSERT INTO jsonTbl VALUES (2, '{"f1":{"f11":11,"f12":12},"f2":2}');
INSERT INTO jsonTbl VALUES (3, '{"f1":[1,"Robert \\"M\\"",true],"f2":[2,"Kevin \\"K\\"",false]}');

--The first operator is “->”, that can be used to fetch field values directly from JSON data. It can be used with a text value identifying the key of field.
-- SELECT b->'f1' AS f1, b->'f3' AS f3 FROM jsonTbl WHERE a = 1;
SELECT  json.filter(b,'f1') AS f1, json.filter(b,'f3') FROM jsonTbl WHERE a =1;

--Multiple keys can also be used in chain to retrieve data or another JSON subset of data.
-- SELECT b->'f1'->'f12' AS f12 FROM jsonTbl WHERE a = 2;
 SELECT json.filter(b,'f1.f12') AS f12 FROM jsonTbl WHERE a = 2;

--When an integer is used as key, you can fetch data directly in a stored array, like that for example:
-- SELECT b->'f1'->0 as f1_0 FROM jsonTbl WHERE a = 3;
SELECT json.filter(b,'f1[0]') as f1_0 FROM jsonTbl WHERE a = 3;

--The second operator added is “->>”. Contrary to “->” that returns a JSON legal text, “->>” returns plain text.
SELECT  json.filter(b,'f3') AS f1, json.text(json.filter(b,'f3')) FROM jsonTbl WHERE a =1;
SELECT json.filter(b,'f1[0]') as f1_0 , json.text(json.filter(b,'f1[0]'))FROM jsonTbl WHERE a = 3;

drop table jsonTbl;

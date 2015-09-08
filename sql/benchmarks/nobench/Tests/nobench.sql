-- The Nobench snippet based on http://pages.cs.wisc.edu/~chasseur/pubs/argo-long.pdf

create table bench10(js json);

copy 10 records into bench10 from stdin;
{"nested_obj": {"num": 4, "str": "GBRDCMBQ"}, "dyn2": true, "dyn1": 9, "nested_arr": ["especially"], "str2": "GBRDCMBQ", "str1": "GBRDCMBQGE======", "sparse_093": "GBRDCMBQGE======", "thousandth": 9, "sparse_090": "GBRDCMBQGE======", "sparse_091": "GBRDCMBQGE======", "sparse_092": "GBRDCMBQGE======", "num": 9, "bool": true, "sparse_095": "GBRDCMBQGE======", "sparse_096": "GBRDCMBQGE======", "sparse_097": "GBRDCMBQGE======", "sparse_098": "GBRDCMBQGE======", "sparse_094": "GBRDCMBQGE======", "sparse_099": "GBRDCMBQGE======"}
{"nested_obj": {"num": 2, "str": "GBRDCMA="}, "dyn2": "GBRDCMJR", "dyn1": 7, "nested_arr": ["its", "for", "if", "he", "questions", "to", "put"], "str2": "GBRDCMA=", "str1": "GBRDCMJR", "sparse_079": "GBRDCMJR", "thousandth": 7, "sparse_078": "GBRDCMJR", "num": 7, "bool": true, "sparse_072": "GBRDCMJR", "sparse_073": "GBRDCMJR", "sparse_070": "GBRDCMJR", "sparse_071": "GBRDCMJR", "sparse_076": "GBRDCMJR", "sparse_077": "GBRDCMJR", "sparse_074": "GBRDCMJR", "sparse_075": "GBRDCMJR"}
{"nested_obj": {"num": 3, "str": "GBRDCMI="}, "dyn2": 8, "dyn1": 8, "nested_arr": [], "str2": "GBRDCMI=", "str1": "GBRDCMBQGA======", "thousandth": 8, "sparse_087": "GBRDCMBQGA======", "sparse_086": "GBRDCMBQGA======", "sparse_085": "GBRDCMBQGA======", "num": 8, "bool": false, "sparse_082": "GBRDCMBQGA======", "sparse_081": "GBRDCMBQGA======", "sparse_080": "GBRDCMBQGA======", "sparse_083": "GBRDCMBQGA======", "sparse_084": "GBRDCMBQGA======", "sparse_089": "GBRDCMBQGA======", "sparse_088": "GBRDCMBQGA======"}
{"nested_obj": {"num": 0, "str": "GBRDA==="}, "dyn2": 5, "dyn1": 5, "nested_arr": ["the", "state", "aware", "''", "the"], "str2": "GBRDA===", "str1": "GBRDCMBR", "sparse_058": "GBRDCMBR", "thousandth": 5, "num": 5, "bool": true, "sparse_059": "GBRDCMBR", "sparse_054": "GBRDCMBR", "sparse_055": "GBRDCMBR", "sparse_056": "GBRDCMBR", "sparse_057": "GBRDCMBR", "sparse_050": "GBRDCMBR", "sparse_051": "GBRDCMBR", "sparse_052": "GBRDCMBR", "sparse_053": "GBRDCMBR"}
{"nested_obj": {"num": 8, "str": "GBRDCMBQGA======"}, "sparse_035": "GBRDCMI=", "dyn2": true, "dyn1": 3, "nested_arr": ["check", "it", "the"], "str2": "GBRDCMBQGA======", "str1": "GBRDCMI=", "thousandth": 3, "sparse_036": "GBRDCMI=", "sparse_037": "GBRDCMI=", "sparse_034": "GBRDCMI=", "num": 3, "bool": true, "sparse_033": "GBRDCMI=", "sparse_030": "GBRDCMI=", "sparse_031": "GBRDCMI=", "sparse_032": "GBRDCMI=", "sparse_038": "GBRDCMI=", "sparse_039": "GBRDCMI="}
{"nested_obj": {"num": 9, "str": "GBRDCMBQGE======"}, "sparse_040": "GBRDCMBQ", "dyn2": "GBRDCMBQ", "dyn1": 4, "nested_arr": ["potatoes", "a", "to", "authentic"], "str2": "GBRDCMBQGE======", "str1": "GBRDCMBQ", "thousandth": 4, "sparse_043": "GBRDCMBQ", "sparse_042": "GBRDCMBQ", "sparse_041": "GBRDCMBQ", "num": 4, "bool": false, "sparse_046": "GBRDCMBQ", "sparse_045": "GBRDCMBQ", "sparse_044": "GBRDCMBQ", "sparse_047": "GBRDCMBQ", "sparse_049": "GBRDCMBQ", "sparse_048": "GBRDCMBQ"}
{"sparse_018": "GBRDC===", "nested_obj": {"num": 6, "str": "GBRDCMJQ"}, "dyn2": "GBRDC===", "dyn1": 1, "nested_arr": ["button-down"], "sparse_010": "GBRDC===", "str2": "GBRDCMJQ", "str1": "GBRDC===", "sparse_013": "GBRDC===", "sparse_014": "GBRDC===", "sparse_015": "GBRDC===", "sparse_016": "GBRDC===", "sparse_017": "GBRDC===", "num": 1, "bool": true, "sparse_011": "GBRDC===", "thousandth": 1, "sparse_019": "GBRDC===", "sparse_012": "GBRDC==="}
{"nested_obj": {"num": 7, "str": "GBRDCMJR"}, "dyn2": 2, "dyn1": 2, "nested_arr": ["she", "beyond"], "str2": "GBRDCMJR", "str1": "GBRDCMA=", "thousandth": 2, "sparse_025": "GBRDCMA=", "sparse_024": "GBRDCMA=", "sparse_027": "GBRDCMA=", "num": 2, "bool": false, "sparse_020": "GBRDCMA=", "sparse_023": "GBRDCMA=", "sparse_022": "GBRDCMA=", "sparse_021": "GBRDCMA=", "sparse_029": "GBRDCMA=", "sparse_028": "GBRDCMA=", "sparse_026": "GBRDCMA="}
{"nested_obj": {"num": 5, "str": "GBRDCMBR"}, "sparse_008": "GBRDA===", "sparse_009": "GBRDA===", "dyn2": false, "dyn1": "GBRDA===", "nested_arr": [], "sparse_007": "GBRDA===", "str2": "GBRDCMBR", "str1": "GBRDA===", "sparse_004": "GBRDA===", "sparse_003": "GBRDA===", "sparse_002": "GBRDA===", "sparse_001": "GBRDA===", "sparse_000": "GBRDA===", "num": 0, "bool": false, "sparse_006": "GBRDA===", "thousandth": 0, "sparse_005": "GBRDA==="}
{"nested_obj": {"num": 1, "str": "GBRDC==="}, "dyn2": false, "dyn1": 6, "nested_arr": ["to", "interrupted", "some", "daily", "has", "averages"], "str2": "GBRDC===", "str1": "GBRDCMJQ", "thousandth": 6, "sparse_069": "GBRDCMJQ", "sparse_068": "GBRDCMJQ", "num": 6, "bool": false, "sparse_061": "GBRDCMJQ", "sparse_060": "GBRDCMJQ", "sparse_063": "GBRDCMJQ", "sparse_062": "GBRDCMJQ", "sparse_065": "GBRDCMJQ", "sparse_064": "GBRDCMJQ", "sparse_067": "GBRDCMJQ", "sparse_066": "GBRDCMJQ"}

select count(*) from bench10;
--Q1:
 -- MongoDB:
--    db["nobench_main"].find({}, ["str1", "num"])
--  Argo/SQL:
--    SELECT str1, num FROM nobench_main;
select json.filter(js, '$..str1,$..num') from bench10;

-- Q2
--  MongoDB:
--    db["nobench_main"].find({}, ["nested_obj.str", "nested_obj.num"])
--  Argo/SQL
--    SELECT nested_obj.str, nested_obj.num FROM nobench_main;
select json.filter(js,'.nested_obj.str,.nested_obj.num') from bench10;

-- Q3(replace XX with literal digits):
--  MongoDB:
--    db["nobench_main"].find(
--        { "$or" : [ { "sparse_XX0" : {"$exists" : True} },
--                    { "sparse_XX9" : {"$exists" : True} } ] },
--        ["sparse_XX0", "sparse_XX9"])
--  Argo/SQL:
--    SELECT sparse_XX0, sparse_XX9 FROM nobench_main;
select json.filter(js,'..sparse_000,..sparse_001') from bench10;

--Q4 (replace XX and YY with different literal digits):
--  MongoDB:
--    db["nobench_main"].find(
--        { "$or" : [ { "sparse_XX0" : {"$exists" : True} },
--                    { "sparse_YY0" : {"$exists" : True} } ] },
--        ["sparse_XX0", "sparse_YY0"])
--  Argo/SQL:
--    SELECT sparse_XX0, sparse_YY0 FROM nobench_main;
select json.filter(js,'..sparse_000,..sparse_110') from bench10;

--Q5 (replace XXXXX with a literal string):
--  MongoDB:
--    db["nobench_main"].find({ "str1" : XXXXX })
--  Argo/SQL:
--    SELECT * FROM nobench_main WHERE str1 = XXXXX;
select json.filter(js,'str1') from bench10;
select json.text(json.filter(js,'str1')) from bench10;
select * from bench10 where json.text(json.filter(js,'str1')) = 'GBRDCMBR';

--Q6 (replace XXXXX and YYYYY with literal integers):
--  MongoDB:
--    db["nobench_main"].find({ "$and": [{ "num" : {"$gte" : XXXXX } },
--                                       { "num" : {"$lt"  : YYYYY } }]})
--  Argo/SQL:
--    SELECT * FROM nobench_main WHERE num BETWEEN XXXXX AND YYYYY;
select json.number(json.filter(js,'num')) from bench10;
select json."integer"(json.filter(js,'num')) from bench10;
select cast(json.text(json.filter(js,'num')) as integer) from bench10;
select * from bench10 where json.number(json.filter(js,'num')) between 0.0 and 3.0;

--Q7 (replace XXXXX and YYYYY with literal integers):
--  MongoDB:
--    db["nobench_main"].find({ "$and": [{ "dyn1" : {"$gte" : XXXXX } },
--                                       { "dyn1" : {"$lt"  : YYYYY } }]})
--  Argo/SQL:
--    SELECT * FROM nobench_main WHERE dyn1 BETWEEN XXXXX AND YYYYY;
select json.number(json.filter(js,'dyn1')) from bench10;
select json."integer"(json.filter(js,'dyn1')) from bench10;
select cast(json.text(json.filter(js,'dyn1')) as integer) from bench10;
select * from bench10 where json.number(json.filter(js,'dyn1')) between 0.0 and 3.0;

-- Q8 (replace XXXXX with one of the "suggested" words from data generation):
--   MongoDB:
--     db["nobench_main"].find({ "nested_arr" : XXXXX })
--   Argo/SQL:
--     SELECT * FROM nobench_main WHERE XXXXX = ANY nested_arr;
select json.filter(js,'nested_arr') from bench10;
select * from bench10 where json.text(json.filter(js,'nested_arr')) = 'check it the';

-- Q9 (replace XXX with literal digits and YYYYY with a literal string):
--   MongoDB:
--     db["nobench_main"].find({ "sparse_XXX" : YYYYY })
--   Argo/SQL:
--     SELECT * FROM nobench_main WHERE sparse_XXX = YYYYY;
select * from bench10 where json.text(json.filter(js,'sparse_000')) = 'GBRDA===';

-- Q10 (replace XXXXX and YYYYY with literal integers):
--   MongoDB:
--     db["nobench_main"].group(
--         {"thousandth" : True},
--         {"$and": [{"num" : { "$gte" : XXXXX } },
--                   {"num" : { "$lt"  : YYYYY } }]},
--         { "total" : 0 },
--         "function(obj, prev) { prev.total += 1; }")
--   Argo/SQL:
--     SELECT COUNT(*) FROM nobench_main WHERE num BETWEEN XXXXX AND YYYYY GROUP BY thousandth;
select f.h, count(*) from (
	select json.filter(js,'..thousanth') as g, json.number(json.filter(js,'num')) as h from bench10 where json.number(json.filter(js,'num')) between 0.0 and 3.0 ) as f
group by f.h;

-- Q11 (replace XXXXX and YYYYY with literal integers):
--   MongoDB:
--     Implemented as Javascript MapReduce job.
--   Argo/SQL:
--     SELECT * FROM nobench_main AS left
--                   INNER JOIN nobench_main AS right
--                   ON (left.nested_obj.str = right.str1)
--                   WHERE left.num BETWEEN XXXXX AND YYYYY;
--select * from bench10 as left inner join bench10 as right on (json.filter(left.js,'nested_obj.str') = json.filter(right.js, 'str1'))
--where json.number(json.filter(left.js,'num')) between 0.0 and 3.0;


-- Q12 (use "extra" data file provided by generator):
--   MongoDB:
--     Use mongoimport command-line tool.
--   PostgreSQL:
--     COPY table FROM file;
--   MySQL:
--     LOAD DATA LOCAL INFILE file REPLACE INTO TABLE table;
-- copy into bench10 from 'datafile'.

drop table bench10;

-- The simple examples of SQL/XML publishing functions
-- and an indication of the code to be derived from SQL
-- assume simple string production and bottom up construction.

-- Round 2. Attempt to simplify the code based on the assumption
-- that we have to adjust the SQL parser. Simple masking a function
-- call is not sufficient.

select xmlelement(name "victim", V.name || ' ' || V.dob ) as result
from victim V;

-- SQL code snippet
--    _15 := sql.resultSet(1,1,_14);
--    sql.rsColumn(_15,"sys.","concat_concat_name","clob",1,0,_14);
--    sql.exportResult(_15,"");


-- SQL/XML code snippet
--    _97 := xml.xml(_14); # coercion to xml value
--	  _98 := xml.element("victim",_97);
--    _99 := xml.str(_98); # coercion to string for export
--    _15 := sql.resultSet(1,1,_14);
--    sql.rsColumn(_15,"sys.","result","str",1,0,_99);

select xmlelement(name "victim", 
		xmlattributes(V.name, V.dob ) )
from victim V;

-- SQL code snippet
--    _12 := sql.resultSet(2,1,_9);
--    sql.rsColumn(_12,"sys.v","name","clob",0,0,_9);
--    sql.rsColumn(_12,"sys.v","dob","clob",0,0,_11);

-- SQL/XML code snippet
--	  _96 := xml.attribute("name",_9);
--	  _97 := xml.attribute("dob",_11);
--    _98 := xml.concat(_96,_97);
--	  _99 := xml.element("victim",_98,nil:bat);
--    _00 := xml.str(_99);
--    _12 := sql.resultSet(1,1,_9);
--    sql.rsColumn(_12,"sys","result","str",0,0,_00);

select xmlelement(name "victim", 
		xmlelement(name "name", V.name ),
		xmlelement(name "birthday", V.dob ) )
from victim V;

--   _94:= xml.xml(_8);
--   _95:= xml.xml(_9);
--   _96:= xml.element("name",_94);
--   _97:= xml.element("birthday",_95);
--   _77:= xml.concat(_96,_97);
--   _98:= xml.element("victim",_77);
--   _99:= xml.str(_98);
--   sql.rsColumn(_12,"sys","result","str",0,0,_99);

select xmlelement(name "victim", 
		xmlelement(name "name", V.name ),
		xmlelement(name "hairtype", 
		(select hair from victims where name=V.name)))
from victim V;

-- identical to previous one after evaluatoin of subquery
-- it may produce multiple answers though

select xmlconcat(
		xmlelement(name "name", V.name ),
		xmlelement(name "birthday", V.dob ) )
from victim V;

-- _94 := xml.xml(_8);
-- _95 := xml.xml(_9);
-- _96 := xml.element("name",_94);
-- _97 := xml.element("birthday",_95);
-- _98 := xml.concat(_96,97);
-- _99 := xml.str(_98);
-- sql.rsColumn(_12,"sys","result","str",0,0,_99);

select xmlelement(name "victims",
	xmlforest( V.name, V.dob as "birthday")
	)
from victim V;
-- _94 := xml.xml(_8);
-- _95 := xml.xml(_9);
-- _96 := xml.element("birthday",_95);
-- _97 := xml.concat(_95,96);
-- _98 := xml.element("victims",_97);
-- _99 := xml.str(_98);
-- sql.rsColumn(_12,"sys","result","str",0,0,_99);

-- group by hair color
select xmlelement(name "jtr",
	xmlattributes(V.hair as "hair"),
	xmlagg( xmlelement("victim", V.name))
	)
from victim V;

select xmlelment(name "jtr",
		xmlforest(
			xmlelement(name="dossier",
				xmlattributes(V.hair as "hair"),
				xmlagg( xmlelement(name="person", V.name)),
			)
			xmlelement(name="total", (select count(*) from victim))
		))
from victim V;

-- escape to XQuery
select xmlgen(<person id="{$name"} dob="{$dob}"</person>)
from victim;

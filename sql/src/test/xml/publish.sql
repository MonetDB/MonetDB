-- The simple examples of SQL/XML publishing functions
-- and an indication of the code to be derived from SQL
-- assume simple string production and bottom up construction.

select xmlelement(name "victim", V.name || ' ' || V.dob ) as result
from victim V;

-- SQL code snippet
--    _15 := sql.resultSet(1,1,_14);
--    sql.rsColumn(_15,"sys.","concat_concat_name","clob",1,0,_14);
--    sql.exportResult(_15,"");


-- SQL/XML code snippet
--	  _98 := xml.element("victim",_14);
--	  _99 := xml.document(_14,"result",_98);
--    _15 := sql.resultSet(1,1,_14);
--    sql.rsColumn(_15,"sys.","result","xml",1,0,_99);

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
--	  _98 := xml.element("victim",_96,_97);
--	  _99 := xml.document(_9,"document",_98);
--    _12 := sql.resultSet(1,1,_9);
--    sql.rsColumn(_12,"sys","document","xml",0,0,_99);

select xmlelement(name "victim", 
		xmlelement(name "name", V.name ),
		xmlelement(name "birthday", V.dob ) )
from victim V;

select xmlelement(name "victim", 
		xmlelement(name "name", V.name ),
		xmlelement(name "hairtype", 
		(select hair from victims where name=V.name)))
from victim V;

select xmlconcat(
		xmlelement(name "name", V.name ),
		xmlelement(name "birthday", V.dob ) )
from victim V;

select xmlelement(name "victims",
	xmlforest( V.name, V.dob as "birthday")
	)
from victim V;

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

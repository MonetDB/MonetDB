-- The simple examples of SQL/XML publishing functions

select xmlelement(name "victim", V.name || ' ' V.dob ) as result
from victim V;

select xmlelement(name "victim", 
		xmlattributes(V.name, V.dob ) )
from victim V;

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

-- xmlforest
-- xmlagg

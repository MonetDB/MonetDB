select c.CustId, c.Name as CustName 
from customers c;

select xmlelement(name "Customer", 
       	xmlelement(name "CustId",  c.CustId),
       	xmlelement(name "CustName", c.Name),
        xmlelement(name "City", c.City))
from customers c;

select xmlelement(name "Customer",
	xmlforest(c.CustId, c.Name AS CustName, c.City))
from customers c;

select * 
from Customers c, Projects p
where c.CustId = p.CustId
order by c.CustId, p.ProjId;

select xmlelement(name "CustomerProj", 
	xmlforest(c.CustId, c.Name AS CustName, p.ProjId, p.Name AS ProjName))
from Customers c, Projects p
where c.CustId = p.CustId
order by c.CustId;

select xmlelement(name project, 
	xmlattributes(p.ProjId as "id"),
	xmlforest(c.CustId, c.Name AS CustName, p.ProjId, p.Name AS ProjName))
from Customers c, Projects p
where c.CustId = p.CustId
order by c.CustId;

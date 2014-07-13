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
order by c.CustId, p.ProjId;

select xmlelement(name project, 
	xmlattributes(p.ProjId as "id"),
	xmlforest(c.CustId, c.Name AS CustName, p.ProjId, p.Name AS ProjName))
from Customers c, Projects p
where c.CustId = p.CustId
order by c.CustId, p.ProjId;


select 
  xmlelement(name "Customer",
    xmlattributes(c.CustId as "id"), 
    xmlforest(c.Name as "name", c.City as city),
    xmlelement(name projects, 
      (select xmlagg(xmlelement(name project, 
         xmlattributes(p.ProjId as "id"),
         xmlforest(p.Name as name)))
        from Projects p
       where p.CustId = c.CustId) )) as "customer_projects"
from Customers c
order by c.CustId;

select 
  xmlelement(name "Customer",
  xmlattributes(c.CustId as "id"), 
  xmlcomment('simple comment test'))
from Customers c;


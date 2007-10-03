

create table Customers (
	CustId integer,
	Name varchar(32),
	City varchar(32)
);

insert into Customers values(1, 'WoodWorks', 'Baltimore');
insert into Customers values(2, 'Software Solutions', 'Boston');
insert into Customers values(3, 'Food Supplies', 'New York');
insert into Customers values(4, 'Hardware Shop', 'Washington');
insert into Customers values(5, 'Books Inc', 'New Orleans');

create table Projects (
	ProjId integer, 
	Name varchar(32),
	CustId integer
);

insert into Projects values(1, 'Medusa', 1);
insert into Projects values(2, 'Pegasus', 4);
insert into Projects values(8, 'Typhon', 4);
insert into Projects values(10, 'Sphinx', 5);

select *
from Customers c, Projects p
where c.CustId = p.CustId 
order by c.CustId, p.ProjId;

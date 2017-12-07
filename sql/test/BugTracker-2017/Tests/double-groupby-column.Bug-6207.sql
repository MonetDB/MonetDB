select name,name from sys.functions group by name limit 2;
select name, name from sys.functions group by name,name limit 2;
select f.name, f.name from sys.functions AS f group by name,name limit 2;

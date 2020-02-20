START TRANSACTION;
create table employees (dep varchar(10), name varchar(20), salary double);
insert into employees values
   ('mgmt', 'Smith',81),
   ('dev', 'Jones',55),
   ('sls', 'Williams',55),
   ('sls', 'Taylor',62),
   ('dev', 'Brown',62),
   ('mgmt', 'Davies',84),
   ('sls', 'Evans',87),
   ('sls', 'Wilson',72),
   ('sls', 'Thomas',72),
   ('mgmt', 'Johnson',100);

SELECT dep, name, salary, CUME_DIST() OVER (partition by dep ORDER BY salary) cume_dist_val FROM employees;

SELECT dep, name, salary, CUME_DIST() OVER (PARTITION BY dep) cume_dist_val FROM employees;

ROLLBACK;

start transaction;
create table employees (dep varchar(10), name varchar(20), salary double, hire_date date);
insert into employees values
    ('mgmt', 'Smith',81,str_to_date('10-08-2019', '%d-%m-%Y')),
    ('dev', 'Jones',55,str_to_date('9-08-2019', '%d-%m-%Y')),
    ('sls', 'Williams',55,str_to_date('14-07-2019', '%d-%m-%Y')),
    ('sls', 'Taylor',62,str_to_date('13-08-2019', '%d-%m-%Y')),
    ('dev', 'Brown',62,str_to_date('6-06-2019', '%d-%m-%Y')),
    ('mgmt', 'Davies',84,str_to_date('4-06-2019', '%d-%m-%Y')),
    ('sls', 'Evans',87,str_to_date('1-09-2019', '%d-%m-%Y')),
    ('sls', 'Wilson',72,str_to_date('21-09-2019', '%d-%m-%Y')),
    ('sls', 'Thomas',72,str_to_date('12-06-2019', '%d-%m-%Y')),
    ('mgmt', 'Johnson',100,str_to_date('12-07-2019', '%d-%m-%Y'));
SELECT dep, name, salary, NTILE(4) OVER (ORDER BY hire_date) FROM employees;
SELECT dep, name, salary, NTILE(4) OVER (PARTITION by dep ORDER BY salary) FROM employees;
SELECT dep, name, salary, NTILE(4) OVER (PARTITION BY dep) FROM employees;
rollback;

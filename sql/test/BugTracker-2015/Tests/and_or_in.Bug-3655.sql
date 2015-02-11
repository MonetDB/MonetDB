CREATE TABLE employee ( employee_id INTEGER NOT NULL, manager_id INTEGER NULL);
INSERT INTO employee (employee_id,manager_id) values (50,70);
INSERT INTO employee (employee_id,manager_id) values (60,70);
INSERT INTO employee (employee_id,manager_id) values (70,null);
select employee_id, manager_id from employee where employee_id = 60 and (manager_id in (-1) or -1 in (-1));
drop table employee;

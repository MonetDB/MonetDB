
-- function needed for MonetDB 
CREATE function to_date(d string, dummy string) returns date
begin
	return str_to_date (d, '%d-%m-%Y');
end;


CREATE TABLE departments (
	  department_id   INTEGER CONSTRAINT departments_pk PRIMARY KEY,
	  department_name VARCHAR(14),
	  location        VARCHAR(13)
);

INSERT INTO departments VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO departments VALUES (20,'RESEARCH','DALLAS');
INSERT INTO departments VALUES (30,'SALES','CHICAGO');
INSERT INTO departments VALUES (40,'OPERATIONS','BOSTON');


CREATE TABLE employees (
	  employee_id   INTEGER CONSTRAINT employees_pk PRIMARY KEY,
	  employee_name VARCHAR(10),
	  job           VARCHAR(9),
	  manager_id    INTEGER,
	  hiredate      DATE,
	  salary        DECIMAL(7,2),
	  commission    DECIMAL(7,2),
	  department_id integer CONSTRAINT emp_department_id_fk REFERENCES departments(department_id)
);

INSERT INTO employees VALUES (7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
INSERT INTO employees VALUES (7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO employees VALUES (7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO employees VALUES (7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
INSERT INTO employees VALUES (7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
INSERT INTO employees VALUES (7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30);
INSERT INTO employees VALUES (7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10);
--INSERT INTO employees VALUES (7788,'SCOTT','ANALYST',7566,to_date('13-JUL-87','dd-mm-rr')-85,3000,NULL,20);
INSERT INTO employees VALUES (7788,'SCOTT','ANALYST',7566,to_date('13-6-87','dd-mm-rr')-85,3000,NULL,20);
INSERT INTO employees VALUES (7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10);
INSERT INTO employees VALUES (7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
--INSERT INTO employees VALUES (7876,'ADAMS','CLERK',7788,to_date('13-JUL-87', 'dd-mm-rr')-51,1100,NULL,20);
INSERT INTO employees VALUES (7876,'ADAMS','CLERK',7788,to_date('13-6-87', 'dd-mm-rr')-51,1100,NULL,20);
INSERT INTO employees VALUES (7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30);
INSERT INTO employees VALUES (7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO employees VALUES (7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10);

SELECT department_name, employee_name FROM 
	departments as d JOIN LATERAL (SELECT employee_name FROM   employees e WHERE  e.department_id = d.department_id) as e on TRUE
order by department_name, employee_name;

SELECT department_name, employee_name FROM 
	departments as d, LATERAL (SELECT employee_name FROM   employees e WHERE  e.department_id = d.department_id) as e
order by department_name, employee_name;

create function emps(dep_id integer) returns table(employee_name string)
begin
	return TABLE(select employee_name from employees e where e.department_id = dep_id);
end;

select department_name, employee_name from departments as d, LATERAL emps(d.department_id) order by department_name, employee_name;

DROP FUNCTION emps;
DROP TABLE employees;
DROP TABLE departments;
DROP FUNCTION to_date(string, string);

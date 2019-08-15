-- copied from https://en.wikibooks.org/wiki/Structured_Query_Language/Window_functions

START TRANSACTION;

CREATE TABLE employee (
  -- define columns (name / type / default value / column constraint)
  id             DECIMAL                           PRIMARY KEY,
  emp_name       VARCHAR(20)                       NOT NULL,
  dep_name       VARCHAR(20)                       NOT NULL,
  salary         DECIMAL(7,2)                      NOT NULL,
  age            DECIMAL(3,0)                      NOT NULL,
  -- define table constraints (it's merely an example table)
  CONSTRAINT empoyee_uk UNIQUE (emp_name, dep_name)
);

INSERT INTO employee VALUES ( 1,  'Matthew', 'Management',  4500, 55);
INSERT INTO employee VALUES ( 2,  'Olivia',  'Management',  4400, 61);
INSERT INTO employee VALUES ( 3,  'Grace',   'Management',  4000, 42);
INSERT INTO employee VALUES ( 4,  'Jim',     'Production',  3700, 35);
INSERT INTO employee VALUES ( 5,  'Alice',   'Production',  3500, 24);
INSERT INTO employee VALUES ( 6,  'Michael', 'Production',  3600, 28);
INSERT INTO employee VALUES ( 7,  'Tom',     'Production',  3800, 35);
INSERT INTO employee VALUES ( 8,  'Kevin',   'Production',  4000, 52);
INSERT INTO employee VALUES ( 9,  'Elvis',   'Service',     4100, 40);
INSERT INTO employee VALUES (10,  'Sophia',  'Sales',       4300, 36);
INSERT INTO employee VALUES (11,  'Samantha','Sales',       4100, 38);

COMMIT WORK;


SELECT * FROM employee order by 1;

-- query 1
SELECT id,
       emp_name,
       dep_name,
       -- The functions FIRST_VALUE() and LAST_VALUE() explain itself by their name. They act within the actual frame.
       FIRST_VALUE(id) OVER (PARTITION BY dep_name ORDER BY id) AS frame_first_row,
       LAST_VALUE(id)  OVER (PARTITION BY dep_name ORDER BY id) AS frame_last_row,
       COUNT(*)        OVER (PARTITION BY dep_name ORDER BY id) AS frame_count,
       -- The functions LAG() and LEAD() explain itself by their name. They act within the actual partition.
       LAG(id)         OVER (PARTITION BY dep_name ORDER BY id) AS prev_row,
       LEAD(id)        OVER (PARTITION BY dep_name ORDER BY id) AS next_row
FROM   employee;

-- query 2
SELECT id,
       emp_name,
       dep_name,
       ROW_NUMBER()           OVER (PARTITION BY dep_name ORDER BY id) AS row_number_in_frame,
       NTH_VALUE(emp_name, 2) OVER (PARTITION BY dep_name ORDER BY id) AS second_row_in_frame,
       LEAD(emp_name, 2)      OVER (PARTITION BY dep_name ORDER BY id) AS two_rows_ahead
FROM   employee;

-- query 3  ROWS
SELECT id, dep_name, salary,
       cast(SUM(salary)  OVER  (PARTITION BY dep_name ORDER BY salary
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as DECIMAL(7,2)) as growing_sum,
       cast(SUM(salary)  OVER  (PARTITION BY dep_name ORDER BY salary
                           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as DECIMAL(7,2)) AS sum_over_1or2or3_rows
FROM   employee;

-- query 4  GROUPS
SELECT id, dep_name, salary,
       cast(SUM(salary)  OVER  (PARTITION BY dep_name ORDER BY salary
                           GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as DECIMAL(7,2)) AS sum_over_groups
FROM   employee;

-- query 5  RANGE, adapted RANGE values 100 into 100.0 and 50 into 50.0
SELECT id, dep_name, salary,
       cast(SUM(salary)  OVER  (PARTITION BY dep_name ORDER BY salary
                           RANGE BETWEEN 100.0 PRECEDING AND 50.0 FOLLOWING) as DECIMAL(7,2)) AS sum_over_range
FROM   employee;


-- cleanup
DROP TABLE employee;



-- copied from https://mode.com/sql-tutorial/sql-window-functions/
-- using https://www.capitalbikeshare.com/system-data
/*
    Duration – Duration of trip
    Start Date – Includes start date and time
    End Date – Includes end date and time
    Start Station – Includes starting station name and number
    End Station – Includes ending station name and number
    Bike Number – Includes ID number of bike used for the trip
    Member Type – Indicates whether user was a "registered" member (Annual Member, 30-Day Member or Day Key Member) or a "casual" rider (Single Trip, 24-Hour Pass, 3-Day Pass or 5-Day Pass)
*/
CREATE SCHEMA tutorial;

CREATE TABLE tutorial.dc_bikeshare_q1_2012 (duration_seconds int, start_time timestamp, end_time timestamp, start_terminal varchar(100), end_terminal varchar(100), bike_id_nr int, member_type varchar(16));

INSERT INTO tutorial.dc_bikeshare_q1_2012 VALUES (155, '2012-01-05 12:30:10', '2012-01-05 12:21:05', 'DIEMEN', 'AMS', 1025, 'registered');
INSERT INTO tutorial.dc_bikeshare_q1_2012 VALUES (165, '2012-01-05 12:30:10', '2012-01-05 12:21:15', 'DIEMEN', 'AMS', 2025, 'casual');
INSERT INTO tutorial.dc_bikeshare_q1_2012 VALUES (155, '2012-01-06 16:30:10', '2012-01-06 18:21:05', 'DIEMEN', 'AMS', 1025, 'registered');
INSERT INTO tutorial.dc_bikeshare_q1_2012 VALUES (165, '2012-01-06 16:30:10', '2012-01-06 18:21:15', 'DIEMEN', 'AMS', 2025, 'casual');
INSERT INTO tutorial.dc_bikeshare_q1_2012 VALUES (55, '2012-01-07 10:30:10', '2012-01-07 10:31:05', 'AMS', 'AMS', 1025, 'registered');
INSERT INTO tutorial.dc_bikeshare_q1_2012 VALUES (65, '2012-01-07 10:30:10', '2012-01-07 10:31:15', 'AMS', 'AMS', 2025, 'casual');

SELECT * FROM tutorial.dc_bikeshare_q1_2012;

SELECT duration_seconds,
       cast(SUM(duration_seconds) OVER (ORDER BY start_time) as bigint) AS running_total
  FROM tutorial.dc_bikeshare_q1_2012;

SELECT start_terminal,
       duration_seconds,
       cast(SUM(duration_seconds) OVER
         (PARTITION BY start_terminal ORDER BY start_time) as bigint)
         AS running_total
  FROM tutorial.dc_bikeshare_q1_2012
 WHERE start_time < '2012-01-08';

SELECT start_terminal,
       duration_seconds,
       cast(SUM(duration_seconds) OVER
         (PARTITION BY start_terminal) as bigint) AS start_terminal_total
  FROM tutorial.dc_bikeshare_q1_2012
 WHERE start_time < '2012-01-08';

SELECT start_terminal,
       duration_seconds,
       cast(SUM(duration_seconds) OVER
         (PARTITION BY start_terminal) as bigint) AS running_total,
       COUNT(duration_seconds) OVER
         (PARTITION BY start_terminal) AS running_count,
       AVG(duration_seconds) OVER
         (PARTITION BY start_terminal) AS running_avg
  FROM tutorial.dc_bikeshare_q1_2012
 WHERE start_time < '2012-01-08';
 
SELECT start_terminal,
       duration_seconds,
       cast(SUM(duration_seconds) OVER
         (PARTITION BY start_terminal ORDER BY start_time) as bigint)
         AS running_total,
       COUNT(duration_seconds) OVER
         (PARTITION BY start_terminal ORDER BY start_time)
         AS running_count,
       AVG(duration_seconds) OVER
         (PARTITION BY start_terminal ORDER BY start_time)
         AS running_avg
  FROM tutorial.dc_bikeshare_q1_2012
 WHERE start_time < '2012-01-08';

SELECT start_terminal,
       start_time,
       duration_seconds,
       ROW_NUMBER() OVER (ORDER BY start_time)
                    AS row_number
  FROM tutorial.dc_bikeshare_q1_2012
 WHERE start_time < '2012-01-08';

SELECT start_terminal,
       start_time,
       duration_seconds,
       ROW_NUMBER() OVER (PARTITION BY start_terminal
                          ORDER BY start_time)
                    AS row_number
  FROM tutorial.dc_bikeshare_q1_2012
 WHERE start_time < '2012-01-08';

SELECT start_terminal,
       duration_seconds,
       RANK() OVER (PARTITION BY start_terminal
                    ORDER BY start_time)
              AS rank
  FROM tutorial.dc_bikeshare_q1_2012
 WHERE start_time < '2012-01-08';

SELECT start_terminal,
       duration_seconds,
       NTILE(4) OVER
         (PARTITION BY start_terminal ORDER BY duration_seconds)
          AS quartile,
       NTILE(5) OVER
         (PARTITION BY start_terminal ORDER BY duration_seconds)
         AS quintile,
       NTILE(100) OVER
         (PARTITION BY start_terminal ORDER BY duration_seconds)
         AS percentile
  FROM tutorial.dc_bikeshare_q1_2012
 WHERE start_time < '2012-01-08'
 ORDER BY start_terminal, duration_seconds;

SELECT start_terminal,
       duration_seconds,
       LAG(duration_seconds, 1) OVER
         (PARTITION BY start_terminal ORDER BY duration_seconds) AS lag,
       LEAD(duration_seconds, 1) OVER
         (PARTITION BY start_terminal ORDER BY duration_seconds) AS lead
  FROM tutorial.dc_bikeshare_q1_2012
 WHERE start_time < '2012-01-08'
 ORDER BY start_terminal, duration_seconds;

SELECT start_terminal,
       duration_seconds,
       duration_seconds -LAG(duration_seconds, 1) OVER
         (PARTITION BY start_terminal ORDER BY duration_seconds)
         AS difference
  FROM tutorial.dc_bikeshare_q1_2012
 WHERE start_time < '2012-01-08'
 ORDER BY start_terminal, duration_seconds;

SELECT *
  FROM (
    SELECT start_terminal,
           duration_seconds,
           duration_seconds -LAG(duration_seconds, 1) OVER
             (PARTITION BY start_terminal ORDER BY duration_seconds)
             AS difference
      FROM tutorial.dc_bikeshare_q1_2012
     WHERE start_time < '2012-01-08'
     ORDER BY start_terminal, duration_seconds
       ) sub
 WHERE sub.difference IS NOT NULL;

SELECT start_terminal,
       duration_seconds,
       NTILE(4) OVER
         (PARTITION BY start_terminal ORDER BY duration_seconds)
         AS quartile,
       NTILE(5) OVER
         (PARTITION BY start_terminal ORDER BY duration_seconds)
         AS quintile,
       NTILE(100) OVER
         (PARTITION BY start_terminal ORDER BY duration_seconds)
         AS percentile
  FROM tutorial.dc_bikeshare_q1_2012
 WHERE start_time < '2012-01-08'
 ORDER BY start_terminal, duration_seconds;

SELECT start_terminal,
       duration_seconds,
       NTILE(4) OVER ntile_window AS quartile,
       NTILE(5) OVER ntile_window AS quintile,
       NTILE(100) OVER ntile_window AS percentile
  FROM tutorial.dc_bikeshare_q1_2012
WINDOW ntile_window AS
         (PARTITION BY start_terminal ORDER BY duration_seconds)
 WHERE start_time < '2012-01-08'
 ORDER BY start_terminal, duration_seconds;

-- cleanup
DROP TABLE tutorial.dc_bikeshare_q1_2012;
DROP SCHEMA tutorial;


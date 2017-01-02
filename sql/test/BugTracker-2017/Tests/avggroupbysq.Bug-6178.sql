START TRANSACTION;

CREATE TABLE x (tailnum STRING, arr_delay INTEGER);

COPY 2 RECORDS INTO x FROM STDIN USING DELIMITERS ',','\n','"' NULL as '';
"N907MQ",
"N907MQ",191

SELECT * FROM x;
SELECT tailnum , AVG( arr_delay ) FROM x GROUP BY tailnum;

-- these two should be the same?!
-- N907MQ|NULL is in the data, hence N907MQ|NULL should also be the result

SELECT tailnum , AVG( arr_delay ) FROM x WHERE tailnum = 'N907MQ' GROUP BY tailnum;
SELECT * FROM ( SELECT tailnum , AVG( arr_delay ) FROM x GROUP BY tailnum ) AS xxx WHERE tailnum = 'N907MQ';




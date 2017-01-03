START TRANSACTION;

CREATE TABLE x (tailnum STRING, arr_delay INTEGER);

COPY 2 RECORDS INTO x FROM STDIN USING DELIMITERS ',','\n','"' NULL as '';
"N907MQ",
"N907MQ",191

-- correct result, 191
SELECT AVG( arr_delay ) FROM x;

-- wrong result, NULL
SELECT tailnum , AVG( arr_delay ) FROM x GROUP BY tailnum;

-- works fine with MIN
SELECT tailnum , MIN( arr_delay ) FROM x GROUP BY tailnum;

-- correct again
SELECT tailnum , AVG( arr_delay ) FROM x WHERE tailnum = 'N907MQ' GROUP BY tailnum;

-- wrong again
SELECT * FROM ( SELECT tailnum , AVG( arr_delay ) FROM x GROUP BY tailnum ) AS xxx WHERE tailnum = 'N907MQ';

-- both work fine with MIN
SELECT tailnum , MIN( arr_delay ) FROM x WHERE tailnum = 'N907MQ' GROUP BY tailnum;
SELECT * FROM ( SELECT tailnum , MIN( arr_delay ) FROM x GROUP BY tailnum ) AS xxx WHERE tailnum = 'N907MQ';


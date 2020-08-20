START TRANSACTION;

CREATE TABLE IF NOT EXISTS json_data (id INT, jdata JSON);

COPY 2 RECORDS INTO json_data FROM STDIN DELIMITERS E',',E'\n',E'|';
1,"abc"
2,|["This CSV field needs to be CSV quoted otherwise we cannot define JSON arrays since we need the comma character", "the CSV quote char (pipe) MUST be escaped: \|", "backslash MUST be escaped twice (once for JSON and ONCE for CSV): \\\\"]|

COPY 2 RECORDS INTO json_data FROM STDIN DELIMITERS E',',E'\n',E'|' NO ESCAPE;
1,"abc"
2,|["This CSV field needs to be CSV quoted otherwise we cannot define JSON arrays since we need the comma character", "the CSV quote char (pipe) CANNOT occur here. It would end the CSV field.", "backslash does not need to be CSV escaped, but MUST be JSON escaped: \\"]|

SELECT * FROM json_data;


ROLLBACK;

CREATE TABLE jstest (
       j json
);

INSERT INTO jstest VALUES ('{"track":{"segments":[{"location":[ 47.763,13.4034 ],"start time":"2018-10-14 10:05:14","HR":73},{"location":[ 47.706,13.2635 ],"start time":"2018-10-14 10:39:21","HR":135}]}}');

select json.filter(j, '$.track.segments[*][*') from jstest;

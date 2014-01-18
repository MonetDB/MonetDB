create table jsoncomp(j json);
insert into jsoncomp values( '{"myBoolean":true,"myList":["a","b"],"myMap":{"c":"d","a":"b"},"myObject":"myClass","myJsonObject":{"myString":"myStringValue"}}');

select * from jsoncomp;

declare js json;
set js = '{"myBoolean":true,"myList":["a","b"],"myMap":{"c":"d","a":"b"},"myObject":"myClass","myJsonObject":{"myString":"myStringValue"}}';

select json.keyarray(js);
select json.valuearray(js);

select json.keyarray(j) from jsoncomp;
select json.valuearray(j) from jsoncomp;

drop table jsoncomp;

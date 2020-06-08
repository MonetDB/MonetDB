create table jsoncomp(j json);
insert into jsoncomp values( '{"myBoolean":true,"myList":["a","b"],"myMap":{"c":"d","a":"b"},"myObject":"myClass","myJsonObject":{"myString":"myStringValue"}}');

select * from jsoncomp;

select json.keyarray('{"myBoolean":true,"myList":["a","b"],"myMap":{"c":"d","a":"b"},"myObject":"myClass","myJsonObject":{"myString":"myStringValue"}}');
select json.valuearray('{"myBoolean":true,"myList":["a","b"],"myMap":{"c":"d","a":"b"},"myObject":"myClass","myJsonObject":{"myString":"myStringValue"}}');

select json.keyarray(j) from jsoncomp;
select json.valuearray(j) from jsoncomp;

drop table jsoncomp;

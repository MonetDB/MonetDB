create table txt( js json);
insert into txt values('{"message" : "indoorDeviceLocation", "id" : "00:1b:21:02:30:cd", "dateTime" : "2007-04-05T14:30", "position" : {"X" : "10", "Y" : "10" }}');
select * from txt;

select json.text(json.filter(js, 'message')) from txt;
select json.text(json.filter(js, 'message')) = 'indoorDeviceLocation' from txt;

drop table txt;

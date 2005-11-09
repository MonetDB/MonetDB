select date '1997-07-31' + interval '1' month;
select date '1997-10-31' + interval '1' month;
select timestamp '1997-07-15 19:00:00' + interval '9' hour;
select time '19:00:00' + interval '9' hour;
select date '1997-07-31' + interval '1' hour;
select interval '0' year + interval '0' month;
select interval '00:00' hour to minute + interval '00:00' minute to second;
select interval '2' hour + interval '74' minute;
-- this last statement should generate an error
select interval '2:74' hour to minute;

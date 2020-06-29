set debug = 1;
set debug = true;
set current_timezone = interval '60' second;
select current_timezone;
set debug = false;
set current_timezone = interval '0' second;
select current_timezone;

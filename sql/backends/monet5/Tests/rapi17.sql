START TRANSACTION;
create function dt(d date) returns string language R { class(d) };
select dt( cast('2015-09-21' as date) );
ROLLBACK;

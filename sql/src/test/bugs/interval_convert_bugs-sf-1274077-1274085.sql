select interval '1' year = interval '12' second;
rollback;
select cast('1-2' as interval year to month );

start transaction;
create table bug6648 (d double);
insert into bug6648 values (1), (0), (-1), (-127), (127), (0.12), (-3.1415629);
analyze sys.bug6648; -- make sure key property is set
select cast(d as tinyint) from bug6648;
rollback;

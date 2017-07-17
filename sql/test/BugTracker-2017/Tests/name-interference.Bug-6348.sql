create table cls_0(i integer, j integer, k string);
insert into cls_0 values
( 1,  99, 'b'),
( 2,  99, 'b'),
( 3,  99, 'b'),
( 4,  99, 'a'),
( 5,  99, 'a'),
( 6,  99, 'a'),
( 7,  33, 'b'),
( 8,  33, 'b'),
( 9,  33, 'b'),
( 10,  33, 'a'),
( 11,  33, 'a'),
( 12,  33, 'a');
select * from cls_0;

-- The metric functions over all columns are derived in one blow.
-- The result can be cached for easy recall
create procedure cls_0_statistics()
begin
    create table cls_0_statistics ( col string, fcn string, tpe string, val string);
    insert into cls_0_statistics values
        ('i', 'min', 'int', (select cast( min( i ) as string)  from cls_0)),
        ('i', 'max', 'int', (select cast( min( i ) as string)  from cls_0)),
        ('j', 'min', 'int', (select cast( min( i ) as string)  from cls_0));
end;

select * from cls_0_statistics;

drop procedure cls_0_statistics;
drop table cls_0_statistics;

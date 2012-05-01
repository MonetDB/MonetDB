start transaction;

create schema datacell;
create table runningcatalog(
    tag timestamp
);

create table datacell.lta(
    tag timestamp
);

create procedure datacell.archive()
begin
    insert into runningcatalog select  * from datacell.lta;
end;
call datacell.archive();

rollback;

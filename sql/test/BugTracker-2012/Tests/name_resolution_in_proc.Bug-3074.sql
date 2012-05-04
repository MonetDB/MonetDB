start transaction;

create schema dc;
create table runningcatalog(
    tag timestamp
);

create table dc.lta(
    tag timestamp
);

create procedure dc.archive()
begin
    insert into runningcatalog select  * from dc.lta;
end;
call dc.archive();

rollback;

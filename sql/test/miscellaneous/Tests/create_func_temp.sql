start transaction;
create function tmp.myt() returns int begin return 1; end;
rollback;
select tmp.myt(); --error  tmp.myt doesn't exist anymore

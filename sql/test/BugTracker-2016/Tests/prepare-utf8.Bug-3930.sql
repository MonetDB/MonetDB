start transaction;

create table bug3930 (a varchar(4));

prepare select * from bug3930 where a = ?;
exec **('ä123');

rollback;

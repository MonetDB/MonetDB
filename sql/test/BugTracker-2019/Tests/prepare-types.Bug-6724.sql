start transaction;
create function mylength1(s string) returns int begin return length(s); end;
create function mylength2(s string, i int) returns int begin return length(s) + i; end;
prepare select mylength1(?);
exec **('abc');
prepare select mylength2(?, 2);
exec **('abc');
rollback;

prepare select coalesce(cast(? as int), 23);
exec **(1);
exec **(); --error

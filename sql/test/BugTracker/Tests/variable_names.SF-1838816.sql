create function foo(_2foo int) returns int begin return _2foo; end;
select foo(2);
drop all function foo ;

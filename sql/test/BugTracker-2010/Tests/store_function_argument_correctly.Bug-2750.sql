create function f ( x varchar(20) ) returns varchar(10) begin return x; end;
select f.name, a.name, a."type", a.type_digits from functions f, args a where a.func_id = f.id and f.name = 'f';
drop function f;

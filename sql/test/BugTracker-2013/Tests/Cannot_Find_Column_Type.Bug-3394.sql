create function tmp1 ()
returns boolean
begin
    create temporary table table1 (
	field1 integer
    );

    return true;
end;

select tmp1();

create function tmp2 ()
returns boolean
begin
    create temporary table table2 (
	field1 json
    );

    return true;
end;

select tmp2();

create function tmp3 ()
returns boolean
begin
    create temporary table table3 (
	field1 float
    );

    return true;
end;

select tmp3();

create function tmp4 ()
returns boolean
begin
    create temporary table table4 (
	field1 uuid
    );

    return true;
end;

select tmp4();

drop function tmp4;
drop function tmp3;
drop function tmp2;
drop function tmp1;

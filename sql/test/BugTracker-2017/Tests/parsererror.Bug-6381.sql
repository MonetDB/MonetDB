create function mycounter1()
returns integer
begin
    declare s int;
    while (true)
    do
        set s =1;
    end while;
    return s;
END;
select * from functions where name ='mycounter1';

-- This semantic identical function is not recognized
create function mycounter2()
returns integer
begin
    declare s int;
    while (select true)
    do
        set s =1;
    end while;
    return s;
END;
sql>select * from functions where name ='mycounter2';

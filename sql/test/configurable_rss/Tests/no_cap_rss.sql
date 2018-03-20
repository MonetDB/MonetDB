create function getrss() 
returns bigint external name status.rss_cursize;

create table test(a int, b int, c double);

insert into test values (1, 0, 1);

create procedure loop_insert(maximum_size bigint)
begin
    declare size bigint;
    set size = (select count(*) from test);

    while size < maximum_size do
        insert into test (select a+1, b+2, rand()*c from test);

        set size = (select count(*) from test);
    end while;
end;

call loop_insert(1000000);

-- it seems that it requires an analytical query to keep memory in ram.
select getrss() > 20000 as resident_set_size_is_bigger_then_20kbytes, quantile(c/a, 0.8) * 0 from test;

drop table test cascade;
drop function getrss;

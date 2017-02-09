set optimizer = 'sequential_pipe';

create table test(value int);
insert into test values (1), (2), (3);

select * from test where value = 12345678900; # value > INT_MAX
explain select * from test where value = 1; # value < INT_MAX

select * from test where value > 12345678900; # value > INT_MAX
explain select * from test where value > 1; # value < INT_MAX

select * from test where value >= 12345678900; # value > INT_MAX
explain select * from test where value >= 1; # value < INT_MAX

select * from test where value < 12345678900; # value > INT_MAX
explain select * from test where value < 1; # value < INT_MAX

select * from test where value <= 12345678900; # value > INT_MAX
explain select * from test where value <= 1; # value < INT_MAX

select * from test where value <> 12345678900; # value > INT_MAX
explain select * from test where value <> 1; # value < INT_MAX

drop table test;

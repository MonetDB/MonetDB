create table SF_2596114 (x double);
explain select power(x,2) from SF_2596114;
explain select x*x from SF_2596114;
drop table SF_2596114;

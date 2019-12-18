declare deterministic timestamp;
set deterministic = now();
select deterministic + 1 - deterministic;
select deterministic + interval '1' second - deterministic;
select deterministic - deterministic;

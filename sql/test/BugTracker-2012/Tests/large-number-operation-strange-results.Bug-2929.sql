select 10000000 * 100000 * 11.51 + 51.097 * 100000;
select convert(100000000000,HUGEINT) * 1000000000 * 11.51 + 51.097 * 1000000000;
select convert (10000000 * 100000 * 11.51,decimal(15)) +convert (51.097 * 100000, decimal(15));
select convert (convert(100000000000,HUGEINT) * 1000000000 * 11.51,decimal(35)) +convert (51.097 * 1000000000, decimal(35));

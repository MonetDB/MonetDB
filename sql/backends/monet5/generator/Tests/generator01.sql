select * from generate_series(0,10,-2);

select * from generate_series(0,10,-2) 
where value <5;

select * from generate_series(0,10,-2) as v
where value <7 and value >3;

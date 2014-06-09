select * from generate_series(0.0,10.0,1.6);

select * from generate_series(0.1,10.0,1.77) 
where value <5.0;

select * from generate_series(0.2,10.0,1.9) as v
where value <7.0 and value >3.0;

select * from generate_series(0.0e0,10.0e0,1.6e0);

select * from generate_series(0.1e0,10.0e0,1.77e0) 
where value <5.0e0;

select * from generate_series(0.2e0,10.0e0,1.9e0) as v
where value <7.0e0 and value >3.0e0;

select * from generate_series(0.2e0,10.0e0,1.9e0) as v
where value > 0.0 and value <7.0e0 and value >3.0e0;

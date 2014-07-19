select * from generate_series(0,10,2);

select * from generate_series(0,10,2) where value <5;

select * from generate_series(0,10,2) as v where value <7 and value >3;

select * from generate_series(0,10,2) as v where value <7 and value >3 and value <=6 and value >=4;

select * from generate_series(10,0,-2);

select * from generate_series(10,0,-2) where value <5;

select * from generate_series(10,0,-2) as v where value <7 and value >3;

select * from generate_series(10,0,-2) as v where value <7 and value >3 and value <=6 and value >=4;

select * from generate_series(-2,8,2);

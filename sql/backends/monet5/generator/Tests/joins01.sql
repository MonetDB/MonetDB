select * from generate_series(0,3,1) as A, generate_series(0,6,2) as B;

select * from generate_series(0,3,1) as A, generate_series(0,6,2) as B
where A.value < B.value;

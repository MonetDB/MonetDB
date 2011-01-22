create array weight ( x integer dimension[0:3], y integer dimension[0:3], w float);

insert into weight values 
(0,0,0.2), (0,1,0.5), (0,2,0.2),
(1,0,0.5), (1,1,1.0), (1,2,0.5),
(2,0,0.2), (2,1,0.5), (2,2,0.2);

create array matrix( x integer dimension[0:7], y integer dimension[0:7], f float default 1.0);

update matrix set f = 0 where x>=5 or y>=5;
update matrix set f = sin(x * f) where x <5 and y < 5;

select * from matrix;

-- smoothing using a reference array using dot product (*)
select [x],[y], avg( f * weight)
from matrix
group by matrix[x:x+2][y:y+2];


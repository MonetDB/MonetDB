create array a ( x integer dimension[0:1:3], y integer dimension[0:1:3], f float);
insert into a values (0,0,0.2), (0,1,0.5), (0,2,0.2), (1,0,0.5), (1,1,1.0), (1,2,0.5), (2,0,0.2), (2,1,0.5), (2,2,0.2); 
select x,y, avg(f * f) from a group by a[x:x+2][y:y+2];
select x,y, f, avg(f * f) from a group by a[x:x+2][y:y+2];
select x,y, f, avg(f * f) as f_avg from a group by a[x:x+2][y:y+2];
drop array a;


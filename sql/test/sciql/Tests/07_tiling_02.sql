create array a (x int dimension[4], y int dimension[4], v float default 37.0);
create array aa (x int dimension[4], y int dimension[4], v1 float default 37.0, v2 float default 42.6);
select x, y,  sum(v), avg(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y, sum(v), avg(v), sum(v), v from a group by a[x-1:x+2][y-2:y+1];
select x, y, sum(v1), avg(v1), v1, sum(v2), avg(v2), v2 from aa group by aa[x-1:x+2][y-2:y+1];
drop array a;
drop array aa;


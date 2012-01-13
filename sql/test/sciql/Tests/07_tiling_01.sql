create array a (x int dimension[4], y int dimension[4], v float default 37);
select x, y,  avg(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  sum(v) from a group by a[x-1:x+2][y-2:y+1];


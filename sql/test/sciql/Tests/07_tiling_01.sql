create array a (x int dimension[4], y int dimension[4], v tinyint default 37);
select x, y,  avg(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  sum(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  min(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  max(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,count(v) from a group by a[x-1:x+2][y-2:y+1];
drop array a;

create array a (x int dimension[4], y int dimension[4], v smallint default 37);
select x, y,  avg(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  sum(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  min(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  max(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,count(v) from a group by a[x-1:x+2][y-2:y+1];
drop array a;

create array a (x int dimension[4], y int dimension[4], v integer default 37);
select x, y,  avg(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  sum(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  min(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  max(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,count(v) from a group by a[x-1:x+2][y-2:y+1];
drop array a;

create array a (x int dimension[4], y int dimension[4], v bigint default 37);
select x, y,  avg(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  sum(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  min(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  max(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,count(v) from a group by a[x-1:x+2][y-2:y+1];
drop array a;

create array a (x int dimension[4], y int dimension[4], v real default 37);
select x, y,  avg(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  sum(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  min(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  max(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,count(v) from a group by a[x-1:x+2][y-2:y+1];
drop array a;

create array a (x int dimension[4], y int dimension[4], v double default 37);
select x, y,  avg(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  sum(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  min(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,  max(v) from a group by a[x-1:x+2][y-2:y+1];
select x, y,count(v) from a group by a[x-1:x+2][y-2:y+1];
drop array a;


create array a (x int dimension[4], y int dimension[4], v tinyint default 37.0);
create array aa (x int dimension[4], y int dimension[4], v1 tinyint default 37.0, v2 tinyint default 42.0);
select * from a group by a[x-1:x+2][*];
select * from a group by a[x-1:x+2][y-2:y+1];
select x, y, sum(v), avg(v), min(v), max(v), sum(v), v from a group by a[x-1:x+2][*];
select * from aa group by aa[*][y-2:y+1];
select * from aa group by aa[x-1:x+2][y-2:y+1];
select x, y, sum(v1), avg(v1), min(v1), max(v1), v1, sum(v2), avg(v2), min(v2), max(v2), v2 from aa group by aa[*][y-2:y+1];
drop array a;
drop array aa;

create array a (x int dimension[4], y int dimension[4], v smallint default 37.0);
create array aa (x int dimension[4], y int dimension[4], v1 smallint default 37.0, v2 smallint default 42.0);
select * from a group by a[x-1:x+2][*];
select * from a group by a[x-1:x+2][y-2:y+1];
select x, y, sum(v), avg(v), min(v), max(v), sum(v), v from a group by a[x-1:x+2][*];
select * from aa group by aa[*][y-2:y+1];
select * from aa group by aa[x-1:x+2][y-2:y+1];
select x, y, sum(v1), avg(v1), min(v1), max(v1), v1, sum(v2), avg(v2), min(v2), max(v2), v2 from aa group by aa[*][y-2:y+1];
drop array a;
drop array aa;

create array a (x int dimension[4], y int dimension[4], v integer default 37.0);
create array aa (x int dimension[4], y int dimension[4], v1 integer default 37.0, v2 integer default 42.0);
select * from a group by a[x-1:x+2][*];
select * from a group by a[x-1:x+2][y-2:y+1];
select x, y, sum(v), avg(v), min(v), max(v), sum(v), v from a group by a[x-1:x+2][*];
select * from aa group by aa[*][y-2:y+1];
select * from aa group by aa[x-1:x+2][y-2:y+1];
select x, y, sum(v1), avg(v1), min(v1), max(v1), v1, sum(v2), avg(v2), min(v2), max(v2), v2 from aa group by aa[*][y-2:y+1];
drop array a;
drop array aa;

create array a (x int dimension[4], y int dimension[4], v bigint default 37.0);
create array aa (x int dimension[4], y int dimension[4], v1 bigint default 37.0, v2 bigint default 42.0);
select * from a group by a[x-1:x+2][*];
select * from a group by a[x-1:x+2][y-2:y+1];
select x, y, sum(v), avg(v), min(v), max(v), sum(v), v from a group by a[x-1:x+2][*];
select * from aa group by aa[*][y-2:y+1];
select * from aa group by aa[x-1:x+2][y-2:y+1];
select x, y, sum(v1), avg(v1), min(v1), max(v1), v1, sum(v2), avg(v2), min(v2), max(v2), v2 from aa group by aa[*][y-2:y+1];
drop array a;
drop array aa;

create array a (x int dimension[4], y int dimension[4], v real default 37.0);
create array aa (x int dimension[4], y int dimension[4], v1 real default 37.0, v2 real default 42.6);
select * from a group by a[x-1:x+2][*];
select * from a group by a[x-1:x+2][y-2:y+1];
select x, y, sum(v), avg(v), min(v), max(v), sum(v), v from a group by a[x-1:x+2][*];
select * from aa group by aa[*][y-2:y+1];
select * from aa group by aa[x-1:x+2][y-2:y+1];
select x, y, sum(v1), avg(v1), min(v1), max(v1), v1, sum(v2), avg(v2), min(v2), max(v2), v2 from aa group by aa[*][y-2:y+1];
drop array a;
drop array aa;

create array a (x int dimension[4], y int dimension[4], v double default 37.0);
create array aa (x int dimension[4], y int dimension[4], v1 double default 37.0, v2 double default 42.6);
select * from a group by a[x-1:x+2][*];
select * from a group by a[x-1:x+2][y-2:y+1];
select x, y, sum(v), avg(v), min(v), max(v), sum(v), v from a group by a[x-1:x+2][*];
select * from aa group by aa[*][y-2:y+1];
select * from aa group by aa[x-1:x+2][y-2:y+1];
select x, y, sum(v1), avg(v1), min(v1), max(v1), v1, sum(v2), avg(v2), min(v2), max(v2), v2 from aa group by aa[*][y-2:y+1];
drop array a;
drop array aa;


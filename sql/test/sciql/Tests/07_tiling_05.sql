create array a_tinyint  (x int dimension[4], y int dimension[4], v tinyint default 0);
create array a_smallint (x int dimension[4], y int dimension[4], v smallint default 0);
create array a_int      (x int dimension[4], y int dimension[4], v integer default 0);
create array a_bigint   (x int dimension[4], y int dimension[4], v bigint default 0);
create array a_real     (x int dimension[4], y int dimension[4], v real default 0);
create array a_double   (x int dimension[4], y int dimension[4], v double default 0);

insert into a_tinyint  select x, y, x * 4 + y from a_tinyint;
insert into a_smallint select x, y, x * 4 + y from a_smallint;
insert into a_int      select x, y, x * 4 + y from a_int;
insert into a_bigint   select x, y, x * 4 + y from a_bigint;
insert into a_real     select x, y, x * 4 + y from a_real;
insert into a_double   select x, y, x * 4 + y from a_double;
select * from a_tinyint;
select * from a_real;

select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_tinyint  group by a_tinyint [x-1:x+2][y:y+1] having v between 5 and 10;
select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_smallint group by a_smallint[x-1:x+2][y:y+1] having v between 5 and 10;
select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_int      group by a_int     [x-1:x+2][y:y+1] having v between 5 and 10;
select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_bigint   group by a_bigint  [x-1:x+2][y:y+1] having v between 5 and 10;
select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_real     group by a_real    [x-1:x+2][y:y+1] having v between 5 and 10;
select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_double   group by a_double  [x-1:x+2][y:y+1] having v between 5 and 10;

select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_tinyint  group by a_tinyint [x-1:x+2][y:y+1] having min(v) between 5 and 10;
select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_smallint group by a_smallint[x-1:x+2][y:y+1] having min(v) between 5 and 10;
select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_int      group by a_int     [x-1:x+2][y:y+1] having min(v) between 5 and 10;
select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_bigint   group by a_bigint  [x-1:x+2][y:y+1] having min(v) between 5 and 10;
select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_real     group by a_real    [x-1:x+2][y:y+1] having min(v) between 5 and 10;
select x, y, v, sum(v) as sum, avg(v) as avg, min(v) as min, max(v) as max, count(v) as cnt from a_double   group by a_double  [x-1:x+2][y:y+1] having min(v) between 5 and 10;

drop array a_tinyint;
drop array a_smallint;
drop array a_int;
drop array a_bigint;
drop array a_real;
drop array a_double;


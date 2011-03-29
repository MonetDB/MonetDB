-- The Gray-Level Co-occurrence matrix is a basic building block in image processing.
-- see http://www.fp.ucalgary.ca/mhallbey for a tutorial on its definition
-- the examples below are taken from it

create array img(
	x int dimension[0:4],
	y int dimension[0:4],
	v int default 0  );

insert into img values (0,1,1),(0,1,1),(0,2,2),(2,3,3);

-- the count matrix
create array corrCnt(
	x int dimension[0:4],
	y int dimension[0:4],
	cnt int default 0);
insert into corrCnt select A[x][y].v, A[x+1][y].v, count(*)
from img as A
group by A[x][y].v, A[x+1][y].v;

-- the probability distribution 
create array corrProb(
	x int dimension[0:4],
	y int dimension[0:4],
	v double default 0.0);
insert into corrProb select x, y, cnt/ (select sum(cnt) from corrCnt) 
from corrCnt;

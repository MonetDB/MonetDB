
CREATE TABLE preAccident1(time integer, carid integer, lane integer, dir integer, seg integer, pos integer);


SELECT in1.carid, in2.carid, in2.time, in1.dir, in1.seg, in1.pos 
FROM 
	preAccident1 AS in1, 
	preAccident1 AS in2, 
	preAccident1 AS in22 
WHERE
	in2.carid <> in1.carid AND 
	in2.pos = in1.pos AND 
	in2.lane = in1.lane AND
	in2.dir = in1.dir AND 
	in2.time >= in1.time AND
	in2.time <= in1.time + 2 AND 
	in22.carid = in2.carid AND 
	in22.pos = in1.pos AND 
	in22.lane = in1.lane AND 
	in22.dir = in1.dir AND 
	in22.time = in2.time + 8;

SELECT in1.carid, in2.carid, in2.time, in1.dir, in1.seg, in1.pos 
FROM 
	preAccident1 AS in1, 
	preAccident1 AS in2, 
	preAccident1 AS in11, 
	preAccident1 AS in22 
WHERE
	in2.carid <> in1.carid AND 
	in2.pos = in1.pos AND 
	in2.lane = in1.lane AND
	in2.dir = in1.dir AND 
	in2.time >= in1.time AND
	in2.time <= in1.time + 2 AND 
	in11.carid = in1.carid AND 
	in11.pos = in1.pos AND 
	in11.lane = in1.lane AND 
	in11.dir = in1.dir AND 
	in11.time = in1.time + 8 AND 
	in22.carid = in2.carid AND 
	in22.pos = in1.pos AND 
	in22.lane = in1.lane AND 
	in22.dir = in1.dir AND 
	in22.time = in2.time + 8;

drop table preAccident1;

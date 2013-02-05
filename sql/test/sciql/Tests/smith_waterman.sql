-- Trying to implement the local sequence alignment algorithm Smith-Waterman using SciQL

-- An array to store the score table
CREATE ARRAY blosum50 (x CHAR(1) DIMENSION, y char(1) dimension, score int);
INSERT INTO blosum50 VALUES ('A', 'A', 5), ('A', 'R', -1), ..., ('V', 'Y', -1), ('V', 'V', 5);

-- The left-hand-side sequence to be aligned, y-axis
CREATE ARRAY seq1 (idx INT DIMENSION[$len1], v CHAR(1));
INSERT INTO seq1 VALUES ('P'), ('A'), ..., ('E');
-- The right-hand-side sequence to be aligned, x-axis
CREATE ARRAY seq2 (idx INT DIMENSION[$len2], v CHAR(1));
INSERT INTO seq1 VALUES ('H'), ('E'), ..., ('E');

-- The cost of a gap
DECLARE d INT; SET d = ???;

-- An array to store the scores of aligning the two sequences,
-- 'ancst' denotes the ancestor of this score: 0 - none; 1 - upper left; 2 - left; 3 - upper
CREATE ARRAY scores (x INT DIMENSION[$len2], y INT DIMENSION[$len1], score INT DEFAULT 0, ancst INT);
INSERT INTO scores (
	SELECT x, y,
	  max4    (0, scores[x-1][y-1].score + blosum50[][].score, scores[x-1][y].score - d, scores[x][y-1].score - d),
	  -- returns which of the input parameters has the max value
	  max4_pos(0, scores[x-1][y-1].score + blosum50[][].score, scores[x-1][y].score - d, scores[x][y-1].score - d),
	FROM scores, blosum50
	-- the first row+column are already initialised with 0 and need no more computations
	WHERE scores.x > 0 AND scores.y > 0;

-- Table to store the alignment result, assuming only one best alignment, but
--   should be easy to be extended for multiple best alignments
-- Need to store (x, y) to denote the position of the letters
CREATE TABLE alignment(x INT, y INT, c1 CHAR(1), c2 CHAR(1), score INT);

DECLARE cur_x INT, cur_y INT, cur_scr INT;
SET cur_scr = SELECT MAX(score) FROM scores;
SET cur_x   = SELECT x FROM scores WHERE score = cur_scr;
SET cur_y   = SELECT y FROM scores WHERE score = cur_scr;

-- FIXME: forgot to deal with the gaps
WHILE cur_scr > 0 DO
	INSERT INTO alignment VALUES (cur_x, cur_y, seq1[cur_y].v, seq2[cur_x].v, cur_scr);
	IF (scores[cur_x][cur_y].ancst = 1) THEN
		SET cur_x = cur_x -1;
		SET cur_y = cur_y -1;
	ELSEIF (scores[cur_x][cur_y].ancst = 2) THEN
		SET cur_x = cur_x -1;
	ELSEIF (scores[cur_x][cur_y].ancst = 3) THEN
		SET cur_y = cur_y -1;
	END IF;

	IF (scores[cur_x][cur_y].ancst = 0) THEN
		SET cur_scr = 0;
	ELSE
		SET cur_scr = scores[cur_x][cur_y].score;
	END IF;
END WHILE;

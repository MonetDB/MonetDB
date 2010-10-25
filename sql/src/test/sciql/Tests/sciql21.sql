CREATE FUNCTION markov( a ARRAY( x int DIMENSION, y int DIMENSION, f float), steps integer)
RETURNS ARRAY( x int DIMENSION, y int DIMENSION, f float) 
EXTERNAL NAME ’markov.loop’;

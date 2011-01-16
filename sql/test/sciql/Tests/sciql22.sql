CREATE ARRAY landsat ( channel integer DIMENSION[7], x integer DIMENSION[1024], y integer DIMENSION[1024], v integer);
UPDATE landsat SET v = noise(v,delta) WHERE channel = 6 and mod(x,6) = 1;

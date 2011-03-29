CREATE ARRAY matrix ( x integer DIMENSION[4], y integer DIMENSION[4], v float DEFAULT 0.0 );
CREATE ARRAY stripes( x integer DIMENSION[4], y integer DIMENSION[4] CHECK(mod(y,2) = 1), v float DEFAULT 0.0);
CREATE ARRAY diagonal( x integer DIMENSION[4], y integer DIMENSION[4] CHECK(x = y), v float DEFAULT 0.0);
CREATE ARRAY sparse( x integer DIMENSION[4], y integer DIMENSION[4], v float DEFAULT 0.0 CHECK(v>=0.0));


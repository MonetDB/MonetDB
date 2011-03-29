CREATE ARRAY matrix ( x integer DIMENSION[4], y integer DIMENSION[4], v float DEFAULT 0.0 );
CREATE ARRAY grid( x integer DIMENSION[4] CHECK(mod(x,2) = 0), y integer DIMENSION[4], v float DEFAULT 0.0);
CREATE ARRAY diagonal( x integer DIMENSION[4], y integer DIMENSION[4] CHECK(x = y), v float );
CREATE ARRAY sparse( x integer DIMENSION[4], y integer DIMENSION[4], v float DEFAULT 0.0 CHECK(v>=0.0));


CREATE ARRAY stripes( x integer DIMENSION[4], y integer DIMENSION[4], v float DEFAULT 0.0 );
CREATE ARRAY stripes2( x integer DIMENSION, y integer DIMENSION, v float DEFAULT 0.0 );
CREATE ARRAY stripes3( x integer DIMENSION[4], y integer DIMENSION, v float DEFAULT 0.0 );

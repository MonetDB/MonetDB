CREATE ARRAY matrix ( x integer DIMENSION[4], y integer DIMENSION[4], val float DEFAULT 0.0 );
CREATE ARRAY grid( x integer DIMENSION[4] CHECK(mod(x,2) = 0), y integer DIMENSION[4], val float DEFAULT 0.0);
CREATE ARRAY diagonal( x integer DIMENSION[4], y integer DIMENSION[4] CHECK(x = y), val float );
CREATE ARRAY sparse( x integer DIMENSION[4], y integer DIMENSION[4], val float DEFAULT 0.0 CHECK(val>=0.0));


CREATE ARRAY stripes( x integer DIMENSION[4], y integer DIMENSION[4], val float DEFAULT 0.0 );
CREATE ARRAY stripes2( x integer DIMENSION, y integer DIMENSION, val float DEFAULT 0.0 );
CREATE ARRAY stripes3( x integer DIMENSION[4], y integer DIMENSION, val float DEFAULT 0.0 );

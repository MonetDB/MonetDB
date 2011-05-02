-- arrays must have at least one dimension
create array err1( payload float);

-- array dimension types should comply with attribute type
create array err3( x char dimension[1:128:1], v float);
create array err4( s varchar(25) dimension['a':'z':1], v float);
create array err5( s varchar(25) dimension[1:3:1], v float);

-- default value expressions should be of the proper type
create array err6( x integer dimension[1:128:1],
	v float default 'unknown');
create array err7( x integer dimension[1:128:1],
	v float default v > 0);
create array err8( x integer dimension[1:128:1],
	v float default true);

-- array dimensions may have undetermined step sizes
create array array10(x integer dimension[1:128:*], v float);
-- arrays may have zero non-dimensional attribute
create array array11( x integer dimension[1:10:1]);


drop array err1;
drop array err3;
drop array err4;
drop array err5;
drop array err6;
drop array err7;
drop array err8;

drop array array10;
drop array array11;


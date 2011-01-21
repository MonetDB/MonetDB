-- arrays should have at least one dimension
create array err1( payload float);

-- arrays need to have one non-dmension attribute
create array err2( x integer dimension[1:10:1]);

-- array dimension types should comply with attribute type
create array err3( x char dimension[1:128:1], v float);
create array err4( s varchar(25) dimension['a':'z':1], v float);
create array err5( s varchar(25) dimension[1:3:1], v float);

-- array dimensions may have undetermined step sizes

create array array10(x integer dimension[1:128:*], v float);

-- default value expressions should be of the proper type

create array array15( x integer dimension[1:128:1],
	v float default 'unknown');
create array array16( x integer dimension[1:128:1],
	v float default v > 0);
create array array17( x integer dimension[1:128:1],
	v float default true);

drop array err1;
drop array err2;
drop array err3;
drop array err4;
drop array err5;
drop array array10;
drop array array15;
drop array array16;
drop array array17;


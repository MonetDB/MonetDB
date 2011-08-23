-- array dimension types should comply with attribute type
create array err(x char dimension[1:128:1], v float);
create array err(s varchar(25) dimension['a':'z':1], v float);
create array err(s varchar(25) dimension[1:3:1], v float);


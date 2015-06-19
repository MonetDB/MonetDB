create array twod_no (x integer dimension[5], y integer dimension[3], v float);
create array twod (x integer dimension[5], y integer dimension[3], v float default -1.0);
create array threed_no(x integer dimension[3], y float dimension[0.1:0.1:2.05], z integer dimension[5:2:12], v varchar(20));
create array threed(x integer dimension[3], y float dimension[0.1:0.1:2.05], z integer dimension[5:2:12], v varchar(20) default 'DEF');

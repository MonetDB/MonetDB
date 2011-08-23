-- default value expressions should be of the proper type
create array err(x integer dimension[128], v float default 'unknown');
create array err(x integer dimension[128], v float default (v > 0));
create array err(x integer dimension[128], v float default true);


select st_xmin(b), st_ymin(b), st_xmax(b), st_ymax(b) from ( select mbr(st_geomfromtext('polygon((0 0, 1 1, 2 2, 0 0),(1 1, 2 2, 0 0, 1 1))')) ) as foo(b);

select st_xmin(b), st_ymin(b), st_xmax(b), st_ymax(b) from ( select st_geomfromtext('polygon((0 0, 1 1, 2 2, 0 0),(1 1, 2 2, 0 0, 1 1))') ) as foo(b);

select mbr(st_mlinefromtext('multilinestring((10 10 10, 20 20 20, 30 30 30), (40 40 40, 50 50 50, 60 60 60))'));


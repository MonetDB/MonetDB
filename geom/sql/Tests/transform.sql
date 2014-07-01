SELECT ST_AsText(ST_Transform(ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))',2249),4326)) As wgs_geom;

select st_astext(st_transform(st_geomfromtext('multipoint(10 20, 30 40)', 2329), 4326));

select st_astext(st_transform(st_geomfromtext('multipoint(1234 125, 30 420)', 2329), 4326));

select st_astext(st_transform(ST_MLineFromText('MULTILINESTRING((10 20, 30 40), (40 50, 60 70))', 2128), 4326));


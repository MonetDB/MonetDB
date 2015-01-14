SELECT ST_SRID(ST_Collect(ST_GeomFromText('POINT(0 0)', 32749), ST_GeomFromText('POINT(1 1)', 32749)));

SELECT ST_Collect(ST_GeomFromText('POINT(0 0)', 32749), ST_GeomFromText('POINT(1 1)', 32740));

select ST_asewkt(ST_makeline(ST_GeomFromText('POINT(0 0)', 3), ST_GeomFromText('POINT(1 1)', 3)));
select ST_makeline(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)', 3));

select 'ST_MakeLine1', ST_AsText(ST_MakeLine(
 ST_GeomFromText('POINT(0 0)'),
 ST_GeomFromText('LINESTRING(1 1, 10 0)')));

select 'ST_MakeLine_agg1', ST_AsText(ST_MakeLine(g)) from (
 values (ST_GeomFromText('POINT(0 0)')),
        (ST_GeomFromText('LINESTRING(1 1, 10 0)')),
        (ST_GeomFromText('LINESTRING(10 0, 20 20)')),
        (ST_GeomFromText('POINT(40 4)'))
) as foo(g);

select ST_makebox2d(ST_GeomFromText('POINT(0 0)', 3), ST_GeomFromText('POINT(1 1)', 3));
select ST_makebox2d(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)', 3));

select ST_3DMakeBox(ST_GeomFromText('POINT(0 0)', 3), ST_GeomFromText('POINT(1 1)', 3));
select ST_3DMakeBox(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)', 3));

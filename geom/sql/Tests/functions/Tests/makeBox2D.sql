select ST_MakeBox2D(ST_Point(-989502.1875, 528439.5625), ST_Point(-987121.375 ,529933.1875));
select ST_MakeBox2D(ST_Point(-989502.1875, 528439.5625), null);
select ST_MakeBox2D(ST_PointFromText('POINT(-989502.1875 528439.5625)'), ST_GeomFromText('linestring(-987121.375 529933.1875, 0 0)'));

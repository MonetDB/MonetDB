create table test1(id int, geom Geometry);
create table test2(x double, y double, z double);

create table papoints AS ( --get points from intersecting patches
	SELECT a.id, x, y, z, geom FROM test1 a, test2 b
	WHERE [a.geom] IntersectsXYZ [x, y, z,28992]) WITH DATA;

create table papoints AS ( --get points from intersecting patches
	SELECT a.type, a.id, x, y, z, geom FROM test1 a
	--LEFT JOIN pointcloud_unclassified b ON (ST_Intersects(a.geom, geometry(b.pa)))
       --LEFT JOIN pointcloud_unclassified b ON (ST_Intersects(a.geom, x, y, z,28992))
       LEFT JOIN test2 b ON ([a.geom] IntersectsXYZ [x, y, z,28992])
) WITH DATA;

drop table test1;
drop table test2;


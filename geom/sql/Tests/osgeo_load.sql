--Spatial Reference System
INSERT INTO spatial_ref_sys VALUES(101, 'POSC', 32214,
'PROJCS["UTM_ZONE_14N", GEOGCS["World Geodetic System
72",DATUM["WGS_72", ELLIPSOID["NWL_10D", 6378135,
298.26]],PRIMEM["Greenwich",
0],UNIT["Meter",1.0]],PROJECTION["Transverse_Mercator"],
PARAMETER["False_Easting", 500000.0],PARAMETER["False_Northing",
0.0],PARAMETER["Central_Meridian", -99.0],PARAMETER["Scale_Factor",
0.9996],PARAMETER["Latitude_of_origin", 0.0],UNIT["Meter", 1.0]]');
-- Lakes
INSERT INTO lake_geom VALUES(101, 1, 5, 1, 52,18, 66,23, 73,9, 48,6,
52,18);
INSERT INTO lake_geom VALUES(101, 2, 5, 1, 59,18, 67,18, 67,13, 59,13,
59,18);
INSERT INTO lakes VALUES (
101, 'BLUE LAKE', 101);
-- Road segments
INSERT INTO road_segment_geom VALUES (
101, 1, 3, 1, 0,18, 10,21, 16,23);
INSERT INTO road_segment_geom VALUES (
101, 1, 3, 2, 28,26, 44,31, NULL,NULL);
INSERT INTO road_segment_geom VALUES (
102, 1, 3, 1, 44,31, 56,34, 70,38);
INSERT INTO road_segment_geom VALUES (
103, 1, 3, 1, 70,38, 72,48, NULL,NULL);
INSERT INTO road_segment_geom VALUES (
104, 1, 3, 1, 70,38, 84,42, NULL,NULL);
INSERT INTO road_segment_geom VALUES (
105, 1, 3, 1, 28,26, 28,0, NULL,NULL);
INSERT INTO road_segments VALUES(102, 'Route 5', NULL, 2, 101);
INSERT INTO road_segments VALUES(103, 'Route 5', 'Main Street', 4, 102);
INSERT INTO road_segments VALUES(104, 'Route 5', NULL, 2, 103);
INSERT INTO road_segments VALUES(105, 'Main Street', NULL, 4, 104);
INSERT INTO road_segments VALUES(106, 'Dirt Road by Green Forest', NULL,
1, 105);
-- DividedRoutes
INSERT INTO divided_route_geom VALUES(101, 1, 9, 1, 10,48, 10,21, 10,0);
INSERT INTO divided_route_geom VALUES(101, 2, 9, 1, 16,0, 10,23, 16,48);
INSERT INTO divided_routes VALUES(119, 'Route 75', 4, 101);
-- Forests
INSERT INTO forest_geom VALUES(101, 1, 11, 1, 28,26, 28,0, 84,0, 84,42,
28,26);
INSERT INTO forest_geom VALUES(101, 1, 11, 2, 52,18, 66,23, 73,9, 48,6,
52,18);
INSERT INTO forest_geom VALUES(101, 2, 11, 1, 59,18, 67,18, 67,13, 59,13,
59,18);
INSERT INTO forests VALUES(109, 'Green Forest', 101);
-- Bridges
INSERT INTO bridge_geom VALUES(101, 1, 1, 1, 44, 31);
INSERT INTO bridges VALUES(110, 'Cam Bridge', 101);
-- Streams
INSERT INTO stream_geom VALUES(101, 1, 3, 1, 38,48, 44,41, 41,36);
INSERT INTO stream_geom VALUES(101, 1, 3, 2, 44,31, 52,18, NULL,NULL);
INSERT INTO stream_geom VALUES(102, 1, 3, 1, 76,0, 78,4, 73,9 );
--
INSERT INTO streams VALUES(111, 'Cam Stream', 101);
INSERT INTO streams VALUES(112, 'Cam Stream', 102);
-- Buildings
INSERT INTO building_pt_geom VALUES(101, 1, 1, 1, 52,30);
INSERT INTO building_pt_geom VALUES(102, 1, 1, 1, 64,33);
INSERT INTO building_area_geom VALUES(101, 1, 5, 1, 50,31, 54,31,
54,29, 50,29, 50,31);
INSERT INTO building_area_geom VALUES(102, 1, 5, 1, 66,34, 62,34, 62,32,
66,32, 66,34);
INSERT INTO buildings VALUES(113, '123 Main Street', 101, 101);
INSERT INTO buildings VALUES(114, '215 Main Street', 102, 102);
-- Ponds
INSERT INTO pond_geom VALUES(101, 1, 11, 1, 24,44, 22,42, 24,40, 24,44 );
INSERT INTO pond_geom VALUES(101, 2, 11, 1, 26,44, 26,40, 28,42, 26,44 );
INSERT INTO ponds VALUES(120, NULL, 'Stock Pond', 101);
-- Named Places
INSERT INTO named_place_geom VALUES(101, 1, 5, 1, 62,48, 84,48, 84,30,
56,30);
INSERT INTO named_place_geom VALUES(101, 1, 5, 2, 56,30, 56,34, 62,48,
NULL,NULL);
INSERT INTO named_place_geom VALUES(102, 1, 5, 1, 67,13, 67,18, 59,18,
59,13);
INSERT INTO named_place_geom VALUES(102, 1, 5, 2, 59,13, 67,13,
NULL,NULL, NULL,NULL);
INSERT INTO named_places VALUES(117, 'Ashton', 101);
INSERT INTO named_places VALUES(118, 'Goose Island', 102);
-- Map Neatlines
INSERT INTO map_neatline_geom VALUES(101, 1, 5, 1, 0,0, 0,48, 84,48,
84,0, 0,0);
INSERT INTO map_neatlines VALUES(115, 101);
-- Geometry Columns
INSERT INTO geometry_columns VALUES (
'lakes', 'shore_gid',
'lake_geom',0, 5, 2, 5, 101);
INSERT INTO geometry_columns VALUES (
'road_segments', 'centerline_gid',
'road_segment_geom',0, 3, 2, 3, 101);
INSERT INTO geometry_columns VALUES (
'divided_routes', 'centerlines_gid',
'divided_route_geom',0, 9, 2, 3, 101);
INSERT INTO geometry_columns VALUES (
'forests', 'boundary_gid',
'forest_geom',0, 11, 2, 5, 101);
INSERT INTO geometry_columns VALUES (
'bridges', 'position_gid',
'bridge_geom',0, 1, 2, 1, 101);
INSERT INTO geometry_columns VALUES (
'streams', 'centerline_gid',
'stream_geom',0, 3, 2, 3, 101);
INSERT INTO geometry_columns VALUES (
'buildings', 'position_gid',
'building_pt_geom',0, 1, 2, 1, 101);
INSERT INTO geometry_columns VALUES (
'buildings', 'footprint_gid',
'building_area_geom',0, 5, 2, 5, 101);
INSERT INTO geometry_columns VALUES (
'ponds', 'shores_gid',
'pond_geom',0, 11, 2, 4, 101);
INSERT INTO geometry_columns VALUES (
'named_places', 'boundary_gid',
'named_place_geom',0, 5, 2, 4, 101);
INSERT INTO geometry_columns VALUES (
'map_neatlines', 'neatline_gid',
'map_neatline_geom',0, 5, 2, 5, 101);

-- tests for Hausdorff distances

-- polygon and polygon
SELECT 'hausdorff_poly_poly', st_hausdorffdistance(
	'POLYGON((0 0, 0 2, 1 2, 2 2, 2 0, 0 0))',
	'POLYGON((0.5 0.5, 0.5 2.5, 1.5 2.5, 2.5 2.5, 2.5 0.5, 0.5 0.5))');
-- 0.707106781186548

-- linestring and linestring
SELECT 'hausdorff_ls_ls', st_hausdorffdistance(
	'LINESTRING (0 0, 2 1)'
	, 'LINESTRING (0 0, 2 0)');
-- 1.0

-- other linestrings
SELECT 'hausdorff_ls_ls_2', st_hausdorffdistance(
	'LINESTRING (0 0, 2 0)', 
	'LINESTRING (0 1, 1 2, 2 1)');
-- 2.0

-- linestring and multipoint
SELECT 'hausdorff_ls_mp', st_hausdorffdistance(
	'LINESTRING (0 0, 2 0)', 
	'MULTIPOINT (0 1, 1 0, 2 1)');
-- 1.0

-- another linestring and linestring
SELECT 'hausdorff_ls_ls_3', st_hausdorffdistance(
	'LINESTRING (130 0, 0 0, 0 150)', 
	'LINESTRING (10 10, 10 150, 130 10)');
-- 14.142135623730951

-- hausdorf with densification
SELECT 'hausdorffdensify_ls_ls', st_hausdorffdistance(
	'LINESTRING (130 0, 0 0, 0 150)'
	, 'LINESTRING (10 10, 10 150, 130 10)', 0.5);
-- 70.0

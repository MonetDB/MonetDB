-- ST_Dimension on 2D: not closed 
SELECT 'dimension_01', ST_Dimension('POLYHEDRALSURFACE(((0 0,0 0,0 1,0 0)))');
SELECT 'dimension_02', ST_Dimension('GEOMETRYCOLLECTION(POLYHEDRALSURFACE(((0 0,0 0,0 1,0 0))))');

-- ST_Dimension on 3D: closed 
SELECT 'dimension_03', ST_Dimension('POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))');
SELECT 'dimension_04', ST_Dimension('GEOMETRYCOLLECTION(POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0))))');

-- ST_Dimension on 4D: closed
SELECT 'dimension_05', ST_Dimension('POLYHEDRALSURFACE(((0 0 0 0,0 0 1 0,0 1 0 2,0 0 0 0)),((0 0 0 0,0 1 0 0,1 0 0 4,0 0 0 0)),((0 0 0 0,1 0 0 0,0 0 1 6,0 0 0 0)),((1 0 0 0,0 1 0 0,0 0 1 0,1 0 0 0)))');
SELECT 'dimension_06', ST_Dimension('GEOMETRYCOLLECTION(POLYHEDRALSURFACE(((0 0 0 0,0 0 1 0,0 1 0 2,0 0 0 0)),((0 0 0 0,0 1 0 0,1 0 0 4,0 0 0 0)),((0 0 0 0,1 0 0 0,0 0 1 6,0 0 0 0)),((1 0 0 0,0 1 0 0,0 0 1 0,1 0 0 0))))');

-- ST_Dimension on 3D: invalid polyedron (a single edge is shared 3 times)
SELECT 'dimension_07', ST_Dimension('POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,0 1 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))');

-- ST_Dimension on 3D: invalid polyedron (redundant point inside each face)
SELECT 'dimension_08', ST_Dimension('POLYHEDRALSURFACE(((0 0 0,1 0 0,1 0 0,0 0 0)),((0 0 1,1 0 1,1 0 1,0 0 1)),((0 0 2,1 0 2,1 0 2,0 0 2)),((0 0 3,1 0 3,1 0 3,0 0 3)))');


-- ST_NumPatches
SELECT 'numpatches_01', ST_NumPatches('POLYHEDRALSURFACE EMPTY');
SELECT 'numpatches_02', ST_NumPatches('POLYHEDRALSURFACE(((0 0,0 0,0 1,0 0)))');
SELECT 'numpatches_03', ST_NumPatches('POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))');

-- ST_PatchN
SELECT 'patchN_01', ST_patchN('POLYHEDRALSURFACE EMPTY', 1);
SELECT 'patchN_02', ST_patchN('POLYHEDRALSURFACE(((0 0,0 0,0 1,0 0)))', 1);
SELECT 'patchN_03', ST_patchN('POLYHEDRALSURFACE(((0 0,0 0,0 1,0 0)))', 0);
SELECT 'patchN_04', ST_patchN('POLYHEDRALSURFACE(((0 0,0 0,0 1,0 0)))', 2);
SELECT 'patchN_05', ST_patchN('POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))', 2);

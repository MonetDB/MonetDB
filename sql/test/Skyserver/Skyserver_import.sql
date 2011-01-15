SET SCHEMA "skyserver";

COPY 25000 RECORDS INTO RC3 FROM 'DATA/RC3.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 30000 RECORDS INTO Stetson FROM 'DATA/Stetson.dat.bz2' USING DELIMITERS '\t', '\015\n', '"' NULL as '';

COPY 1000 RECORDS INTO QsoCatalogAll FROM 'DATA/QsoCatalogAll.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 100 RECORDS INTO QsoConcordance FROM 'DATA/QsoConcordance.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 3000 RECORDS INTO Target FROM 'DATA/Target.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO TilingRun FROM 'DATA/TilingRun.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO TargetParam FROM 'DATA/TargetParam.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';


COPY 1000 RECORDS INTO Rmatrix FROM 'DATA/Rmatrix.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

select 'Region';
COPY 28000 RECORDS INTO Region FROM 'DATA/Region1.dat.bz2' USING DELIMITERS '\t', '|', '"';

COPY 1000 RECORDS INTO SiteDBs FROM 'DATA/SiteDBs.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO QueryResults FROM 'DATA/QueryResults.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';


COPY 1000 RECORDS INTO RecentQueries FROM 'DATA/RecentQueries.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO SiteConstants FROM 'DATA/SiteConstants.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO Glossary FROM 'DATA/Glossary1.dat.bz2' USING DELIMITERS '\t', '|'; 

COPY 1000 RECORDS INTO Algorithm FROM 'DATA/Algorithm1.dat.bz2' USING DELIMITERS '\t', '$', '"';

COPY 1000 RECORDS INTO TableDesc FROM 'DATA/TableDesc1.dat.bz2' USING DELIMITERS '\t', '|', '"';

COPY 1000 RECORDS INTO DBObjects FROM 'DATA/DBObjects1.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO History FROM 'DATA/History.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO Dependency FROM 'DATA/Dependency.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO PubHistory FROM 'DATA/PubHistory.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO LoadHistory FROM 'DATA/LoadHistory.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO "Diagnostics" FROM 'DATA/Diagnostics.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO SiteDiagnostics FROM 'DATA/SiteDiagnostics.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

--select 'Versions';
COPY 1000 RECORDS INTO Versions FROM 'DATA/Versions.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO PartitionMap FROM 'DATA/PartitionMap1.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO DataConstants FROM 'DATA/DataConstants1.dat.bz2' USING DELIMITERS '\t', '|', '"';

COPY 1000 RECORDS INTO SDSSConstants FROM 'DATA/SDSSConstants1.dat.bz2' USING DELIMITERS '|', '\015\n', '"';

COPY 1000 RECORDS INTO StripeDefs FROM 'DATA/StripeDefs1.dat.bz2' USING DELIMITERS '\t', '\015\n', '"' NULL as '';

COPY 1000 RECORDS INTO RunShift FROM 'DATA/RunShift.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO ProfileDefs FROM 'DATA/ProfileDefs.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

--select 'Mask';
COPY 20000 RECORDS INTO Mask FROM 'DATA/Mask.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 26000 RECORDS INTO "Match" FROM 'DATA/Match.dat.bz2'  USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO SpecObjAll FROM 'DATA/SpecObjAll.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO HoleObj FROM 'DATA/HoleObj.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO SpecPhotoAll FROM 'DATA/SpecPhotoAll.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO QuasarCatalog FROM 'DATA/QuasarCatalog.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

--COPY 28000 RECORDS INTO SpecLineIndex FROM 'DATA/SpecLineIndex1.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 39000 RECORDS INTO SpecLineAll FROM 'DATA/SpecLineAll.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 41000 RECORDS INTO XCRedshift FROM 'DATA/XCRedshift.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO ELRedShift FROM 'DATA/ELRedShift.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 3000 RECORDS INTO TargetInfo FROM 'DATA/TargetInfo.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

select 'TiledTargetAll';
COPY 815000 RECORDS INTO TiledTargetAll FROM 'DATA/TiledTargetAll.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 2000 RECORDS INTO PlateX FROM 'DATA/PlateX.dat.bz2'  USING DELIMITERS '\t', '\015\n', '"';

COPY 80000 RECORDS INTO Sector2Tile FROM 'DATA/Sector2Tile.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 760000 RECORDS INTO TilingInfo FROM 'DATA/TilingInfo.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 2000 RECORDS INTO TileAll FROM 'DATA/TileAll.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO TilingNote FROM 'DATA/TilingNote.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO TilingGeometry FROM 'DATA/TilingGeometry.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 40000 RECORDS INTO RegionConvex FROM 'DATA/RegionConvex.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 21000 RECORDS INTO Region2Box FROM 'DATA/Region2Box.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 130000 RECORDS INTO RegionArcs FROM 'DATA/RegionArcs.dat.bz2'  USING DELIMITERS '\t', '\015\n', '"';

COPY 10000 RECORDS INTO Sector FROM 'DATA/Sector1.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 130000 RECORDS INTO HalfSpace FROM 'DATA/HalfSpace.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO Inventory FROM 'DATA/Inventory.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 3000 RECORDS INTO DBColumns FROM 'DATA/DBColumns1.dat.bz2' USING DELIMITERS '\t', '\015\n', '"' NULL as '';

COPY 1000 RECORDS INTO DBViewCols FROM 'DATA/DBViewCols.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO IndexMap FROM 'DATA/IndexMap1.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 10000 RECORDS INTO BestTarget2Sector FROM 'DATA/BestTarget2Sector.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

--select 'Segment';
COPY 2000 RECORDS INTO Segment FROM 'DATA/Segment.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO Field FROM 'DATA/Field.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 200300 RECORDS INTO PhotoObjAll FROM 'DATA/PhotoObjAll.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';
select count(*) from PhotoObjAll;

COPY 200300 RECORDS INTO PhotoTag FROM 'DATA/PhotoTag.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 2000 RECORDS INTO Frame FROM 'DATA/Frame.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO RunQA FROM 'DATA/RunQA.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 13000 RECORDS INTO FieldProfile FROM 'DATA/FieldProfile.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 7000000 RECORDS INTO PhotoProfile FROM 'DATA/PhotoProfile.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 26000 RECORDS INTO USNOB FROM 'DATA/USNOB.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 22000 RECORDS INTO USNO FROM 'DATA/USNO.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 200300 RECORDS INTO PhotoAuxAll FROM 'DATA/PhotoAuxAll.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO Photoz FROM 'DATA/Photoz.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO First FROM 'DATA/First.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO Rosat FROM 'DATA/Rosat.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';


COPY 200000 RECORDS INTO ObjMask FROM 'DATA/ObjMask.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 15000 RECORDS INTO MatchHead FROM 'DATA/MatchHead.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 3000 RECORDS INTO MaskedObject FROM 'DATA/MaskedObject.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1100000 RECORDS INTO Neighbors FROM 'DATA/Neighbors.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

--COPY 200000 RECORDS INTO Zone FROM 'DATA/Zone.dat.bz2' USING DELIMITERS '\t', '\015\n';

COPY 1000 RECORDS INTO FileGroupMap FROM 'DATA/FileGroupMap.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

COPY 1000 RECORDS INTO Chunk FROM 'DATA/Chunk.dat.bz2' USING DELIMITERS '\t', '\015\n', '"';

select 'ALL DONE';

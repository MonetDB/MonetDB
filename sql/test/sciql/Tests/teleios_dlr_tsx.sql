-- inspect SQL catalog
select * from schemas as s join tables as t on s.id = t.schema_id where s.name = 'rs';
select * from rs.files as f join rs.catalog as c on f.fileid = c.fileid;

-- attach TSX (GeoTIFF) images to DB
call rs.attach('/tmp/DLR/Collection01/TSX1_SAR__MGD_RE___HS_S_SRA_20080105T093811_20080105T093812/IMAGEDATA/IMAGE_HH_SRA_spot_042.tif');
call rs.attach('/tmp/DLR/Collection01/TSX1_SAR__MGD_RE___HS_S_SRA_20080216T083621_20080216T083621/IMAGEDATA/IMAGE_HH_SRA_spot_050.tif');
call rs.attach('/tmp/DLR/Collection01/TSX1_SAR__MGD_RE___HS_S_SRA_20080303T225930_20080303T225931/IMAGEDATA/IMAGE_HH_SRA_spot_041.tif');
call rs.attach('/tmp/DLR/Collection01/TSX1_SAR__MGD_RE___HS_S_SRA_20080424T141832_20080424T141832/IMAGEDATA/IMAGE_HH_SRA_spot_043.tif');
call rs.attach('/tmp/DLR/Collection01/TSX1_SAR__MGD_RE___HS_S_SRA_20080807T013407_20080807T013407/IMAGEDATA/IMAGE_HH_SRA_spot_042.tif');
call rs.attach('/tmp/DLR/Collection02/TSX1_SAR__MGD_RE___HS_S_SRA_20080220T135055_20080220T135055/IMAGEDATA/IMAGE_HH_SRA_spot_050.tif');
call rs.attach('/tmp/DLR/Collection02/TSX1_SAR__MGD_RE___HS_S_SRA_20080220T135244_20080220T135244/IMAGEDATA/IMAGE_HH_SRA_spot_048.tif');
call rs.attach('/tmp/DLR/Collection02/TSX1_SAR__MGD_RE___HS_S_SRA_20080221T150727_20080221T150728/IMAGEDATA/IMAGE_HH_SRA_spot_043.tif');
call rs.attach('/tmp/DLR/Collection02/TSX1_SAR__MGD_RE___HS_S_SRA_20080221T150920_20080221T150920/IMAGEDATA/IMAGE_HH_SRA_spot_042.tif');
call rs.attach('/tmp/DLR/Collection02/TSX1_SAR__MGD_RE___HS_S_SRA_20080221T150945_20080221T150946/IMAGEDATA/IMAGE_HH_SRA_spot_048.tif');
call rs.attach('/tmp/DLR/Collection02/TSX1_SAR__MGD_RE___HS_S_SRA_20080427T150945_20080427T150946/IMAGEDATA/IMAGE_HH_SRA_spot_048.tif');
call rs.attach('/tmp/DLR/Collection02/TSX1_SAR__MGD_RE___HS_S_SRA_20090127T150946_20090127T150946/IMAGEDATA/IMAGE_HH_SRA_spot_049.tif');
call rs.attach('/tmp/DLR/Collection06/TSX1_SAR__MGD_RE___HS_S_SRA_20080105T093821_20080105T093821/IMAGEDATA/IMAGE_HH_SRA_spot_051.tif');
call rs.attach('/tmp/DLR/Collection07/TSX1_SAR__MGD_RE___HS_S_SRA_20080308T151336_20080308T151337/IMAGEDATA/IMAGE_HH_SRA_spot_052.tif');
call rs.attach('/tmp/DLR/Collection07/TSX1_SAR__MGD_RE___HS_S_SRA_20080417T162304_20080417T162305/IMAGEDATA/IMAGE_HH_SRA_spot_048.tif');
call rs.attach('/tmp/DLR/Collection08/TSX1_SAR__MGD_RE___HS_S_SRA_20080417T130704_20080417T130705/IMAGEDATA/IMAGE_HH_SRA_spot_047.tif');
call rs.attach('/tmp/DLR/Collection08/TSX1_SAR__MGD_RE___HS_S_SRA_20080417T130747_20080417T130748/IMAGEDATA/IMAGE_HH_SRA_spot_044.tif');
call rs.attach('/tmp/DLR/Collection08/TSX1_SAR__MGD_RE___HS_S_SRA_20080417T130846_20080417T130847/IMAGEDATA/IMAGE_HH_SRA_spot_045.tif');
call rs.attach('/tmp/DLR/Collection09/TSX1_SAR__MGD_RE___HS_S_SRA_20080904T231959_20080904T232000/IMAGEDATA/IMAGE_HH_SRA_spot_035.tif');

-- inspect SQL catalog
select * from schemas as s join tables as t on s.id = t.schema_id where s.name = 'rs';
select * from rs.files as f join rs.catalog as c on f.fileid = c.fileid;

-- load TSX (GeoTIFF) images into arrays
call rs.import(1);
call rs.import(2);
call rs.import(3);
call rs.import(4);
call rs.import(5);
call rs.import(6);
call rs.import(7);
call rs.import(8);
call rs.import(9);
call rs.import(10);
call rs.import(11);
call rs.import(12);
call rs.import(13);
call rs.import(14);
call rs.import(15);
call rs.import(16);
call rs.import(17);
call rs.import(18);
call rs.import(19);

-- inspect SQL catalog
select * from schemas as s join tables as t on s.id = t.schema_id where s.name = 'rs';
select * from rs.files as f join rs.catalog as c on f.fileid = c.fileid;

-- inspect loaded images / arrays
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image1;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image2;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image3;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image4;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image5;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image6;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image7;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image8;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image9;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image10;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image11;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image12;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image13;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image14;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image15;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image16;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image17;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image18;
select min(x), max(x), min(y), max(y), min(intensity), max(intensity) from rs.image19;

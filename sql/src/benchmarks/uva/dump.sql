
set pagesize 50000;
set linesize 2048;
set colsep ',';
set heading OFF;
set trimspool ON;

spool VxSegmentation_tab.txt;
select * from VxSegmentation_tab;
spool VxSegment_tab.txt;
select * from VxSegment_tab;
spool VxSegmentStringFeature_tab.txt;
select * from VxSegmentStringFeature_tab;
spool VxSegmentDoubleFeature_tab.txt;
select * from VxSegmentDoubleFeature_tab;
spool VxSegmentIntFeature_tab.txt;
select * from VxSegmentIntFeature_tab;
quit;

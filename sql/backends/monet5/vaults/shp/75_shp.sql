create procedure SHPattach(fname string) external name shp.attach;
create procedure SHPload(fid integer) external name shp.load;
create procedure SHPload(fid integer, filter geometry) external name shp.import;

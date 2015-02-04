

create procedure listdir(dirname string) external name fits.listdir;
create procedure fitsattach(fname string) external name fits.attach;
create procedure fitsload(tname string) external name fits.load;
create procedure listdirpat(dirname string,pat string) external name fits.listdirpattern;

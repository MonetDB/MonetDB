create procedure lidarattach(fname string) external name lidar.attach;
create procedure lidarload(tname string) external name lidar.load;
create procedure lidarexport(tname string, fname string, format string) external name lidar.export;

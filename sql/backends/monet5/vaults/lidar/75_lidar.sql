-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

create procedure lidarattach(fname string) external name lidar.attach;
create procedure lidarload(tname string) external name lidar.load;
create procedure lidarexport(tname string, fname string, format string) external name lidar.export;

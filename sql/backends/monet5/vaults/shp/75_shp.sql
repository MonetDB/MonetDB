-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

create procedure SHPattach(fname string) external name shp.attach;
create procedure SHPload(fid integer) external name shp.import;
create procedure SHPload(fid integer, filter geometry) external name shp.import;

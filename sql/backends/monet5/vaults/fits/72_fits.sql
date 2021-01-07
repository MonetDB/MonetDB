-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.


create procedure listdir(dirname string) external name fits.listdir;
create procedure fitsattach(fname string) external name fits.attach;
create procedure fitsload(tname string) external name fits.load;
create procedure listdirpat(dirname string,pat string) external name fits.listdirpattern;

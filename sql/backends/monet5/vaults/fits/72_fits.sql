-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2024, 2025 MonetDB Foundation;
-- Copyright August 2008 - 2023 MonetDB B.V.;
-- Copyright 1997 - July 2008 CWI.

create procedure listdir(dirname string) external name fits.listdir;
create procedure fitsattach(fname string) external name fits.attach;
create procedure fitsload(tname string) external name fits.load;
create procedure listdirpat(dirname string,pat string) external name fits.listdirpattern;

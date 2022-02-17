-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.



create function sys.copy_blocksize()
returns int
external name "copy".get_blocksize;

create procedure sys.copy_blocksize(size int)
external name "copy".set_blocksize;

create function sys.copy_parallel()
returns bool
external name "copy".get_parallel;

create procedure sys.copy_parallel(parallel bool)
external name "copy".set_parallel;

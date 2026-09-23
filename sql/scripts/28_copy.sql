-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at https://mozilla.org/MPL/2.0/.
--
-- For copyright information, see the file debian/copyright.

create function sys.copy_blocksize()
returns int
external name "copy".get_blocksize;

create procedure sys.copy_blocksize(size int)
external name "copy".set_blocksize;

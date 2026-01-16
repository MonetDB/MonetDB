-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- For copyright information, see the file debian/copyright.

create function sys.regexp_replace(ori string, pat string, rep string, flg string)
returns string external name pcre.replace;

grant execute on function regexp_replace(string, string, string, string) to public;

create function sys.regexp_replace(ori string, pat string, rep string)
returns string
begin
    return sys.regexp_replace(ori, pat, rep, '');
end;

grant execute on function regexp_replace(string, string, string) to public;

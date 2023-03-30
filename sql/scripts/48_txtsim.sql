-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

-- Calculates Levenshtein distance between two strings,
-- operation costs ins/del = 1, replacement = 1
create function sys.levenshtein(x string, y string)
returns int external name txtsim.levenshtein;
grant execute on function levenshtein(string, string) to public;

-- Calculates Levenshtein distance between two strings,
-- variable operation costs (ins/del, replacement)
create function sys.levenshtein(x string, y string, insdel int, rep int)
returns int external name txtsim.levenshtein;
grant execute on function levenshtein(string, string, int, int) to public;

-- Calculates Levenshtein distance between two strings,
-- variable operation costs (ins/del, replacement, transposition)
-- backwards compatibility purposes(this should be Damerau Levenshtein)
create function sys.levenshtein(x string, y string, insdel int, rep int, trans int)
returns int external name txtsim.levenshtein;
grant execute on function levenshtein(string, string, int, int, int) to public;

-- Calculates Damerau-Levenshtein distance between two strings,
-- operation costs ins/del = 1, replacement = 1, transposition = 2
create function sys.damerau_levenshtein(x string, y string)
returns int external name txtsim.damerau_levenshtein;
grant execute on function damerau_levenshtein(string, string) to public;

-- Calculates Damerau-Levenshtein distance between two strings,
-- variable operation costs (ins/del, replacement, transposition)
create function sys.damerau_levenshtein(x string, y string, insdel int, rep int, trans int)
returns int external name txtsim.damerau_levenshtein;
grant execute on function damerau_levenshtein(string, string, int, int, int) to public;

-- Alias for Damerau-Levenshtein(str,str), insdel cost = 1, replace cost = 1 and transpose = 2
create function sys.editdistance(x string, y string)
returns int external name txtsim.editdistance;
grant execute on function editdistance(string, string) to public;

-- Alias for Damerau-Levenshtein(str,str), insdel cost = 1, replace cost = 1 and transpose = 1
create function sys.editdistance2(x string, y string)
returns int external name txtsim.editdistance2;
grant execute on function editdistance2(string, string) to public;

-- Soundex function for phonetic matching
create function sys.soundex(x string)
returns string external name txtsim.soundex;
grant execute on function soundex(string) to public;

-- Calculate the soundexed editdistance
create function sys.difference(x string, y string)
returns int external name txtsim.stringdiff;
grant execute on function difference(string, string) to public;

-- 'Normalizes' strings (eg. toUpper and replaces non-alphanumerics with one space
create function sys.qgramnormalize(x string)
returns string external name txtsim.qgramnormalize;
grant execute on function qgramnormalize(string) to public;

-- (DEPRECATED) Normalized edit distance between two strings
create function sys.similarity(x string, y string)
returns double external name txtsim.similarity;
grant execute on function similarity(string, string) to public;

-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

CREATE SEQUENCE sys.vaultid AS int;

CREATE TABLE sys.vault (
vid 			int PRIMARY KEY,-- Internal key
kind			string,			-- vault kind (CSV, FITS,..)
source			string,			-- remote file name for cURL to access
target			string,			-- file name of source file in vault
created			timestamp,		-- timestamp upon entering the cache
lru				timestamp		-- least recently used
);

create function vaultLocation()
returns string
external name vault.getdirectory;

create function vaultSetLocation(dir string)
returns string
external name vault.setdirectory;

create function vaultBasename(fnme string, split string)
returns string
external name vault.basename;

create function vaultImport(source string, target string)
returns timestamp
external name vault.import;

create function vaultRemove(target string)
returns timestamp
external name vault.remove;

create procedure vaultVacuum( t timestamp)
begin
update vault
  set created= remove(target),
  lru = null
  where  created < t;
end;

-- The contents of this file are subject to the MonetDB Public License
-- Version 1.1 (the "License"); you may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
-- http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
--
-- Software distributed under the License is distributed on an "AS IS"
-- basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
-- License for the specific language governing rights and limitations
-- under the License.
--
-- The Original Code is the MonetDB Database System.
--
-- The Initial Developer of the Original Code is CWI.
-- Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
-- Copyright August 2008-2011 MonetDB B.V.
-- All Rights Reserved.



CREATE SEQUENCE sys.vaultid AS int;

CREATE TABLE sys.vault (
vid 			int PRIMARY KEY,-- Internal key
kind			string,			-- vault kind (CSV, MSEED, FITS,..)
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

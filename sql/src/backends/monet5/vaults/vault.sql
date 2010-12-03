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
-- Copyright August 2008-2010 MonetDB B.V.
-- All Rights Reserved.

-- The data vault interface for SQL

CREATE SEQUENCE sys.vaultid AS int;

CREATE TABLE sys.vault (
vid             int PRIMARY KEY DEFAULT NEXT VALUE FOR sys.vaultid ,
kind            string,         -- vault kind (CSV, MSEED, FITS,..)
source          string,         -- remote file name for cURL to access
target          string,         -- location of source file in the local vault
refresh         boolean,        -- refresh each time of access
cached          timestamp       -- when a local copy was stored
);

create function getVaultDir()
returns string
external name vault.getdirectory;

-- refresh the vault
create procedure refreshVault(nme string)
external name vault.refresh;


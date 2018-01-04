-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- only system functions until now
create table systemfunctions (function_id)
	as (select id from functions) with data;
grant select on systemfunctions to public;

create trigger system_update_schemas after update on sys.schemas for each statement call sys_update_schemas(); 
create trigger system_update_tables after update on sys._tables for each statement call sys_update_tables(); 

-- only system tables until now
update _tables set system = true;

-- only system schemas until now
update schemas set system = true;

-- correct invalid FK schema ids, set them to schema id 2000 (the "sys" schema)
UPDATE sys.types     SET schema_id = (SELECT id FROM sys.schemas WHERE name = 'sys') WHERE schema_id = 0 AND schema_id NOT IN (SELECT id from sys.schemas);
UPDATE sys.functions SET schema_id = (SELECT id FROM sys.schemas WHERE name = 'sys') WHERE schema_id = 0 AND schema_id NOT IN (SELECT id from sys.schemas);

-- make sure all gets commited
COMMIT;

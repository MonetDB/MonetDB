-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

create trigger system_update_schemas after update on sys.schemas for each statement call sys_update_schemas();
create trigger system_update_tables after update on sys._tables for each statement call sys_update_tables();

-- only system functions until now
update sys.functions set system = true;
create view sys.systemfunctions as select id as function_id from sys.functions where system;
grant select on sys.systemfunctions to public;

-- only system tables until now
update sys._tables set system = true;

-- only system schemas until now
update sys.schemas set system = true;

-- correct invalid FK schema ids, set them to schema id 2000 (the "sys" schema)
UPDATE sys.types     SET schema_id = (SELECT id FROM sys.schemas WHERE name = 'sys') WHERE schema_id = 0 AND schema_id NOT IN (SELECT id from sys.schemas);
UPDATE sys.functions SET schema_id = (SELECT id FROM sys.schemas WHERE name = 'sys') WHERE schema_id = 0 AND schema_id NOT IN (SELECT id from sys.schemas);

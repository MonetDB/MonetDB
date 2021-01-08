-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

-- Workload Capture and Replay

-- Replica commands

create schema wlr;

create procedure wlr.master(dbname string)
external name wlr.master;

create procedure wlr.stop()
external name wlr.stop;

-- accept the error reported an skip the record
create procedure wlr.accept()
external name wlr.accept;

-- run it forever
create procedure wlr.replicate()
external name wlr.replicate;

-- run replicator until condition is met
create procedure wlr.replicate(pointintime timestamp)
external name wlr.replicate;

create procedure wlr.replicate(id tinyint)
external name wlr.replicate;

create procedure wlr.replicate(id smallint)
external name wlr.replicate;

create procedure wlr.replicate(id integer)
external name wlr.replicate;

create procedure wlr.replicate(id bigint)
external name wlr.replicate;

-- control the interval for replication 
create procedure wlr.beat(duration integer)
external name wlr."setbeat";

create function wlr.clock() returns string
external name wlr."getclock";

create function wlr.tick() returns bigint
external name wlr."gettick";


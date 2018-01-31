-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- start/stop collecting MDBS profiler traces in a profiler_logs pool with a specific heartbeat
create schema profiler;

create procedure profiler.start() external name profiler."start";
create procedure profiler.stop() external name profiler.stop;

create procedure profiler.setheartbeat(beat int) external name profiler.setheartbeat;
create function profiler.getlimit() returns integer external name profiler.getlimit;
create procedure  profiler.setlimit(lim integer) external name profiler.setlimit;

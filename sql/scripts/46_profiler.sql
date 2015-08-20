-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2008-2015 MonetDB B.V.

-- start/stop collecting mdbs_profiler traces in profiler_logs pool with specific heartbeat
create schema mdbs_profiler;

create procedure mdbs_profiler.start() external name profiler.start;
create procedure mdbs_profiler.stop() external name profiler.stop;

create procedure mdbs_profiler.setheartbeat(beat int) external name profiler.setheartbeat;
create procedure mdbs_profiler.setpoolsize(poolsize int) external name profiler.setpoolsize;

create procedure mdbs_profiler.setstream(host string, port int) external name profiler.setstream;

-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2008-2015 MonetDB B.V.

-- Clustering a relational table should be done with care.
-- For, the oid's are used in join-indices.

-- Clustering of tables may improve IO performance
-- The foreign key constraints should be dropped before
-- and re-established after the cluster operation.

create procedure cluster1(sys string, tab string)
	external name sql.cluster1;

create procedure cluster2(sys string, tab string)
	external name sql.cluster2;


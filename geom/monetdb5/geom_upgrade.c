/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * @a Kostis Kyzirakos, Foteini Alvanaki
 */


#include "geom.h"
#include "gdk_logger.h"

str
geom_sql_upgrade(int olddb)
{
	size_t bufsize = 4096000, pos = 0;
	str buf;

	if ((buf = GDKmalloc(bufsize)) == NULL)
		return NULL;

	/* drop old functions */
	if (olddb) {
		pos += snprintf(buf + pos, bufsize - pos,
				"drop function \"st_makeline\";\n"
				"drop function \"st_intersects\";\n"
				"drop function \"st_dwithin\";\n"
				"drop function \"st_collect\";\n"
				"drop aggregate \"st_collect\";\n");
	}

	/* create the new geometry types */
	pos += snprintf(buf + pos, bufsize - pos,
			"CREATE FUNCTION ST_DistanceGeographic(geom1 Geometry, geom2 Geometry) RETURNS double EXTERNAL NAME geom.\"DistanceGeographic\";\n"
			"GRANT EXECUTE ON FUNCTION ST_DistanceGeographic(Geometry, Geometry) TO PUBLIC;\n"
			"CREATE FILTER FUNCTION ST_DWithinGeographic(geom1 Geometry, geom2 Geometry, distance double) EXTERNAL NAME geom.\"DWithinGeographic\";\n"
			"GRANT EXECUTE ON FILTER ST_DWithinGeographic(Geometry, Geometry, double) TO PUBLIC;\n"
			"CREATE FILTER FUNCTION ST_IntersectsGeographic(geom1 Geometry, geom2 Geometry) EXTERNAL NAME geom.\"IntersectsGeographic\";\n"
			"GRANT EXECUTE ON FILTER ST_IntersectsGeographic(Geometry, Geometry) TO PUBLIC;\n"
			"CREATE AGGREGATE ST_Collect(geom Geometry) RETURNS Geometry external name aggr.\"Collect\";\n"
			"GRANT EXECUTE ON AGGREGATE ST_Collect(Geometry) TO PUBLIC;\n"
			"CREATE FILTER FUNCTION ST_Intersects(geom1 Geometry, geom2 Geometry) EXTERNAL NAME rtree.\"Intersects\";\n"
			"GRANT EXECUTE ON FILTER ST_Intersects(Geometry, Geometry) TO PUBLIC;\n"
			"CREATE FILTER FUNCTION ST_Intersects_NoIndex(geom1 Geometry, geom2 Geometry) EXTERNAL NAME geom.\"Intersects_noindex\";\n"
			"GRANT EXECUTE ON FILTER ST_Intersects_NoIndex(Geometry, Geometry) TO PUBLIC;\n"
			"CREATE FILTER FUNCTION ST_DWithin(geom1 Geometry, geom2 Geometry, distance double) EXTERNAL NAME rtree.\"DWithin\";\n"
			"GRANT EXECUTE ON FILTER ST_DWithin(Geometry, Geometry, double) TO PUBLIC;\n"
			"CREATE FILTER FUNCTION ST_DWithin_NoIndex(geom1 Geometry, geom2 Geometry, distance double) EXTERNAL NAME geom.\"DWithin_noindex\";\n"
			"GRANT EXECUTE ON FILTER ST_DWithin_NoIndex(Geometry, Geometry, double) TO PUBLIC;\n"
			"CREATE AGGREGATE ST_MakeLine(geom Geometry) RETURNS Geometry external name aggr.\"MakeLine\";\n"
			"GRANT EXECUTE ON AGGREGATE ST_MakeLine(Geometry) TO PUBLIC;\n"
			"CREATE FUNCTION ST_Collect(geom1 Geometry, geom2 Geometry) RETURNS Geometry EXTERNAL NAME geom.\"Collect\";\n"
			"GRANT EXECUTE ON FUNCTION ST_Collect(Geometry, Geometry) TO PUBLIC;\n");

	pos += snprintf(buf + pos, bufsize - pos,
			"update sys.functions set system = true where name in ("
			"'ST_DistanceGeographic', 'ST_DWithinGeographic', 'ST_IntersectsGeographic', 'ST_Collect',"
			"'ST_Intersects', 'ST_Intersects_NoIndex', 'ST_DWithin', 'ST_DWithin_NoIndex', 'ST_MakeLine') "
			"and schema_id = (select id from sys.schemas where name = 'sys');\n");
	return buf;
}

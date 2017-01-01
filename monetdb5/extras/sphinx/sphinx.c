/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @f sphinx
 * @a S.A.M.M. de Konink
 * @v 0.2
 * @* The Sphinx module
 * The Sphinx module implements an external full text search engine returning a
 * list of identifiers based on a query string and an index to search upon.
 *
 */
#include "monetdb_config.h"
#include "sphinx.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_exception.h"
#include <sphinxclient.h>

/* COMMAND "SPHINXsearchIndexLimit": Search the query on the specified indices, with limit
 * SIGNATURE: SPHINXsearchIndexLimit(str, str, int) : bat[oid,lng]; */
static str
sphinx_searchIndexLimit(BAT **ret, /* put pointer to BAT[oid,int] record here. */
						str query, str index, int limit)
{
	int i;
	BAT *bn;
	sphinx_client *client;
	sphinx_result *res;

	client = sphinx_create ( SPH_TRUE );
	if (client == NULL)
		throw(MAL, "sphinx.searchIndexLimit", "Cannot create Sphinx object");

	sphinx_set_limits ( client, 0, limit, limit, 0 );

	res = sphinx_query ( client, query, index, NULL );
	if (!res || (res && res->num_matches == 0)) {
		bn = COLnew(0, TYPE_lng, 0, TRANSIENT);
		if (bn == NULL)
			throw(MAL, "sphinx.searchIndex", MAL_MALLOC_FAIL);
	} else {
		bn = COLnew(0, TYPE_lng, res->num_matches, TRANSIENT);
		if (bn == NULL)
			throw(MAL, "sphinx.searchIndex", MAL_MALLOC_FAIL);
		for ( i = 0; i < res->num_matches; i++ ) {
			lng sphinx_id = sphinx_get_id ( res, i );
			bunfastapp(bn, &sphinx_id);
		}

	}
	sphinx_destroy (client);

	bn->tsorted = 0;
	bn->trevsorted = 0;
	bn->tnonil = 1;
	BATkey(bn, FALSE);

	*ret = bn;
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(bn->batCacheid);
	sphinx_destroy(client);
	throw(MAL, "sphinx.searchIndex", MAL_MALLOC_FAIL);
}

str
SPHINXsearchIndexLimit(bat *ret, const str *query, const str *index, const int *limit)
{
	BAT *b = NULL;
	str msg = sphinx_searchIndexLimit(&b, *query, *index, *limit);

	if (msg) {
		return msg;
	}
	assert(b != NULL);
	*ret = b->batCacheid;
	BBPkeepref(*ret);
	return msg;
}


/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
	oid o = 0;

	client = sphinx_create ( SPH_TRUE );
	if (client == NULL)
		throw(MAL, "sphinx.searchIndexLimit", "Cannot create Sphinx object");

	sphinx_set_limits ( client, 0, limit, limit, 0 );

	res = sphinx_query ( client, query, index, NULL );
	if (!res || (res && res->num_matches == 0)) {
		bn = BATnew(TYPE_void, TYPE_lng, 0);
	} else {
		bn = BATnew(TYPE_void, TYPE_lng, res->num_matches);
		for ( i = 0; i < res->num_matches; i++ ) {
			lng sphinx_id = sphinx_get_id ( res, i );
			o++;
			BUNfastins(bn, &o, &sphinx_id);
		}

	}
	sphinx_destroy (client);

	bn->hseqbase = 0;
        bn->T->sorted = 0;
        bn->T->nonil = 1;
	BATkey(BATmirror(bn), FALSE);

	*ret = bn;
	return MAL_SUCCEED;
}

str
SPHINXsearchIndexLimit(int *ret, str *query, str *index, int *limit)
{
	BAT *b = NULL;
	str msg = sphinx_searchIndexLimit(&b, *query, *index, *limit);

	if (!b)
		throw(MAL, "sphinx.searchIndex", "Cannot create Sphinx object");
	*ret = b->batCacheid;
	BBPkeepref(*ret);
	return msg;
}

/* str sphinx_searchIndexLimit(int *ret, str *query, str *index, int *limit); */
str
SPHINXsearchIndexLimitWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat ret;
	BAT *t;
	int *r = (int *) getArgReference(stk, pci, 0);
	str *query = (str *) getArgReference(stk, pci, 1);
	str *index = (str *) getArgReference(stk, pci, 2);
	int *limit = (int *) getArgReference(stk, pci, 3);

	(void) cntxt;
	(void) mb;
	SPHINXsearchIndexLimit(&ret, query, index, limit);

	t = BATnew(TYPE_str, TYPE_bat, 1);
	BUNins(t, "id", &ret, FALSE);

	BBPdecref(ret, TRUE);
	*r = t->batCacheid;
	BBPkeepref(*r);
	return MAL_SUCCEED;
}


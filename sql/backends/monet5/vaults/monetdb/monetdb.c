/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "rel_proto_loader.h"
#include "rel_exp.h"

#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "mal_parser.h"
#include "mal_builder.h"
#include "mal_namespace.h"
#include "mal_exception.h"
#include "mal_linker.h"
#include "mal_backend.h"
#include "sql_types.h"
#include "rel_bin.h"
#include "sql_storage.h"
#include "mapi.h"
#include "msettings.h"

#include "rel_remote.h"
#include "rel_basetable.h"
#include <unistd.h>

static char *sql_template(allocator *sa, const char **parts);

typedef struct mdb_loader_t {
	char *uri;
	const char *sname;
	const char *tname;
} mdb_loader_t;


/*
 * returns an error string (static or via tmp sa_allocator allocated), NULL on success
 *
 * Extend the subfunc f with result columns, ie.
	f->res = typelist;
	f->coltypes = typelist;
	f->colnames = nameslist; use tname if passed, for the relation name
 * Fill the list res_exps, with one result expressions per resulting column.
 */
static str
monetdb_relation(mvc *sql, sql_subfunc *f, char *raw_uri, list *res_exps, char *aname)
{
	str ret; // intentionally uninitialized to provoke control flow warnings

	const char *uri_error = NULL;
	Mapi dbh = NULL;
	MapiHdl hdl = NULL;

	// Normalize uri
	msettings *mp = sa_msettings_create(sql->sa);
	if (!mp) {
		ret = sa_message(sql->sa, "could not allocate msettings");
		goto end;
	}

	if (
		(uri_error = msettings_parse_url(mp, raw_uri))
		|| (uri_error = msettings_validate(mp))
	) {
		ret = sa_message(sql->sa, "uri '%s' invalid: %s\n", raw_uri, uri_error);
		goto end;
	}
	const char *uri = sa_msettings_to_string(mp, sql->sa, strlen(raw_uri));

	const char *sname = msetting_string(mp, MP_TABLESCHEMA);   // not MP_SCHEMA, that's something else
	const char *tname = msetting_string(mp, MP_TABLE);
	assert(sname != NULL && tname != NULL); // msetting_string() never returns NULL, can return ""
	if (!sname[0] || !tname[0]) {
		ret = sa_message(sql->sa, "monetdb_loader" "schema and/or table missing in '%s'\n", uri);
		goto end;
	}

	/* set up mapi connection; user and password will possibly be overridden in the uri */
	dbh = mapi_mapiuri(uri, "monetdb", "monetdb", "sql");
	if (dbh == NULL) {
		ret = MAL_MALLOC_FAIL;
		goto end;
	}
	if (mapi_reconnect(dbh) < 0) {
		ret = sa_strdup(sql->sa, mapi_error_str(dbh));
		goto end;
	}
	mapi_cache_limit(dbh, 100);

	/* construct the query with proper quoting */
	char *query;
	query = sql_template(sql->sa, (const char*[]) {
		"select c.name, c.type, c.type_digits, c.type_scale from sys.schemas s, sys._tables t, sys._columns c where s.name = R'",
		sname,
		"' and s.id = t.schema_id and t.name = R'",
		tname,
		"' and t.id = c.table_id order by c.number;",
		NULL
	});

	if ((hdl = mapi_query(dbh, query)) == NULL || mapi_error(dbh)) {
		ret = sa_strdup(sql->sa, mapi_error_str(dbh));
		goto end;
	}

	if (mapi_get_row_count(hdl) == 0) { /* non existing table */
		ret = sa_message(sql->sa, "Table %s.%s is missing on remote server", sname, tname);
		goto end;
	}

	if (!aname)
		aname = sa_strdup(sql->sa, tname);

	f->tname = sa_strdup(sql->sa, aname);

	list *typelist = sa_list(sql->sa);
	list *nameslist = sa_list(sql->sa);
	while (mapi_fetch_row(hdl)) {
		char *nme = sa_strdup(sql->sa, mapi_fetch_field(hdl, 0));
		char *tpe = mapi_fetch_field(hdl, 1);
		char *dig = mapi_fetch_field(hdl, 2);
		char *scl = mapi_fetch_field(hdl, 3);

		sql_subtype *t = NULL;
		if (scl && scl[0]) {
			int d = (int)strtol(dig, NULL, 10);
			int s = (int)strtol(scl, NULL, 10);
			t = sql_bind_subtype(sql->sa, tpe, d, s);
		} else if (dig && dig[0]) {
			int d = (int)strtol(dig, NULL, 10);
			t = sql_bind_subtype(sql->sa, tpe, d, 0);
		} else
			t = sql_bind_subtype(sql->sa, tpe, 0, 0);
		if (!t) {
			ret = sa_message(sql->sa, "monetdb_loader" "type %s not found\n", tpe);
			goto end;
		}
		sql_exp *ne = exp_column(sql->sa, f->tname, nme, t, CARD_MULTI, 1, 0, 0);
		set_basecol(ne);
		ne->alias.label = -(sql->nid++);
		list_append(res_exps, ne);
		list_append(typelist, t);
		list_append(nameslist, nme);
	}

	f->res = typelist;
	f->coltypes = typelist;
	f->colnames = nameslist;

	mdb_loader_t *r = (mdb_loader_t *)sa_alloc(sql->sa, sizeof(mdb_loader_t));
	r->sname = sname;
	r->tname = tname;
	r->uri = sa_strdup(sql->sa, uri);
	f->sname = (char*)r; /* pass mdb_loader */
	ret = NULL;

end:
	if (hdl)
		mapi_close_handle(hdl);
	if (dbh)
		mapi_destroy(dbh);
	// do not destroy mp because r->sname and r->tname point inside it
	return ret;
}

/* Return the concatenation of the arguments, with quotes doubled
 * ONLY IN THE ODD positions.
 * The list of arguments must be terminated with a NULL.
 *
 * For example, { "SELECT R'",        "it's",        "' AS bla",        NULL }
 * becomes "SELECT R'it''s' AS bla"
 */
static char *
sql_template(allocator *sa, const char **parts)
{
	int nparts = 0;
	while (parts[nparts] != NULL)
		nparts++;

	size_t max_length = 0;
	for (int i = 0; i < nparts; i++) {
		size_t length = strlen(parts[i]);
		if (i % 2 == 1)
			length += length;
		max_length += length;
	}
	char *result = sa_alloc(sa, max_length + 1);

	char *w = result;
	for (int i = 0; i < nparts; i++) {
		for (const char *r = parts[i]; *r; r++) {
			*w++ = *r;
			if (*r == '\'' && i % 2 == 1)
				*w++ = *r;  // double that quote
		}
	}

	assert(w <= result + max_length);
	return result;
}


static void *
monetdb_load(void *BE, sql_subfunc *f, char *uri, sql_exp *topn)
{
	(void)uri; // assumed to be equivalent to mdb_loader_t->uri, though maybe unnormalized.
	(void)topn;

	backend *be = (backend*)BE;
	mvc *sql = be->mvc;
	mdb_loader_t *r = (mdb_loader_t*)f->sname;
	char name[16], *nme;

	nme = number2name(name, sizeof(name), ++be->remote);

	/* create proper relation for remote side */
	/* table ( project ( REMOTE ( table ) [ cols ] ) [ cols ] REMOTE Prop ) [ cols ] */

	sql_table *t;
	if (mvc_create_table( &t, be->mvc, be->mvc->session->tr->tmp/* misuse tmp schema */, r->tname /*gettable name*/, tt_remote, false, SQL_DECLARED_TABLE, 0, 0, false) != LOG_OK)
		/* alloc error */
		return NULL;
	t->query = r->uri; /* set uri */
	node *n, *nn = f->colnames->h, *tn = f->coltypes->h;
	for (n = f->res->h; n; n = n->next, nn = nn->next, tn = tn->next) {
		const char *name = nn->data;
		sql_subtype *tp = tn->data;
		sql_column *c = NULL;

		if (!tp || mvc_create_column(&c, be->mvc, t, name, tp) != LOG_OK) {
			return NULL;
		}
	}

	sql_rel *rel = NULL;
	rel = rel_basetable(sql, t, f->tname);
	rel_base_use_all(sql, rel);

	rel = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 0, 0));
	prop *p = rel->p = prop_create(sql->sa, PROP_REMOTE, rel->p);
	tid_uri *tu = SA_NEW(sql->sa, tid_uri);
	tu->id = t->base.id;
	tu->uri = mapiuri_uri(r->uri, sql->sa);
	p->id = tu->id;
	p->value.pval = append(sa_list(sql->sa), tu);

	stmt *s = stmt_func(be, NULL, sa_strdup(sql->sa, nme), rel, 0);
	return s;
}

static str
MONETDBprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt; (void)mb; (void)stk; (void)pci;

	pl_register("monetdb", &monetdb_relation, &monetdb_load);
	pl_register("monetdbs", &monetdb_relation, &monetdb_load);
	pl_register("mapi", &monetdb_relation, &monetdb_load);
	return MAL_SUCCEED;
}

static str
MONETDBepilogue(void *ret)
{
	pl_unregister("monetdb");
	pl_unregister("monetdbs");
	pl_unregister("mapi");
	(void)ret;
	return MAL_SUCCEED;
}

#include "sql_scenario.h"
#include "mel.h"

static mel_func monetdb_init_funcs[] = {
	pattern("monetdb", "prelude", MONETDBprelude, false, "", noargs),
	command("monetdb", "epilogue", MONETDBepilogue, false, "", noargs),
{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_monetdb_mal)
{ mal_module("monetdb", NULL, monetdb_init_funcs); }

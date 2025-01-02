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

#include "rel_remote.h"
#include "rel_basetable.h"
#include <unistd.h>

typedef struct mdb_loader_t {
	char *uri;
	char *sname;
	char *tname;
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
monetdb_relation(mvc *sql, sql_subfunc *f, char *uri, list *res_exps, char *aname)
{
	char *uric = sa_strdup(sql->sa, uri);
	char *tname, *sname = NULL;

	if (!mapiuri_valid(uric))
		return sa_message(sql->sa, "monetdb_loader" "uri invalid '%s'\n", uri);

	tname = strrchr(uric, '/');
	if (tname) {
		tname[0] = 0;
		sname = strrchr(uric, '/');
	}
	if (!sname)
		return sa_message(sql->sa, "monetdb_loader" "schema and/or table missing in '%s'\n", uri);

	sname[0] = 0; /* stripped the schema/table name */
	sname++;
	tname++;
	char buf[256];
	const char *query = "select c.name, c.type, c.type_digits, c.type_scale from sys.schemas s, sys._tables t, sys._columns c where s.name = '%s' and s.id = t.schema_id and t.name = '%s' and t.id = c.table_id order by c.number;";
	if (snprintf(buf, 256, query, sname, tname) < 0)
		return RUNTIME_LOAD_ERROR;

	/* setup mapi connection */
	/* TODO get username password from uri! */
	Mapi dbh = mapi_mapiuri(uric, "monetdb", "monetdb", "sql");
	if (mapi_reconnect(dbh) < 0) {
		printf("%s\n", mapi_error_str(dbh));
		return RUNTIME_LOAD_ERROR;
	}
	mapi_cache_limit(dbh, 100);
	MapiHdl hdl;
	if ((hdl = mapi_query(dbh, buf)) == NULL || mapi_error(dbh)) {
		printf("%s\n", mapi_error_str(dbh));
		return RUNTIME_LOAD_ERROR;
	}

	if (!aname)
		aname = tname;

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
		if (!t)
			return sa_message(sql->sa, "monetdb_loader" "type %s not found\n", tpe);
		sql_exp *ne = exp_column(sql->sa, f->tname, nme, t, CARD_MULTI, 1, 0, 0);
		set_basecol(ne);
		ne->alias.label = -(sql->nid++);
		list_append(res_exps, ne);
		list_append(typelist, t);
		list_append(nameslist, nme);
	}
	mapi_close_handle(hdl);
	mapi_destroy(dbh);

	f->res = typelist;
	f->coltypes = typelist;
	f->colnames = nameslist;

	mdb_loader_t *r = (mdb_loader_t *)sa_alloc(sql->sa, sizeof(mdb_loader_t));
	r->sname = sname;
	r->tname = tname;
	r->uri = uric;
	f->sname = (char*)r; /* pass mdb_loader */
	return NULL;
}

static void *
monetdb_load(void *BE, sql_subfunc *f, char *uri, sql_exp *topn)
{
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
	t->query = uri; /* set uri */
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
	tu->uri = r->uri;
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

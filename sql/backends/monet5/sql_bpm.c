/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * @f sql_bpm
 */
#include <monetdb_config.h>
#include <sql_bpm.h>
#include <sql_mem.h>
#include <sql_mvc.h>
#include <sql.h>
#include <mal_exception.h>
#include <bpm/bpm_storage.h>
#include <bat/bat_utils.h>
#include <mal_mapi.h>

static BAT *
mat_bind(mvc *m, char *sname, char *tname, char *cname, int access, int part)
{
	sql_schema *s = mvc_bind_schema(m, sname);
	sql_table *t = mvc_bind_table(m, s, tname);
	sql_column *c = mvc_bind_column(m, t, cname);
	sql_bpm *p = c->data;

	if (p->nr <= part) {
		return NULL;
	} else {
		if (access == RD_UPD){
			assert(p->parts[part].ubid);
			return temp_descriptor(p->parts[part].ubid);
		}
		assert(p->parts[part].bid);
		return temp_descriptor(p->parts[part].bid);
	}
}

/* str mat_bind_wrap(int *bid, str *sname, str *tname, str *cname, int *access, int *part); */
str
mat_bind_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL;
	mvc *m = NULL;
	str msg = getContext(cntxt, mb, &m, NULL);
	int *bid = (int *)getArgReference(stk, pci, 0);
	str *sname = (str *)getArgReference(stk, pci, 1);
	str *tname = (str *)getArgReference(stk, pci, 2);
	str *cname = (str *)getArgReference(stk, pci, 3);
	int *access = (int *)getArgReference(stk, pci, 4);
	int *part = (int *)getArgReference(stk, pci, 5);

	if (msg)
		return msg;
	b = mat_bind(m, *sname, *tname, *cname, *access, *part);
	if (b) {
		BBPkeepref( *bid = b->batCacheid);
		return MAL_SUCCEED;
	}
	throw(SQL, "mat.bind", "limitation in transaction scope");
}

static BAT *
mat_bind_idxbat(mvc *m, char *sname, char *cname, int access, int part)
{
	sql_schema *s = mvc_bind_schema(m, sname);
	sql_idx *i = mvc_bind_idx(m, s, cname);
	sql_bpm *p = i->data;

	if (p->nr <= part) {
		return NULL;
	} else {
		if (access == RD_UPD)
			return temp_descriptor(p->parts[part].ubid);
		return temp_descriptor(p->parts[part].bid);
	}
}

/* str mat_bind_idxbat_wrap(int *bid, str *sname, str *tname, str *cname, int *access, int *part); */
str
mat_bind_idxbat_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL;
	mvc *m = NULL;
	str msg = getContext(cntxt, mb, &m, NULL);
	int *bid = (int *)getArgReference(stk, pci, 0);
	str *sname = (str *)getArgReference(stk, pci, 1);
	str *tname = (str *)getArgReference(stk, pci, 2);
	str *cname = (str *)getArgReference(stk, pci, 3);
	int *access = (int *)getArgReference(stk, pci, 4);
	int *part = (int *)getArgReference(stk, pci, 5);

	if (msg)
		return msg;
	(void)tname;
	b = mat_bind_idxbat(m, *sname, *cname, *access, *part);
	if (b) {
		BBPkeepref( *bid = b->batCacheid);
		return MAL_SUCCEED;
	}
	throw(SQL, "mat.bind", "limitation in transaction scope");
}

/* str Cparts(int *d, str *sname, str *tname, str *cname, int *access, int *parts); */
str
Cparts(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getContext(cntxt, mb, &m, NULL);
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	sql_bpm *p;
	int err = 0;
	int *d = (int *)getArgReference(stk, pci, 0);
	str *sname = (str *)getArgReference(stk, pci, 1);
	str *tname = (str *)getArgReference(stk, pci, 2);
	str *cname = (str *)getArgReference(stk, pci, 3);
	int *parts = (int *)getArgReference(stk, pci, 5);

	(void)d;
	if (msg)
		return msg;

	s = mvc_bind_schema(m, *sname);
 	t = mvc_bind_table(m, s, *tname);
 	c = mvc_bind_column(m, t, *cname);
	if (c) {
		p = c->data;
	} else {
		sql_idx *i = mvc_bind_idx(m, s, *cname);
		p = i->data;
	}
	err = *parts != (p->nr);
	if (!err)
		return MAL_SUCCEED;
	throw(OPTIMIZER, "mal.assert", "parts changed");
}

str
mat_dummy(void)
{
	throw(OPTIMIZER, "mat.dummy", "not optimized away");
}

static void
inc_parts(sql_table *t)
{
	node *n;

	if (isTempTable(t) || isView(t))
		return ;
	for (n=t->columns.set->h; n; n = n->next) {
		/*sql_column *c = n->data;
		if (c->data && !bpm_add_partition(c->data))
			return;*/
	}
	if (t->idxs.set) for (n=t->idxs.set->h; n; n = n->next) {
		/*sql_idx *i = n->data;
		if (i->data && !bpm_add_partition(i->data))
			return;*/
	}
}

/* str inc_parts_wrap(int *d, str *sname, str *tname); */
str
inc_parts_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getContext(cntxt, mb, &m, NULL);
	sql_schema *s;
	sql_table *t;
	str *sname = (str *)getArgReference(stk, pci, 1);
	str *tname = (str *)getArgReference(stk, pci, 2);

	if (msg)
		return msg;

	s = mvc_bind_schema(m, *sname);
	if (active_store_type != store_bpm)
		throw(SQL, "mat.inc_parts",
			"currently no bpm store support available");
	if (!s)
		throw(SQL, "mat.inc_parts", "%s",
			sql_message("schema %s unknown", *sname));
 	t = mvc_bind_table(m, s, *tname);
	if (!t)
		throw(SQL, "mat.inc_parts", "%s",
			sql_message("table %s unknown", *tname));
	inc_parts(t);
	return MAL_SUCCEED;
}

str
send_part(sql_column *col, int part, BAT *b, str *conn)
{
/*	str tmp = NULL, ident = "romulo";
	connection c;
	sql_bpm *bpm = col->data;*/

	(void)col;
	(void)part;
	(void)b;
	(void)conn;
	/* lookup conn */
/*
	if ((tmp= RMTfindconn(&c, *conn)) != MAL_SUCCEED)
		throw(SQL, "mat.send_part", "%s", tmp);
*/

	/*send the fragment to the remote site*/
/*
	if ((tmp = RMTinternalput(&ident, c->mconn, newBatType(b->H->type, b->T->type), &b->batCacheid)) != MAL_SUCCEED)
		throw(SQL, "mat.send_part", "%s", tmp);

*/
	/*store the information about the fragment location*/
	/*if (!bpm_set_part_location(bpm, part, *conn, ident))
		throw(SQL, "mat.send_part", "The storage of the remote location information failed!!");*/
	return MAL_SUCCEED;
}

/* str send_part_wrap(str *host, str *sname, str *tname, str *col, int part); */
str
send_part_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	BAT *b = NULL;
	str msg = getContext(cntxt, mb, &m, NULL);
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	str *host  = (str *)getArgReference(stk, pci, 1);
	str *sname = (str *)getArgReference(stk, pci, 2);
	str *tname = (str *)getArgReference(stk, pci, 3);
	str *cname = (str *)getArgReference(stk, pci, 4);
	int *part = (int *)getArgReference(stk, pci, 5);
	int access = 0;

	if (msg)
		return msg;

	s = mvc_bind_schema(m, *sname);
	if (!s)
		throw(SQL, "mat.send_part", "%s",
			sql_message("schema %s unknown", *sname));
 	t = mvc_bind_table(m, s, *tname);
	if (!t)
		throw(SQL, "mat.send_part", "%s",
			sql_message("table %s unknown", *tname));
	c = mvc_bind_column(m, t, *cname);
	if (!c)
		throw(SQL, "mat.send_part", "%s",
			sql_message("column %s unknown", *cname));

	b = mat_bind(m, *sname, *tname, *cname, access, *part);
	if (!b)
		throw(SQL, "mat.send_part", "%s",
				sql_message("table %s with column %s does not contain part %d", *tname, *cname, *part));

	send_part(c, *part, b, host);
	return MAL_SUCCEED;
}

str
get_part(sql_column *col, int part, ptr b)
{
/*	str tmp, host = NULL, remote = NULL;
	connection c;
	sql_bpm *bpm = col->data;*/

	(void)col;
	/*bpm_get_part_location(bpm, part, &host, &remote);*/

	/* lookup conn */
/*
	if ((tmp = RMTfindconn(&c, host)) != MAL_SUCCEED)
		throw(SQL, "mat.get_part", "%s", tmp);
*/

	/*get a fragment from the remote site */
/*
	if ((tmp = RMTinternalget(c->mconn, remote, b)) != MAL_SUCCEED)
		throw(SQL, "mat.get_part", "%s", tmp);
*/

	if(!b)
		throw(SQL, "mat.get_part", "%s", sql_message("Get remote part %d for column %s", part, col->base.name));
	return MAL_SUCCEED;
}

/* str get_part_wrap(int *bid, str *sname, str *tname, str *col, int part); */
str
get_part_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getContext(cntxt, mb, &m, NULL);
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	str *sname = (str *)getArgReference(stk, pci, 1);
	str *tname = (str *)getArgReference(stk, pci, 2);
	str *cname = (str *)getArgReference(stk, pci, 3);
	int *part = (int *)getArgReference(stk, pci, 4);

	if (msg)
		return msg;

	s = mvc_bind_schema(m, *sname);
	if (!s)
		throw(SQL, "mat.send_part", "%s",
			sql_message("schema %s unknown", *sname));
 	t = mvc_bind_table(m, s, *tname);
	if (!t)
		throw(SQL, "mat.send_part", "%s",
			sql_message("table %s unknown", *tname));
	c = mvc_bind_column(m, t, *cname);
	if (!c)
		throw(SQL, "mat.send_part", "%s",
			sql_message("column %s unknown", *cname));
	get_part(c, *part,  &stk->stk[getArg(pci, 0)]);
	if (&stk->stk[getArg(pci, 0)])
		return MAL_SUCCEED;
	throw(SQL, "mat.get_part", "limitation in transaction scope");
}

/* str print_part_wrap(int *bid, str *sname, str *tname, str *col, int part); */
str
print_part_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	ValPtr val = NULL;
	str msg = getContext(cntxt, mb, &m, NULL);
	sql_schema *s;
	sql_table *t;
	sql_column *c;
	str *sname = (str *)getArgReference(stk, pci, 1);
	str *tname = (str *)getArgReference(stk, pci, 2);
	str *cname = (str *)getArgReference(stk, pci, 3);
	int *part = (int *)getArgReference(stk, pci, 4);

	if (msg)
		return msg;

	s = mvc_bind_schema(m, *sname);
	if (!s)
		throw(SQL, "mat.send_part", "%s",
			sql_message("schema %s unknown", *sname));
 	t = mvc_bind_table(m, s, *tname);
	if (!t)
		throw(SQL, "mat.send_part", "%s",
			sql_message("table %s unknown", *tname));
	c = mvc_bind_column(m, t, *cname);
	if (!c)
		throw(SQL, "mat.send_part", "%s",
			sql_message("column %s unknown", *cname));

	get_part(c, *part,  &val);
	if (val) {
		VALinit(val, newBatType(TYPE_oid, TYPE_any), NULL);
		/*BATprint(b);*/
		return MAL_SUCCEED;
	}
	throw(SQL, "mat.get_part", "limitation in transaction scope");
}

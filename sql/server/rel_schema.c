/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_trans.h"
#include "rel_rel.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "rel_exp.h"
#include "rel_schema.h"
#include "rel_remote.h"
#include "rel_psm.h"
#include "sql_parser.h"
#include "sql_privileges.h"

#define qname_index(qname) qname_table(qname)
#define qname_func(qname) qname_table(qname)
#define qname_type(qname) qname_table(qname)

static sql_table *
_bind_table(sql_table *t, sql_schema *ss, sql_schema *s, char *name)
{
	sql_table *tt = NULL;

	if (t && strcmp(t->base.name, name) == 0)
		tt = t;
	if (!tt && ss) 
		tt = find_sql_table(ss, name);
	if (!tt && s) 
		tt = find_sql_table(s, name);
	return tt;
}

static sql_rel *
rel_table(mvc *sql, int cat_type, const char *sname, sql_table *t, int nr)
{
	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);

	append(exps, exp_atom_int(sql->sa, nr));
	append(exps, exp_atom_str(sql->sa, sname, sql_bind_localtype("str") ));
	append(exps, exp_atom_ptr(sql->sa, t));
	rel->l = rel_basetable(sql, t, t->base.name);
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = cat_type;
	rel->exps = exps;
	rel->card = CARD_MULTI;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_alter_table(sql_allocator *sa, int cattype, char *sname, char *tname, char *sname2, char *tname2, int action)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	append(exps, exp_atom_clob(sa, sname));
	append(exps, exp_atom_clob(sa, tname));
	assert((sname2 && tname2) || (!sname2 && !tname2));
	if (sname2) {
		append(exps, exp_atom_clob(sa, sname2));
		append(exps, exp_atom_clob(sa, tname2));
	}
	append(exps, exp_atom_int(sa, action));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = cattype;
	rel->exps = exps;
	rel->card = CARD_MULTI;
	rel->nrcols = 0;
	return rel;
}

sql_rel *
rel_list(sql_allocator *sa, sql_rel *l, sql_rel *r) 
{
	sql_rel *rel = rel_create(sa);

	if (!l)
		return r;
	rel->l = l;
	rel->r = r;
	rel->op = op_ddl;
	rel->flag = DDL_LIST;
	return rel;
}

static sql_rel *
view_rename_columns( mvc *sql, char *name, sql_rel *sq, dlist *column_spec)
{
	dnode *n = column_spec->h;
	node *m = sq->exps->h;
	list *l = new_exp_list(sql->sa);

	for (; n && m; n = n->next, m = m->next) {
		char *cname = n->data.sval;
		sql_exp *e = m->data;
		sql_exp *n;
	       
		if (!exp_is_atom(e) && !e->name)
			exp_setname(sql->sa, e, NULL, cname);
		n = exp_is_atom(e)?e:exp_column(sql->sa, exp_relname(e), e->name, exp_subtype(e), sq->card, has_nil(e), is_intern(e));

		exp_setname(sql->sa, n, NULL, cname);
		list_append(l, n);
	}
	/* skip any intern columns */
	for (; m; m = m->next) {
		sql_exp *e = m->data;
		if (!is_intern(e))
			break;
	}
	if (n || m) 
		return sql_error(sql, 02, "M0M03!Column lists do not match");
	(void)name;
	sq = rel_project(sql->sa, sq, l);
	set_processed(sq);
	return sq;
}

static int
as_subquery( mvc *sql, sql_table *t, sql_rel *sq, dlist *column_spec, const char *msg )
{
        sql_rel *r = sq;

	if (!r)
		return 0;

        if (is_topn(r->op) || is_sample(r->op))
                r = sq->l;

	if (column_spec) {
		dnode *n = column_spec->h;
		node *m = r->exps->h;

		for (; n && m; n = n->next, m = m->next) {
			char *cname = n->data.sval;
			sql_exp *e = m->data;
			sql_subtype *tp = exp_subtype(e);

			if (mvc_bind_column(sql, t, cname)) {
				sql_error(sql, 01, "42S21!%s: duplicate column name %s", msg, cname);
				return -1;
			}
			mvc_create_column(sql, t, cname, tp);
		}
		if (n || m) {
			sql_error(sql, 01, "21S02!%s: number of columns does not match", msg);
			return -1;
		}
	} else {
		node *m;

		for (m = r->exps->h; m; m = m->next) {
			sql_exp *e = m->data;
			const char *cname = exp_name(e);
			sql_subtype *tp = exp_subtype(e);

			if (!cname)
				cname = "v";
			if (mvc_bind_column(sql, t, cname)) {
				sql_error(sql, 01, "42S21!%s: duplicate column name %s", msg, cname);
				return -1;
			}
			mvc_create_column(sql, t, cname, tp);
		}
	}
	return 0;
}

sql_table *
mvc_create_table_as_subquery( mvc *sql, sql_rel *sq, sql_schema *s, const char *tname, dlist *column_spec, int temp, int commit_action )
{
	int tt =(temp == SQL_REMOTE)?tt_remote:
		(temp == SQL_STREAM)?tt_stream:
	        (temp == SQL_MERGE_TABLE)?tt_merge_table:
	        (temp == SQL_REPLICA_TABLE)?tt_replica_table:tt_table;

	sql_table *t = mvc_create_table(sql, s, tname, tt, 0, SQL_DECLARED_TABLE, commit_action, -1);
	if (as_subquery( sql, t, sq, column_spec, "CREATE TABLE") != 0)

		return NULL;
	return t;
}

static char *
table_constraint_name(symbol *s, sql_table *t)
{
	/* create a descriptive name like table_col_pkey */
	char *suffix;		/* stores the type of this constraint */
	dnode *nms = NULL;
	char *buf;
	size_t buflen;
	size_t len;
	size_t slen;

	switch (s->token) {
		case SQL_UNIQUE:
			suffix = "_unique";
			nms = s->data.lval->h;	/* list of columns */
			break;
		case SQL_PRIMARY_KEY:
			suffix = "_pkey";
			nms = s->data.lval->h;	/* list of columns */
			break;
		case SQL_FOREIGN_KEY:
			suffix = "_fkey";
			nms = s->data.lval->h->next->data.lval->h;	/* list of colums */
			break;
		default:
			suffix = "_?";
			nms = NULL;
	}

	/* copy table name */
	len = strlen(t->base.name);
	buflen = BUFSIZ;
	slen = strlen(suffix);
	while (len + slen >= buflen)
		buflen += BUFSIZ;
	buf = malloc(buflen);
	strcpy(buf, t->base.name);

	/* add column name(s) */
	for (; nms; nms = nms->next) {
		slen = strlen(nms->data.sval);
		while (len + slen + 1 >= buflen) {
			buflen += BUFSIZ;
			buf = realloc(buf, buflen);
		}
		snprintf(buf + len, buflen - len, "_%s", nms->data.sval);
		len += slen + 1;
	}

	/* add suffix */
	slen = strlen(suffix);
	while (len + slen >= buflen) {
		buflen += BUFSIZ;
		buf = realloc(buf, buflen);
	}
	snprintf(buf + len, buflen - len, "%s", suffix);

	return buf;
}

static char *
column_constraint_name(symbol *s, sql_column *sc, sql_table *t)
{
	/* create a descriptive name like table_col_pkey */
	char *suffix;		/* stores the type of this constraint */
	static char buf[BUFSIZ];

	switch (s->token) {
		case SQL_UNIQUE:
			suffix = "unique";
			break;
		case SQL_PRIMARY_KEY:
			suffix = "pkey";
			break;
		case SQL_FOREIGN_KEY:
			suffix = "fkey";
			break;
		default:
			suffix = "?";
	}

	snprintf(buf, BUFSIZ, "%s_%s_%s", t->base.name, sc->base.name, suffix);

	return buf;
}

static int
column_constraint_type(mvc *sql, char *name, symbol *s, sql_schema *ss, sql_table *t, sql_column *cs)
{
	int res = SQL_ERR;

	if (!ss && (s->token != SQL_NULL && s->token != SQL_NOT_NULL)) {
		(void) sql_error(sql, 02, "42000!CONSTRAINT: constraints on declared tables are not supported\n");
		return res;
	}
	switch (s->token) {
	case SQL_UNIQUE:
	case SQL_PRIMARY_KEY: {
		key_type kt = (s->token == SQL_UNIQUE) ? ukey : pkey;
		sql_key *k;

		if (kt == pkey && t->pkey) {
			(void) sql_error(sql, 02, "42000!CONSTRAINT PRIMARY KEY: a table can have only one PRIMARY KEY\n");
			return res;
		}
		if (name && mvc_bind_key(sql, ss, name)) {
			(void) sql_error(sql, 02, "42000!CONSTRAINT PRIMARY KEY: key %s already exists", name);
			return res;
		}
		k = (sql_key*)mvc_create_ukey(sql, t, name, kt);

		mvc_create_kc(sql, k, cs);
		mvc_create_ukey_done(sql, k);
		res = SQL_OK;
	} 	break;
	case SQL_FOREIGN_KEY: {
		dnode *n = s->data.lval->h;
		char *rsname = qname_schema(n->data.lval);
		char *rtname = qname_table(n->data.lval);
		int ref_actions = n->next->next->next->data.i_val; 
		sql_schema *rs;
		sql_table *rt;
		sql_fkey *fk;
		list *cols;
		sql_key *rk = NULL;

		assert(n->next->next->next->type == type_int);
/*
		if (isTempTable(t)) {
			(void) sql_error(sql, 02, "42000!CONSTRAINT: constraints on temporary tables are not supported\n");
			return res;
		}
*/

		if (rsname) 
			rs = mvc_bind_schema(sql, rsname);
		else 
			rs = cur_schema(sql);
		rt = _bind_table(t, ss, rs, rtname);
		if (!rt) {
			(void) sql_error(sql, 02, "42S02!CONSTRAINT FOREIGN KEY: no such table '%s'\n", rtname);
			return res;
		}
		if (name && mvc_bind_key(sql, ss, name)) {
			(void) sql_error(sql, 02, "42000!CONSTRAINT FOREIGN KEY: key '%s' already exists", name);
			return res;
		}

		/* find unique referenced key */
		if (n->next->data.lval) {	
			char *rcname = n->next->data.lval->h->data.sval;

			cols = list_append(sa_list(sql->sa), rcname);
			rk = mvc_bind_ukey(rt, cols);
		} else if (rt->pkey) {
			/* no columns specified use rt.pkey */
			rk = &rt->pkey->k;
		}
		if (!rk) {
			(void) sql_error(sql, 02, "42000!CONSTRAINT FOREIGN KEY: could not find referenced PRIMARY KEY in table %s.%s\n", rsname, rtname);
			return res;
		}
		fk = mvc_create_fkey(sql, t, name, fkey, rk, ref_actions & 255, (ref_actions>>8) & 255);
		mvc_create_fkc(sql, fk, cs);
		res = SQL_OK;
	} 	break;
	case SQL_NOT_NULL:
	case SQL_NULL: {
		int null = (s->token != SQL_NOT_NULL);

		mvc_null(sql, cs, null);
		res = SQL_OK;
	} 	break;
	}
	if (res == SQL_ERR) {
		(void) sql_error(sql, 02, "M0M03!unknown constraint (" PTRFMT ")->token = %s\n", PTRFMTCAST s, token2string(s->token));
	}
	return res;
}

static int
column_option(
		mvc *sql,
		symbol *s,
		sql_schema *ss,
		sql_table *t,
		sql_column *cs)
{
	int res = SQL_ERR;

	assert(cs);
	switch (s->token) {
	case SQL_CONSTRAINT: {
		dlist *l = s->data.lval;
		char *opt_name = l->h->data.sval;
		symbol *sym = l->h->next->data.sym;

		if (!sym) /* For now we only parse CHECK Constraints */
			return SQL_OK;
		if (!opt_name)
			opt_name = column_constraint_name(sym, cs, t);
		res = column_constraint_type(sql, opt_name, sym, ss, t, cs);
	} 	break;
	case SQL_DEFAULT: {
		symbol *sym = s->data.sym;
		char *err = NULL, *r;

		if (sym->token == SQL_COLUMN) {
			sql_exp *e = rel_logical_value_exp(sql, NULL, sym, sql_sel);
			
			if (e && is_atom(e->type)) {
				atom *a = exp_value(e, sql->args, sql->argc);

				if (atom_null(a)) {
					mvc_default(sql, cs, NULL);
					res = SQL_OK;
					break;
				}
			}
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';
		}
	       	r = symbol2string(sql, s->data.sym, &err);
		if (!r) {
			(void) sql_error(sql, 02, "42000!incorrect default value '%s'\n", err?err:"");
			if (err) _DELETE(err);
			return SQL_ERR;
		} else {
			mvc_default(sql, cs, r);
			_DELETE(r);
			res = SQL_OK;
		}
	} 	break;
	case SQL_ATOM: {
		AtomNode *an = (AtomNode *) s;

		assert(0);
		if (!an || !an->a) {
			mvc_default(sql, cs, NULL);
		} else {
			atom *a = an->a;

			if (a->data.vtype == TYPE_str) {
				mvc_default(sql, cs, a->data.val.sval);
			} else {
				char *r = atom2string(sql->sa, a);

				mvc_default(sql, cs, r);
			}
		}
		res = SQL_OK;
	} 	break;
	case SQL_NOT_NULL:
	case SQL_NULL: {
		int null = (s->token != SQL_NOT_NULL);

		mvc_null(sql, cs, null);
		res = SQL_OK;
	} 	break;
	}
	if (res == SQL_ERR) {
		(void) sql_error(sql, 02, "M0M03!unknown column option (" PTRFMT ")->token = %s\n", PTRFMTCAST s, token2string(s->token));
	}
	return res;
}

static int
column_options(mvc *sql, dlist *opt_list, sql_schema *ss, sql_table *t, sql_column *cs)
{
	assert(cs);

	if (opt_list) {
		dnode *n = NULL;

		for (n = opt_list->h; n; n = n->next) {
			int res = column_option(sql, n->data.sym, ss, t, cs);

			if (res == SQL_ERR)
				return SQL_ERR;
		}
	}
	return SQL_OK;
}

static int
table_foreign_key(mvc *sql, char *name, symbol *s, sql_schema *ss, sql_table *t)
{
	dnode *n = s->data.lval->h;
	char *rsname = qname_schema(n->data.lval);
	char *rtname = qname_table(n->data.lval);
	sql_schema *fs;
	sql_table *ft;

	if (rsname)
		fs = mvc_bind_schema(sql, rsname);
	else
		fs = ss;
	ft = mvc_bind_table(sql, fs, rtname);
	/* self referenced table */
	if (!ft && t->s == fs && strcmp(t->base.name, rtname) == 0)
		ft = t;
	if (!ft) {
		sql_error(sql, 02, "42S02!CONSTRAINT FOREIGN KEY: no such table '%s'\n", rtname);
		return SQL_ERR;
	} else if (list_find_name(t->keys.set, name)) {
		sql_error(sql, 02, "42000!CONSTRAINT FOREIGN KEY: key '%s' already exists", name);
		return SQL_ERR;
	} else {
		sql_key *rk = NULL;
		sql_fkey *fk;
		dnode *nms = n->next->data.lval->h;
		node *fnms;
		int ref_actions = n->next->next->next->next->data.i_val;

		assert(n->next->next->next->next->type == type_int);
		if (name && mvc_bind_key(sql, ss, name)) {
			sql_error(sql, 02, "42000!Create Key failed, key '%s' already exists", name);
			return SQL_ERR;
		}
		if (n->next->next->data.lval) {	/* find unique referenced key */
			dnode *rnms = n->next->next->data.lval->h;
			list *cols = sa_list(sql->sa);

			for (; rnms; rnms = rnms->next)
				list_append(cols, rnms->data.sval);

			/* find key in ft->keys */
			rk = mvc_bind_ukey(ft, cols);
		} else if (ft->pkey) {	
			/* no columns specified use ft.pkey */
			rk = &ft->pkey->k;
		}
		if (!rk) {
			sql_error(sql, 02, "42000!CONSTRAINT FOREIGN KEY: could not find referenced PRIMARY KEY in table '%s'\n", ft->base.name);
			return SQL_ERR;
		}
		fk = mvc_create_fkey(sql, t, name, fkey, rk, ref_actions & 255, (ref_actions>>8) & 255);

		for (fnms = rk->columns->h; nms && fnms; nms = nms->next, fnms = fnms->next) {
			char *nm = nms->data.sval;
			sql_column *c = mvc_bind_column(sql, t, nm);

			if (!c) {
				sql_error(sql, 02, "42S22!CONSTRAINT FOREIGN KEY: no such column '%s' in table '%s'\n", nm, t->base.name);
				return SQL_ERR;
			}
			mvc_create_fkc(sql, fk, c);
		}
		if (nms || fnms) {
			sql_error(sql, 02, "42000!CONSTRAINT FOREIGN KEY: not all columns are handled\n");
			return SQL_ERR;
		}
	}
	return SQL_OK;
}

static int
table_constraint_type(mvc *sql, char *name, symbol *s, sql_schema *ss, sql_table *t)
{
	int res = SQL_OK;

	switch (s->token) {
	case SQL_UNIQUE:
	case SQL_PRIMARY_KEY: {
		key_type kt = (s->token == SQL_PRIMARY_KEY ? pkey : ukey);
		dnode *nms = s->data.lval->h;
		sql_key *k;

		if (kt == pkey && t->pkey) {
			sql_error(sql, 02, "42000!CONSTRAINT PRIMARY KEY: a table can have only one PRIMARY KEY\n");
			return SQL_ERR;
		}
		if (name && mvc_bind_key(sql, ss, name)) {
			sql_error(sql, 02, "42000!CONSTRAINT %s: key '%s' already exists",
					kt == pkey ? "PRIMARY KEY" : "UNIQUE", name);
			return SQL_ERR;
		}
			
 		k = (sql_key*)mvc_create_ukey(sql, t, name, kt);
		for (; nms; nms = nms->next) {
			char *nm = nms->data.sval;
			sql_column *c = mvc_bind_column(sql, t, nm);

			if (!c) {
				sql_error(sql, 02, "42S22!CONSTRAINT %s: no such column '%s' for table '%s'",
						kt == pkey ? "PRIMARY KEY" : "UNIQUE",
						nm, t->base.name);
				return SQL_ERR;
			} 
			(void) mvc_create_kc(sql, k, c);
		}
		mvc_create_ukey_done(sql, k);
	} 	break;
	case SQL_FOREIGN_KEY:
		res = table_foreign_key(sql, name, s, ss, t);
		break;
	}
	if (res != SQL_OK) {
		sql_error(sql, 02, "M0M03!table constraint type: wrong token (" PTRFMT ") = %s\n", PTRFMTCAST s, token2string(s->token));
		return SQL_ERR;
	}
	return res;
}

static int
table_constraint(mvc *sql, symbol *s, sql_schema *ss, sql_table *t)
{
	int res = SQL_OK;

	if (s->token == SQL_CONSTRAINT) {
		dlist *l = s->data.lval;
		char *opt_name = l->h->data.sval;
		symbol *sym = l->h->next->data.sym;

		if (!opt_name)
			opt_name = table_constraint_name(sym, t);
		res = table_constraint_type(sql, opt_name, sym, ss, t);
		if (opt_name != l->h->data.sval)
			free(opt_name);
	}

	if (res != SQL_OK) {
		sql_error(sql, 02, "M0M03!table constraint: wrong token (" PTRFMT ") = %s\n", PTRFMTCAST s, token2string(s->token));
		return SQL_ERR;
	}
	return res;
}

static int
create_column(mvc *sql, symbol *s, sql_schema *ss, sql_table *t, int alter)
{
	dlist *l = s->data.lval;
	char *cname = l->h->data.sval;
	sql_subtype *ctype = &l->h->next->data.typeval;
	dlist *opt_list = NULL;
	int res = SQL_OK;

(void)ss;
	if (alter && !isTable(t)) {
		sql_error(sql, 02, "42000!ALTER TABLE: cannot add column to VIEW '%s'\n", t->base.name);
		return SQL_ERR;
	}
	if (l->h->next->next)
		opt_list = l->h->next->next->data.lval;

	if (cname && ctype) {
		sql_column *cs = NULL;

		cs = find_sql_column(t, cname);
		if (cs) {
			sql_error(sql, 02, "42S21!%s TABLE: a column named '%s' already exists\n", (alter)?"ALTER":"CREATE", cname);
			return SQL_ERR;
		}
		cs = mvc_create_column(sql, t, cname, ctype);
		if (column_options(sql, opt_list, ss, t, cs) == SQL_ERR)
			return SQL_ERR;
	}
	return res;
}

static int
table_element(mvc *sql, symbol *s, sql_schema *ss, sql_table *t, int alter)
{
	int res = SQL_OK;

	if (alter && (isView(t) || ((isMergeTable(t) || isReplicaTable(t)) && (s->token != SQL_TABLE && s->token != SQL_DROP_TABLE && cs_size(&t->tables)>0)) || (isTable(t) && (s->token == SQL_TABLE || s->token == SQL_DROP_TABLE)) )){
		char *msg = "";

		switch (s->token) {
		case SQL_TABLE: 	
			msg = "add table to"; 
			break;
		case SQL_COLUMN: 	
			msg = "add column to"; 
			break;
		case SQL_CONSTRAINT: 	
			msg = "add constraint to"; 
			break;
		case SQL_COLUMN_OPTIONS:
		case SQL_DEFAULT:
		case SQL_NOT_NULL:
		case SQL_NULL:
			msg = "set column options for"; 
			break;
		case SQL_STORAGE:
			msg = "set column storage for"; 
			break;
		case SQL_DROP_DEFAULT:
			msg = "drop default column option from"; 
			break;
		case SQL_DROP_TABLE:
			msg = "drop table from"; 
			break;
		case SQL_DROP_COLUMN:
			msg = "drop column from"; 
			break;
		case SQL_DROP_CONSTRAINT:
			msg = "drop constraint from"; 
			break;
		}
		sql_error(sql, 02, "42000!ALTER TABLE: cannot %s %s '%s'\n",
				msg, 
				isMergeTable(t)?"MERGE TABLE":
				isReplicaTable(t)?"REPLICA TABLE":"VIEW",
				t->base.name);
		return SQL_ERR;
	}

	switch (s->token) {
	case SQL_COLUMN:
		res = create_column(sql, s, ss, t, alter);
		break;
	case SQL_CONSTRAINT:
		res = table_constraint(sql, s, ss, t);
		break;
	case SQL_COLUMN_OPTIONS:
	{
		dnode *n = s->data.lval->h;
		char *cname = n->data.sval;
		sql_column *c = mvc_bind_column(sql, t, cname);
		dlist *olist = n->next->data.lval;

		if (!c) {
			sql_error(sql, 02, "42S22!ALTER TABLE: no such column '%s'\n", cname);
			return SQL_ERR;
		} else {
			return column_options(sql, olist, ss, t, c);
		}
	} 	break;
	case SQL_DEFAULT:
	{
		char *r, *err = NULL;
		dlist *l = s->data.lval;
		char *cname = l->h->data.sval;
		symbol *sym = l->h->next->data.sym;
		sql_column *c = mvc_bind_column(sql, t, cname);

		if (!c) {
			sql_error(sql, 02, "42S22!ALTER TABLE: no such column '%s'\n", cname);
			return SQL_ERR;
		}
		r = symbol2string(sql, sym, &err);
		if (!r) {
			(void) sql_error(sql, 02, "42000!incorrect default value '%s'\n", err?err:"");
			if (err) _DELETE(err);
			return SQL_ERR;
		}
		mvc_default(sql, c, r);
		_DELETE(r);
	}
	break;
	case SQL_STORAGE:
	{
		dlist *l = s->data.lval;
		char *cname = l->h->data.sval;
		char *storage_type = l->h->next->data.sval;
		sql_column *c = mvc_bind_column(sql, t, cname);

		if (!c) {
			sql_error(sql, 02, "42S22!ALTER TABLE: no such column '%s'\n", cname);
			return SQL_ERR;
		}
		mvc_storage(sql, c, storage_type);
	}
	break;
	case SQL_NOT_NULL:
	case SQL_NULL:
	{
		dnode *n = s->data.lval->h;
		char *cname = n->data.sval;
		sql_column *c = mvc_bind_column(sql, t, cname);
		int null = (s->token != SQL_NOT_NULL);

		if (!c) {
			sql_error(sql, 02, "42S22!ALTER TABLE: no such column '%s'\n", cname);
			return SQL_ERR;
		}
		mvc_null(sql, c, null);
	} 	break;
	case SQL_DROP_DEFAULT:
	{
		char *cname = s->data.sval;
		sql_column *c = mvc_bind_column(sql, t, cname);
		if (!c) {
			sql_error(sql, 02, "42S22!ALTER TABLE: no such column '%s'\n", cname);
			return SQL_ERR;
		}
		mvc_drop_default(sql,c);
	} 	break;
	case SQL_LIKE:
	{
		char *sname = qname_schema(s->data.lval);
		char *name = qname_table(s->data.lval);
		sql_schema *os = NULL;
		sql_table *ot = NULL;
		node *n;

		if (sname && !(os = mvc_bind_schema(sql, sname))) {
			sql_error(sql, 02, "3F000!CREATE TABLE: no such schema '%s'", sname);
			return SQL_ERR;
		}
		if (!os)
			os = ss;
	       	ot = mvc_bind_table(sql, os, name);
		if (!ot) {
			sql_error(sql, 02, "3F000!CREATE TABLE: no such table '%s'", name);
			return SQL_ERR;
		}
		for (n = ot->columns.set->h; n; n = n->next) {
			sql_column *oc = n->data;

			(void)mvc_create_column(sql, t, oc->base.name, &oc->type);
		}
	} 	break;
	case SQL_DROP_COLUMN:
	{
		dlist *l = s->data.lval;
		char *cname = l->h->data.sval;
		int drop_action = l->h->next->data.i_val;
		sql_column *col = mvc_bind_column(sql, t, cname);

		assert(l->h->next->type == type_int);
		if (col == NULL) {
			sql_error(sql, 02, "42S22!ALTER TABLE: no such column '%s'\n", cname);
			return SQL_ERR;
		}
		if (cs_size(&t->columns) <= 1) {
			sql_error(sql, 02, "42000!ALTER TABLE: cannot drop column '%s': table needs at least one column\n", cname);
			return SQL_ERR;
		}
		if (t->system) {
			sql_error(sql, 02, "42000!ALTER TABLE: cannot drop column '%s': table is a system table\n", cname);
			return SQL_ERR;
		}
		if (isView(t)) {
			sql_error(sql, 02, "42000!ALTER TABLE: cannot drop column '%s': '%s' is a view\n", cname, t->base.name);
			return SQL_ERR;
		}
		if (!drop_action && mvc_check_dependency(sql, col->base.id, COLUMN_DEPENDENCY, NULL)) {
			sql_error(sql, 02, "2BM37!ALTER TABLE: cannot drop column '%s': there are database objects which depend on it\n", cname);
			return SQL_ERR;
		}
		if (!drop_action  && t->keys.set) {
			node *n, *m;

			for (n = t->keys.set->h; n; n = n->next) {
				sql_key *k = n->data;
				for (m = k->columns->h; m; m = m->next) {
					sql_kc *kc = m->data;
					if (strcmp(kc->c->base.name, cname) == 0) {
						sql_error(sql, 02, "2BM37!ALTER TABLE: cannot drop column '%s': there are constraints which depend on it\n", cname);
						return SQL_ERR;
					}
				}
			}
		}
		mvc_drop_column(sql, t, col, drop_action);
	} 	break;
	case SQL_DROP_CONSTRAINT:
		assert(0);
	}
	if (res == SQL_ERR) {
		sql_error(sql, 02, "M0M03!unknown table element (" PTRFMT ")->token = %s\n", PTRFMTCAST s, token2string(s->token));
		return SQL_ERR;
	}
	return res;
}

sql_rel *
rel_create_table(mvc *sql, sql_schema *ss, int temp, const char *sname, const char *name, symbol *table_elements_or_subquery, int commit_action, const char *loc)
{
	sql_schema *s = NULL;

	int instantiate = (sql->emode == m_instantiate);
	int deps = (sql->emode == m_deps);
	int create = (!instantiate && !deps);
	int tt = (temp == SQL_REMOTE)?tt_remote:
		 (temp == SQL_STREAM)?tt_stream:
	         (temp == SQL_MERGE_TABLE)?tt_merge_table:
	         (temp == SQL_REPLICA_TABLE)?tt_replica_table:tt_table;

	(void)create;
	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, "3F000!CREATE TABLE: no such schema '%s'", sname);

	if (temp != SQL_PERSIST && tt == tt_table && 
			commit_action == CA_COMMIT)
		commit_action = CA_DELETE;
	
	if (temp != SQL_DECLARED_TABLE) {
		if (temp != SQL_PERSIST && tt == tt_table) {
			s = mvc_bind_schema(sql, "tmp");
			if (temp == SQL_LOCAL_TEMP && sname && strcmp(sname, s->base.name) != 0)
				return sql_error(sql, 02, "3F000!CREATE TABLE: local tempory tables should be stored in the '%s' schema", s->base.name);
		} else if (s == NULL) {
			s = ss;
		}
	}

	if (temp != SQL_DECLARED_TABLE && s)
		sname = s->base.name;

	if (mvc_bind_table(sql, s, name)) {
		char *cd = (temp == SQL_DECLARED_TABLE)?"DECLARE":"CREATE";
		return sql_error(sql, 02, "42S01!%s TABLE: name '%s' already in use", cd, name);
	} else if (temp != SQL_DECLARED_TABLE && (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && temp == SQL_LOCAL_TEMP))){
		return sql_error(sql, 02, "42000!CREATE TABLE: insufficient privileges for user '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	} else if (table_elements_or_subquery->token == SQL_CREATE_TABLE) { 
		/* table element list */
		dnode *n;
		dlist *columns = table_elements_or_subquery->data.lval;
		sql_table *t;
	       
		if (tt == tt_remote) {
			if (!mapiuri_valid(loc))
				return sql_error(sql, 02, "42000!CREATE TABLE: incorrect uri '%s' for remote table '%s'", loc, name);
			t = mvc_create_remote(sql, s, name, SQL_DECLARED_TABLE, loc);
		} else {
			t = mvc_create_table(sql, s, name, tt, 0, SQL_DECLARED_TABLE, commit_action, -1);
		}
		if (!t)
			return NULL;

		for (n = columns->h; n; n = n->next) {
			symbol *sym = n->data.sym;
			int res = table_element(sql, sym, s, t, 0);

			if (res == SQL_ERR) 
				return NULL;
		}
		temp = (tt == tt_table)?temp:SQL_PERSIST;
		return rel_table(sql, DDL_CREATE_TABLE, sname, t, temp);
	} else { /* [col name list] as subquery with or without data */
		sql_rel *sq = NULL, *res = NULL;
		dlist *as_sq = table_elements_or_subquery->data.lval;
		dlist *column_spec = as_sq->h->data.lval;
		symbol *subquery = as_sq->h->next->data.sym;
		int with_data = as_sq->h->next->next->data.i_val;
		sql_table *t = NULL; 

		assert(as_sq->h->next->next->type == type_int);
		sq = rel_selects(sql, subquery);
		if (!sq)
			return NULL;

		/* create table */
		if ((t = mvc_create_table_as_subquery( sql, sq, s, name, column_spec, temp, commit_action)) == NULL) { 
			rel_destroy(sq);
			return NULL;
		}

		/* insert query result into this table */
		temp = (tt == tt_table)?temp:SQL_PERSIST;
		res = rel_table(sql, DDL_CREATE_TABLE, sname, t, temp);
		if (with_data) {
			res = rel_insert(sql, res, sq);
		} else {
			rel_destroy(sq);
		}
		return res;
	}
	/*return NULL;*/ /* never reached as all branches of the above if() end with return ... */
}

static sql_rel *
rel_create_view(mvc *sql, sql_schema *ss, dlist *qname, dlist *column_spec, symbol *query, int check, int persistent)
{
	char *name = qname_table(qname);
	char *sname = qname_schema(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL;
	int instantiate = (sql->emode == m_instantiate || !persistent);
	int deps = (sql->emode == m_deps);
	int create = (!instantiate && !deps);

(void)ss;
	(void) check;		/* Stefan: unused!? */
	if (sname && !(s = mvc_bind_schema(sql, sname))) 
		return sql_error(sql, 02, "3F000!CREATE VIEW: no such schema '%s'", sname);
	if (s == NULL)
		s = cur_schema(sql);

	if (create && mvc_bind_table(sql, s, name) != NULL) {
		return sql_error(sql, 02, "42S01!CREATE VIEW: name '%s' already in use", name);
	} else if (create && (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && persistent == SQL_LOCAL_TEMP))) {
		return sql_error(sql, 02, "42000!CREATE VIEW: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	} else if (query) {
		sql_rel *sq = NULL;
		char *q = QUERY(sql->scanner);

		if (query->token == SQL_SELECT) {
			SelectNode *sn = (SelectNode *) query;

			if (sn->limit)
				return sql_error(sql, 01, "0A000!42000!CREATE VIEW: LIMIT not supported");
			if (sn->orderby)
				return sql_error(sql, 01, "42000!CREATE VIEW: ORDER BY not supported");
		}

		sq = schema_selects(sql, s, query);
		if (!sq)
			return NULL;

		if (!create) {
			if (column_spec) {
				dnode *n = column_spec->h;
				node *m = sq->exps->h;

				for (; n && m; n = n->next, m = m->next)
					;
				if (n || m) {
					sql_error(sql, 01, "21S02!WITH CLAUSE: number of columns does not match");
					rel_destroy(sq);
					return NULL;
				}
			}
			rel_add_intern(sql, sq);
		}

		if (create) {
			q = query_cleaned(q);
			t = mvc_create_view(sql, s, name, SQL_DECLARED_TABLE, q, 0);
			GDKfree(q);
			if (as_subquery( sql, t, sq, column_spec, "CREATE VIEW") != 0) {
				rel_destroy(sq);
				return NULL;
			}
			return rel_table(sql, DDL_CREATE_VIEW, s->base.name, t, SQL_PERSIST);
		}
		t = mvc_bind_table(sql, s, name);
		if (!persistent && column_spec) 
			sq = view_rename_columns( sql, name, sq, column_spec);
		if (sq && sq->op == op_project && sq->l && sq->exps && sq->card == CARD_AGGR) {
			exps_setcard(sq->exps, CARD_MULTI);
			sq->card = CARD_MULTI;
		}
		return sq;
	}
	return NULL;
}

static sql_rel *
rel_schema2(sql_allocator *sa, int cat_type, char *sname, char *auth, int nr)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	append(exps, exp_atom_clob(sa, sname));
	append(exps, exp_atom_clob(sa, auth));
	append(exps, exp_atom_int(sa, nr));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = cat_type;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_schema3(sql_allocator *sa, int cat_type, char *sname, char *tname, char *name)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	append(exps, exp_atom_clob(sa, sname));
	append(exps, exp_atom_clob(sa, tname));
	append(exps, exp_atom_clob(sa, name));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = cat_type;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_drop_type(mvc *sql, dlist *qname, int drop_action)
{
	char *name = qname_table(qname);
	char *sname = qname_schema(qname);
	sql_schema *s = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname))) 
		return sql_error(sql, 02, "3F000!DROP TYPE: no such schema '%s'", sname);
	if (s == NULL)
		s = cur_schema(sql);

	if (schema_bind_type(sql, s, name) == NULL) {
		return sql_error(sql, 02, "42S01!DROP TYPE: type '%s' does not exist", name);
	} else if (!mvc_schema_privs(sql, s)) {
		return sql_error(sql, 02, "42000!DROP TYPE: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	}
	return rel_schema2(sql->sa, DDL_DROP_TYPE, s->base.name, name, drop_action);
}

static sql_rel *
rel_create_type(mvc *sql, dlist *qname, char *impl)
{
	char *name = qname_table(qname);
	char *sname = qname_schema(qname);
	sql_schema *s = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname))) 
		return sql_error(sql, 02, "3F000!CREATE TYPE: no such schema '%s'", sname);
	if (s == NULL)
		s = cur_schema(sql);

	if (schema_bind_type(sql, s, name) != NULL) {
		return sql_error(sql, 02, "42S01!CREATE TYPE: name '%s' already in use", name);
	} else if (!mvc_schema_privs(sql, s)) {
		return sql_error(sql, 02, "42000!CREATE TYPE: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	}
	return rel_schema3(sql->sa, DDL_CREATE_TYPE, s->base.name, name, impl);
}
static char *
dlist_get_schema_name(dlist *name_auth)
{
	assert(name_auth && name_auth->h);
	return name_auth->h->data.sval;
}

static char *
schema_auth(dlist *name_auth)
{
	assert(name_auth && name_auth->h && dlist_length(name_auth) == 2);
	return name_auth->h->next->data.sval;
}

static sql_rel *
rel_schema(sql_allocator *sa, int cat_type, char *sname, char *auth, int nr)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	append(exps, exp_atom_int(sa, nr));
	append(exps, exp_atom_clob(sa, sname));
	if (auth)
		append(exps, exp_atom_clob(sa, auth));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = cat_type;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_create_schema(mvc *sql, dlist *auth_name, dlist *schema_elements)
{
	char *name = dlist_get_schema_name(auth_name);
	char *auth = schema_auth(auth_name);
	int auth_id = sql->role_id;

	if (auth && (auth_id = sql_find_auth(sql, auth)) < 0) {
		sql_error(sql, 02, "28000!CREATE SCHEMA: no such authorization '%s'", auth);
		return NULL;
	}
	if (sql->user_id != USER_MONETDB && sql->role_id != ROLE_SYSADMIN) {
		sql_error(sql, 02, "42000!CREATE SCHEMA: insufficient privileges for user '%s'", stack_get_string(sql, "current_user"));
		return NULL;
	}
	if (!name) 
		name = auth;
	assert(name);
	if (mvc_bind_schema(sql, name)) {
		sql_error(sql, 02, "3F000!CREATE SCHEMA: name '%s' already in use", name);
		return NULL;
	} else {
		sql_schema *os = sql->session->schema;
		dnode *n;
		sql_schema *ss = SA_ZNEW(sql->sa, sql_schema);
		sql_rel *ret;

		ret = rel_schema(sql->sa, DDL_CREATE_SCHEMA, name, auth, 0);

		ss->base.name = name;
		ss->auth_id = auth_id;
		ss->owner = sql->user_id;

		sql->session->schema = ss;
		n = schema_elements->h;
		while (n) {
			sql_rel *res = rel_semantic(sql, n->data.sym);
			if (!res) {
				rel_destroy(ret);
				return NULL;
			}
			ret = rel_list(sql->sa, ret, res);
			n = n->next;
		}
		sql->session->schema = os;
		return ret;
	}
}

static str
get_schema_name( mvc *sql, char *sname, char *tname)
{
	if (!sname) {
		sql_schema *ss = cur_schema(sql);
		sql_table *t = mvc_bind_table(sql, ss, tname);
		if (!t)
			ss = tmp_schema(sql);
		sname = ss->base.name;
	}
	return sname;
}

static sql_rel *
sql_alter_table(mvc *sql, dlist *qname, symbol *te)
{
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL;

	if (sname && !(s=mvc_bind_schema(sql, sname))) {
		(void) sql_error(sql, 02, "3F000!ALTER TABLE: no such schema '%s'", sname);
		return NULL;
	}
	if (!s)
		s = cur_schema(sql);

	if ((t = mvc_bind_table(sql, s, tname)) == NULL) {
		if (mvc_bind_table(sql, mvc_bind_schema(sql, "tmp"), tname) != NULL) 
			return sql_error(sql, 02, "42S02!ALTER TABLE: not supported on TEMPORARY table '%s'", tname);
		return sql_error(sql, 02, "42S02!ALTER TABLE: no such table '%s' in schema '%s'", tname, s->base.name);
	} else {
		node *n;
		sql_rel *res = NULL, *r;
		sql_table *nt = NULL;
		sql_exp ** updates, *e;

		assert(te);
		if (t && te && te->token == SQL_DROP_CONSTRAINT) {
			dlist *l = te->data.lval;
			char *kname = l->h->data.sval;
			int drop_action = l->h->next->data.i_val;
			
			sname = get_schema_name(sql, sname, tname);
			return rel_schema(sql->sa, DDL_DROP_CONSTRAINT, sname, kname, drop_action);
		}

		if (t->persistence != SQL_DECLARED_TABLE)
			sname = s->base.name;

		if (te && (te->token == SQL_TABLE || te->token == SQL_DROP_TABLE)) {
			char *ntname = te->data.lval->h->data.sval;

			/* TODO partition sname */
			if (te->token == SQL_TABLE) {
				return rel_alter_table(sql->sa, DDL_ALTER_TABLE_ADD_TABLE, sname, tname, sname, ntname, 0);
			} else {
				int drop_action = te->data.lval->h->next->data.i_val;

				return rel_alter_table(sql->sa, DDL_ALTER_TABLE_DEL_TABLE, sname, tname, sname, ntname, drop_action);
			}
		}

		/* read only or read write */
		if (te && te->token == SQL_ALTER_TABLE) {
			int state = te->data.i_val;

			if (state == tr_readonly) 
				state = TABLE_READONLY;
			else if (state == tr_append) 
				state = TABLE_APPENDONLY;
			else
				state = TABLE_WRITABLE;
			return rel_alter_table(sql->sa, DDL_ALTER_TABLE_SET_ACCESS, sname, tname, NULL, NULL, state);
		}

	       	nt = dup_sql_table(sql->sa, t);
		if (!nt || (te && table_element(sql, te, s, nt, 1) == SQL_ERR)) 
			return NULL;

		if (t->s && !nt->s)
			nt->s = t->s;

		res = rel_table(sql, DDL_ALTER_TABLE, sname, nt, 0);

		if (!isTable(nt))
			return res;

		/* new columns need update with default values */
		updates = table_update_array(sql, nt);
		e = exp_column(sql->sa, nt->base.name, "%TID%", sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
		r = rel_project(sql->sa, res, append(new_exp_list(sql->sa),e));
		if (nt->columns.nelm) {
			list *cols = new_exp_list(sql->sa);
			for (n = nt->columns.nelm; n; n = n->next) {
				sql_column *c = n->data;
				if (c->def) {
					char *d = sql_message("select %s;", c->def);
					e = rel_parse_val(sql, d, sql->emode);
					_DELETE(d);
				} else {
					e = exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL));
				}
				if (!e || (e = rel_check_type(sql, &c->type, e, type_equal)) == NULL) {
					rel_destroy(r);
					return NULL;
				}
				list_append(cols, exp_column(sql->sa, nt->base.name, c->base.name, &c->type, CARD_MULTI, 0, 0));

				assert(!updates[c->colnr]);
				exp_setname(sql->sa, e, c->t->base.name, c->base.name);
				updates[c->colnr] = e;
			}
			res = rel_update(sql, res, r, updates, cols); 
		} else { /* new indices or keys */
			res = rel_update(sql, res, r, updates, NULL); 
		}
		return res;
	}
}

static sql_rel *
rel_role(sql_allocator *sa, char *grantee, char *auth, int grantor, int admin, int type)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	assert(type == DDL_GRANT_ROLES || type == DDL_REVOKE_ROLES);
	append(exps, exp_atom_clob(sa, grantee));
	append(exps, exp_atom_clob(sa, auth));
	append(exps, exp_atom_int(sa, grantor));
	append(exps, exp_atom_int(sa, admin));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = type;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_grant_roles(mvc *sql, sql_schema *schema, dlist *roles, dlist *grantees, int grant, int grantor)
{
	sql_rel *res = NULL;
	/* grant roles to the grantees */
	dnode *r, *g;

	(void) schema;
	for (r = roles->h; r; r = r->next) {
		char *role = r->data.sval;

		for (g = grantees->h; g; g = g->next) {
			char *grantee = g->data.sval;

			if ((res = rel_list(sql->sa, res, rel_role(sql->sa, grantee, role, grantor, grant, DDL_GRANT_ROLES))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_revoke_roles(mvc *sql, sql_schema *schema, dlist *roles, dlist *grantees, int admin, int grantor)
{
	sql_rel *res = NULL;
	/* revoke roles from the grantees */
	dnode *r, *g;

	(void) schema;
	for (r = roles->h; r; r = r->next) {
		char *role = r->data.sval;

		for (g = grantees->h; g; g = g->next) {
			char *grantee = g->data.sval;

			if ((res = rel_list(sql->sa, res, rel_role(sql->sa, grantee, role, grantor, admin, DDL_REVOKE_ROLES))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_priv(sql_allocator *sa, char *sname, char *name, char *grantee, int privs, char *cname, int grant, int grantor, int type)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	assert(type == DDL_GRANT || type == DDL_REVOKE);
	append(exps, exp_atom_clob(sa, sname));
	append(exps, exp_atom_clob(sa, name));
	append(exps, exp_atom_clob(sa, grantee));
	append(exps, exp_atom_int(sa, privs));
	append(exps, cname?(void*)exp_atom_clob(sa, cname):(void*)cname);
	append(exps, exp_atom_int(sa, grant));
	append(exps, exp_atom_int(sa, grantor));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = type;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_func_priv(sql_allocator *sa, char *sname, int func, char *grantee, int privs, int grant, int grantor, int type)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	assert(type == DDL_GRANT_FUNC || type == DDL_REVOKE_FUNC);
	append(exps, exp_atom_clob(sa, sname));
	append(exps, exp_atom_int(sa, func));
	append(exps, exp_atom_clob(sa, grantee));
	append(exps, exp_atom_int(sa, privs));
	append(exps, exp_atom_int(sa, grant));
	append(exps, exp_atom_int(sa, grantor));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = type;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_grant_global(mvc *sql, sql_schema *cur, dlist *privs, dlist *grantees, int grant, int grantor)
{
	sql_rel *res = NULL;
	char *sname = cur->base.name;
	dnode *gn;

	if (!privs)
		return NULL;
	sname = cur->base.name;
	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		for (opn = privs->h; opn; opn = opn->next) {
			int priv = opn->data.i_val;

			if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, NULL, grantee, priv, NULL, grant, grantor, DDL_GRANT))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_grant_table(mvc *sql, sql_schema *cur, dlist *privs, dlist *qname, dlist *grantees, int grant, int grantor)
{
	sql_rel *res = NULL;
	dnode *gn;
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE;
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);

	if (!sname)
		sname = cur->base.name;
	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		if (!privs) {
			if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, all, NULL, grant, grantor, DDL_GRANT))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
			continue;
		}
		for (opn = privs->h; opn; opn = opn->next) {
			symbol *op = opn->data.sym;
			int priv = PRIV_SELECT;
	
			switch (op->token) {
			case SQL_SELECT:
				priv = PRIV_SELECT;
				break;
			case SQL_UPDATE:
				priv = PRIV_UPDATE;
				break;
			case SQL_INSERT:
				priv = PRIV_INSERT;
				break;
			case SQL_DELETE:
				priv = PRIV_DELETE;
				break;
			case SQL_EXECUTE:
			default:
				return sql_error(sql, 02, "42000!Cannot GRANT EXECUTE on table name %s", tname);
			}

			if ((op->token == SQL_SELECT || op->token == SQL_UPDATE) && op->data.lval) {
				dnode *cn;

				for (cn = op->data.lval->h; cn; cn = cn->next) {
					char *cname = cn->data.sval;
					if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, priv, cname, grant, grantor, DDL_GRANT))) == NULL) {
						rel_destroy(res);
						return NULL;
					}
				}
			} else if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, priv, NULL, grant, grantor, DDL_GRANT))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_grant_func(mvc *sql, sql_schema *cur, dlist *privs, dlist *qname, dlist *typelist, int type, dlist *grantees, int grant, int grantor)
{
	sql_rel *res = NULL;
	dnode *gn;
	char *sname = qname_schema(qname);
	char *fname = qname_func(qname);
	sql_schema *s = NULL;
	sql_func *func = NULL;

	if (sname)
		s = mvc_bind_schema(sql, sname);
	else
		s = cur;
	func = resolve_func(sql, s, fname, typelist, type, "GRANT");
	if (!func) 
		return NULL;
	if (!func->s) 
		return sql_error(sql, 02, "42000!Cannot GRANT EXECUTE on system function '%s'", fname);

	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		if (!privs) {
			if ((res = rel_list(sql->sa, res, rel_func_priv(sql->sa, s->base.name, func->base.id, grantee, PRIV_EXECUTE, grant, grantor, DDL_GRANT_FUNC))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
			continue;
		}
		for (opn = privs->h; opn; opn = opn->next) {
			symbol *op = opn->data.sym;
	
			if (op->token != SQL_EXECUTE) 
				return sql_error(sql, 02, "42000!Can only GRANT 'EXECUTE' on function '%s'", fname);
			if ((res = rel_list(sql->sa, res, rel_func_priv(sql->sa, s->base.name, func->base.id, grantee, PRIV_EXECUTE, grant, grantor, DDL_GRANT_FUNC))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}


static sql_rel *
rel_grant_privs(mvc *sql, sql_schema *cur, dlist *privs, dlist *grantees, int grant, int grantor)
{
	dlist *obj_privs = privs->h->data.lval;
	symbol *obj = privs->h->next->data.sym;
	int token = obj->token;

	if (token == SQL_NAME) {
		dlist *qname = obj->data.lval;
		char *sname = qname_schema(qname);
		char *tname = qname_table(qname);
		sql_schema *s = cur;

		if (sname)
			s = mvc_bind_schema(sql, sname);
		if (s && mvc_bind_table(sql, s, tname) != NULL)
			token = SQL_TABLE;
	}

	switch (token) {
	case SQL_GRANT: 
		return rel_grant_global(sql, cur, obj_privs, grantees, grant, grantor);
	case SQL_TABLE:
	case SQL_NAME:
		return rel_grant_table(sql, cur, obj_privs, obj->data.lval, grantees, grant, grantor);
	case SQL_FUNC: {
		dlist *r = obj->data.lval;
		dlist *qname = r->h->data.lval;
		dlist *typelist = r->h->next->data.lval;
		int type = r->h->next->next->data.i_val;

		return rel_grant_func(sql, cur, obj_privs, qname, typelist, type, grantees, grant, grantor);
	}
	default:
		return sql_error(sql, 02, "M0M03!Grant: unknown token %d", token);
	}
}

static sql_rel *
rel_revoke_global(mvc *sql, sql_schema *cur, dlist *privs, dlist *grantees, int grant, int grantor)
{
	sql_rel *res = NULL;
	char *sname = cur->base.name;
	dnode *gn;

	if (!privs)
		return NULL;
	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		for (opn = privs->h; opn; opn = opn->next) {
			int priv = opn->data.i_val;

			if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, NULL, grantee, priv, NULL, grant, grantor, DDL_REVOKE))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_revoke_table(mvc *sql, sql_schema *cur, dlist *privs, dlist *qname, dlist *grantees, int grant, int grantor)
{
	dnode *gn;
	sql_rel *res = NULL;
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE;
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);

	if (!sname)
		sname = cur->base.name;
	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		if (!privs) {
			if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, all, NULL, grant, grantor, DDL_REVOKE))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
			continue;
		}
		for (opn = privs->h; opn; opn = opn->next) {
			symbol *op = opn->data.sym;
			int priv = PRIV_SELECT;

			switch (op->token) {
			case SQL_SELECT:
				priv = PRIV_SELECT;
				break;
			case SQL_UPDATE:
				priv = PRIV_UPDATE;
				break;

			case SQL_INSERT:
				priv = PRIV_INSERT;
				break;
			case SQL_DELETE:
				priv = PRIV_DELETE;
				break;

			case SQL_EXECUTE:
			default:
				return sql_error(sql, 02, "42000!Cannot GRANT EXECUTE on table name %s", tname);
			}

			if ((op->token == SQL_SELECT || op->token == SQL_UPDATE) && op->data.lval) {
				dnode *cn;

				for (cn = op->data.lval->h; cn; cn = cn->next) {
					char *cname = cn->data.sval;
					if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, priv, cname, grant, grantor, DDL_REVOKE))) == NULL) {
						rel_destroy(res);
						return NULL;
					}
				}
			} else if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, priv, NULL, grant, grantor, DDL_REVOKE))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_revoke_func(mvc *sql, sql_schema *cur, dlist *privs, dlist *qname, dlist *typelist, int type, dlist *grantees, int grant, int grantor)
{
	dnode *gn;
	sql_rel *res = NULL;
	char *sname = qname_schema(qname);
	char *fname = qname_func(qname);
	sql_func *func = NULL;

	sql_schema *s = NULL;

	if (sname)
		s = mvc_bind_schema(sql, sname);
	else
		s = cur;
	func = resolve_func(sql, s, fname, typelist, type, "REVOKE");
	if (!func) 
		return NULL;
	if (!func->s)
		return sql_error(sql, 02, "42000!Cannot REVOKE EXECUTE on system function '%s'", fname);
	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		if (!privs) {
			if ((res = rel_list(sql->sa, res, rel_func_priv(sql->sa, s->base.name, func->base.id, grantee, PRIV_EXECUTE, grant, grantor, DDL_REVOKE_FUNC))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
			continue;
		}
		for (opn = privs->h; opn; opn = opn->next) {
			symbol *op = opn->data.sym;

			if (op->token != SQL_EXECUTE) 
				return sql_error(sql, 02, "42000!Can only REVOKE EXECUTE on function name %s", fname);

			if ((res = rel_list(sql->sa, res, rel_func_priv(sql->sa, s->base.name, func->base.id, grantee, PRIV_EXECUTE, grant, grantor, DDL_REVOKE_FUNC))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_revoke_privs(mvc *sql, sql_schema *cur, dlist *privs, dlist *grantees, int grant, int grantor)
{
	dlist *obj_privs = privs->h->data.lval;
	symbol *obj = privs->h->next->data.sym;
	int token = obj->token;

	if (token == SQL_NAME) {
		dlist *qname = obj->data.lval;
		char *sname = qname_schema(qname);
		char *tname = qname_table(qname);
		sql_schema *s = cur;

		if (sname)
			s = mvc_bind_schema(sql, sname);
		if (s && mvc_bind_table(sql, s, tname) != NULL)
			token = SQL_TABLE;
	}

	switch (token) {
	case SQL_GRANT: 
		return rel_revoke_global(sql, cur, obj_privs, grantees, grant, grantor);
	case SQL_TABLE:
		return rel_revoke_table(sql, cur, obj_privs, obj->data.lval, grantees, grant, grantor);
	case SQL_NAME:
		return rel_revoke_table(sql, cur, obj_privs, obj->data.lval, grantees, grant, grantor);
	case SQL_FUNC: {
		dlist *r = obj->data.lval;
		dlist *qname = r->h->data.lval;
		dlist *typelist = r->h->next->data.lval;
		int type = r->h->next->next->data.i_val;

		return rel_revoke_func(sql, cur, obj_privs, qname, typelist, type, grantees, grant, grantor);
	}
	default:
		return sql_error(sql, 02, "M0M03!Grant: unknown token %d", token);
	}
}

/* iname, itype, sname.tname (col1 .. coln) */
static sql_rel *
rel_create_index(mvc *sql, char *iname, idx_type itype, dlist *qname, dlist *column_list)
{
	sql_schema *s = NULL;
	sql_table *t, *nt;
	sql_rel *r, *res;
	sql_exp ** updates, *e;
	sql_idx *i;
	dnode *n;
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	       
	if (sname && !(s = mvc_bind_schema(sql, sname))) 
		return sql_error(sql, 02, "3F000!CREATE INDEX: no such schema '%s'", sname);
	if (!s) 
		s = cur_schema(sql);
	i = mvc_bind_idx(sql, s, iname);
	if (i) 
		return sql_error(sql, 02, "42S11!CREATE INDEX: name '%s' already in use", iname);
	t = mvc_bind_table(sql, s, tname);
	if (!t) {
		return sql_error(sql, 02, "42S02!CREATE INDEX: no such table '%s'", tname);
	} else if (isView(t)) {
		return sql_error(sql, 02, "42S02!CREATE INDEX: cannot create index on view '%s'", tname);
	}
	sname = get_schema_name( sql, sname, tname);
	nt = dup_sql_table(sql->sa, t);

	if (t->persistence != SQL_DECLARED_TABLE)
		sname = s->base.name;
	if (t->s && !nt->s)
		nt->s = t->s;

	/* add index here */
	i = mvc_create_idx(sql, nt, iname, itype);
	for (n = column_list->h; n; n = n->next) {
		sql_column *c = mvc_bind_column(sql, nt, n->data.sval);

		if (!c) 
			return sql_error(sql, 02, "42S22!CREATE INDEX: no such column '%s'", n->data.sval);
		mvc_create_ic(sql, i, c);
	}

	/* new columns need update with default values */
	updates = table_update_array(sql, nt); 
	e = exp_column(sql->sa, nt->base.name, "%TID%", sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
	res = rel_table(sql, DDL_ALTER_TABLE, sname, nt, 0);
	r = rel_project(sql->sa, res, append(new_exp_list(sql->sa),e));
	res = rel_update(sql, res, r, updates, NULL); 
	return res;
}

static sql_rel *
rel_create_user(sql_allocator *sa, char *user, char *passwd, int enc, char *fullname, char *schema)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	append(exps, exp_atom_clob(sa, user));
	append(exps, exp_atom_clob(sa, passwd));
	append(exps, exp_atom_int(sa, enc));
	append(exps, exp_atom_clob(sa, schema));
	append(exps, exp_atom_clob(sa, fullname));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = DDL_CREATE_USER;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_alter_user(sql_allocator *sa, char *user, char *passwd, int enc, char *schema, char *oldpasswd)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	append(exps, exp_atom_clob(sa, user));
	append(exps, exp_atom_clob(sa, passwd));
	append(exps, exp_atom_int(sa, enc));
	append(exps, exp_atom_clob(sa, schema));
	append(exps, exp_atom_clob(sa, oldpasswd));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = DDL_ALTER_USER;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

sql_rel *
rel_schemas(mvc *sql, symbol *s)
{
	sql_rel *ret = NULL;

	if (s->token != SQL_CREATE_TABLE && s->token != SQL_CREATE_VIEW && STORE_READONLY) 
		return sql_error(sql, 06, "25006!schema statements cannot be executed on a readonly database.");

	switch (s->token) {
	case SQL_CREATE_SCHEMA:
	{
		dlist *l = s->data.lval;

		ret = rel_create_schema(sql, l->h->data.lval,
				l->h->next->next->next->data.lval);
	} 	break;
	case SQL_DROP_SCHEMA:
	{
		dlist *l = s->data.lval;
		dlist *auth_name = l->h->data.lval;

		assert(l->h->next->type == type_int);
		ret = rel_schema(sql->sa, DDL_DROP_SCHEMA, 
			   dlist_get_schema_name(auth_name),
			   NULL,
			   l->h->next->data.i_val);	/* drop_action */
	} 	break;
	case SQL_CREATE_TABLE:
	{
		dlist *l = s->data.lval;
		dlist *qname = l->h->next->data.lval;
		char *sname = qname_schema(qname);
		char *name = qname_table(qname);
		int temp = l->h->data.i_val;

		assert(l->h->type == type_int);
		assert(l->h->next->next->next->type == type_int);
		ret = rel_create_table(sql, cur_schema(sql), temp, sname, name, l->h->next->next->data.sym, l->h->next->next->next->data.i_val, l->h->next->next->next->next->data.sval);
	} 	break;
	case SQL_CREATE_VIEW:
	{
		dlist *l = s->data.lval;

		assert(l->h->next->next->next->type == type_int);
		assert(l->h->next->next->next->next->type == type_int);
		ret = rel_create_view(sql, NULL, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.sym, l->h->next->next->next->data.i_val, l->h->next->next->next->next->data.i_val);
	} 	break;
	case SQL_DROP_TABLE:
	{
		dlist *l = s->data.lval;
		char *sname = qname_schema(l->h->data.lval);
		char *tname = qname_table(l->h->data.lval);

		assert(l->h->next->type == type_int);
		sname = get_schema_name(sql, sname, tname);
		ret = rel_schema(sql->sa, DDL_DROP_TABLE, sname, tname, l->h->next->data.i_val);
	} 	break;
	case SQL_DROP_VIEW:
	{
		dlist *l = s->data.lval;
		char *sname = qname_schema(l->h->data.lval);
		char *tname = qname_table(l->h->data.lval);

		assert(l->h->next->type == type_int);
		sname = get_schema_name(sql, sname, tname);
		ret = rel_schema(sql->sa, DDL_DROP_VIEW, sname, tname, l->h->next->data.i_val);
	} 	break;
	case SQL_ALTER_TABLE:
	{
		dlist *l = s->data.lval;

		ret = sql_alter_table(sql, 
			l->h->data.lval,      /* table name */
		  	l->h->next->data.sym);/* table element */
	} 	break;
	case SQL_GRANT_ROLES:
	{
		dlist *l = s->data.lval;

		assert(l->h->next->next->type == type_int);
		assert(l->h->next->next->next->type == type_int);
		ret = rel_grant_roles(sql, cur_schema(sql), l->h->data.lval,	/* authids */
				  l->h->next->data.lval,	/* grantees */
				  l->h->next->next->data.i_val,	/* admin? */
				  l->h->next->next->next->data.i_val == cur_user ? sql->user_id : sql->role_id);
		/* grantor ? */
	} 	break;
	case SQL_REVOKE_ROLES:
	{
		dlist *l = s->data.lval;

		assert(l->h->next->next->type == type_int);
		assert(l->h->next->next->next->type == type_int);
		ret = rel_revoke_roles(sql, cur_schema(sql), l->h->data.lval,	/* authids */
				  l->h->next->data.lval,	/* grantees */
				  l->h->next->next->data.i_val,	/* admin? */
				  l->h->next->next->next->data.i_val  == cur_user? sql->user_id : sql->role_id);
		/* grantor ? */
	} 	break;
	case SQL_GRANT:
	{
		dlist *l = s->data.lval;

		assert(l->h->next->next->type == type_int);
		assert(l->h->next->next->next->type == type_int);
		ret = rel_grant_privs(sql, cur_schema(sql), l->h->data.lval,	/* privileges */
				  l->h->next->data.lval,	/* grantees */
				  l->h->next->next->data.i_val,	/* grant ? */
				  l->h->next->next->next->data.i_val  == cur_user? sql->user_id : sql->role_id);
		/* grantor ? */
	} 	break;
	case SQL_REVOKE:
	{
		dlist *l = s->data.lval;

		assert(l->h->next->next->type == type_int);
		assert(l->h->next->next->next->type == type_int);
		ret = rel_revoke_privs(sql, cur_schema(sql), l->h->data.lval,	/* privileges */
				   l->h->next->data.lval,	/* grantees */
				   l->h->next->next->data.i_val,	/* grant ? */
				   l->h->next->next->next->data.i_val  == cur_user? sql->user_id : sql->role_id);
		/* grantor ? */
	} 	break;
	case SQL_CREATE_ROLE:
	{
		dlist *l = s->data.lval;
		char *rname = l->h->data.sval;
		ret = rel_schema2(sql->sa, DDL_CREATE_ROLE, rname, NULL,
				 l->h->next->data.i_val  == cur_user? sql->user_id : sql->role_id);
	} 	break;
	case SQL_DROP_ROLE:
	{
		char *rname = s->data.sval;
		ret = rel_schema2(sql->sa, DDL_DROP_ROLE, rname, NULL, 0);
	} 	break;
	case SQL_CREATE_INDEX: {
		dlist *l = s->data.lval;

		assert(l->h->next->type == type_int);
		ret = rel_create_index(sql, l->h->data.sval, (idx_type) l->h->next->data.i_val, l->h->next->next->data.lval, l->h->next->next->next->data.lval);
	} 	break;
	case SQL_DROP_INDEX: {
		dlist *l = s->data.lval;
		char *sname = qname_schema(l);

		if (!sname)
			sname = cur_schema(sql)->base.name;
		ret = rel_schema2(sql->sa, DDL_DROP_INDEX, sname, qname_index(l), 0);
	} 	break;
	case SQL_CREATE_USER: {
		dlist *l = s->data.lval;

		ret = rel_create_user(sql->sa, l->h->data.sval,	/* user name */
				  l->h->next->data.sval,	/* password */
				  l->h->next->next->next->next->data.i_val == SQL_PW_ENCRYPTED, /* encrypted */
				  l->h->next->next->data.sval,	/* fullname */
				  l->h->next->next->next->data.sval);	/* dschema */
	} 	break;
	case SQL_DROP_USER:
		ret = rel_schema2(sql->sa, DDL_DROP_USER, s->data.sval, NULL, 0);
		break;
	case SQL_ALTER_USER: {
		dlist *l = s->data.lval;
		dnode *a = l->h->next->data.lval->h;

		ret = rel_alter_user(sql->sa, l->h->data.sval,	/* user */
				     a->data.sval,	/* passwd */
				     a->next->next->data.i_val == SQL_PW_ENCRYPTED, /* encrypted */
				     a->next->data.sval,	/* schema */
				     a->next->next->next->data.sval /* old passwd */
		    );
	} 	break;
	case SQL_RENAME_USER: {
		dlist *l = s->data.lval;

		ret = rel_schema2(sql->sa, DDL_RENAME_USER, l->h->data.sval, l->h->next->data.sval, 0);
	} 	break;
	case SQL_CREATE_TYPE: {
		dlist *l = s->data.lval;

		ret = rel_create_type(sql, l->h->data.lval, l->h->next->data.sval);
	} 	break;
	case SQL_DROP_TYPE: {
		dlist *l = s->data.lval;
		ret = rel_drop_type(sql, l->h->data.lval, l->h->next->data.i_val);
	} 	break;
	default:
		return sql_error(sql, 01, "M0M03!schema statement unknown symbol(" PTRFMT ")->token = %s", PTRFMTCAST s, token2string(s->token));
	}

	sql->type = Q_SCHEMA;
	return ret;
}

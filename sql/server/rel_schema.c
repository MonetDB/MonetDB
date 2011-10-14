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


#include "monetdb_config.h"
#include "rel_trans.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "rel_exp.h"
#include "rel_bin.h"
#include "rel_subquery.h"
#include "rel_schema.h"
#include "sql_parser.h"
#include "sql_privileges.h"

#define qname_index(qname) qname_table(qname)

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
rel_table(mvc *sql, int cat_type, char *sname, sql_table *t, int nr)
{
	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);

	append(exps, exp_atom_int(sql->sa, nr));
	append(exps, exp_atom_str(sql->sa, sname, sql_bind_localtype("str") ));
	if (t)
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
	if (n || m) {
		list_destroy(l);
		return sql_error(sql, 02, "Column lists do not match");
	}
	(void)name;
	sq = rel_project(sql->sa, sq, l);
	set_processed(sq);
	return sq;
}

static char *
as_subquery( mvc *sql, sql_table *t, sql_rel *sq, dlist *column_spec )
{
        sql_rel *r = sq;

	if (!r)
		return NULL;

        if (is_topn(r->op))
                r = sq->l;

	if (column_spec) {
		dnode *n = column_spec->h;
		node *m = r->exps->h;

		for (; n; n = n->next, m = m->next) {
			char *cname = n->data.sval;
			sql_exp *e = m->data;
			sql_subtype *tp = exp_subtype(e);

			if (mvc_bind_column(sql, t, cname))
				return cname;
			mvc_create_column(sql, t, cname, tp);
		}
	} else {
		node *m;

		for (m = r->exps->h; m; m = m->next) {
			sql_exp *e = m->data;
			char *cname = exp_name(e);
			sql_subtype *tp = exp_subtype(e);

			if (!cname)
				cname = "v";
			if (mvc_bind_column(sql, t, cname))
				return cname;
			mvc_create_column(sql, t, cname, tp);
		}
	}
	return NULL;
}

sql_table *
mvc_create_table_as_subquery( mvc *sql, sql_rel *sq, sql_schema *s, char *tname, dlist *column_spec, int temp, int commit_action )
{
	char *n;
	int tt = (temp == SQL_STREAM)?tt_stream:
	         ((temp == SQL_MERGE_TABLE)?tt_merge_table:tt_table);

	sql_table *t = mvc_create_table(sql, s, tname, tt, 0, SQL_DECLARED_TABLE, commit_action, -1);
	if ((n = as_subquery( sql, t, sq, column_spec)) != NULL) {
		sql_error(sql, 01, "CREATE TABLE: duplicate column name %s", n);

		return NULL;
	}
	return t;
}


static char *
table_constraint_name(symbol *s, sql_table *t)
{
	/* create a descriptive name like table_col_pkey */
	char *suffix;		/* stores the type of this constraint */
	dnode *nms = NULL;
	static char buf[BUFSIZ];

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
	strncpy(buf, t->base.name, BUFSIZ);

	/* add column name(s) */
	for (; nms; nms = nms->next) {
		strncat(buf, "_", BUFSIZ - strlen(buf));
		strncat(buf, nms->data.sval, BUFSIZ - strlen(buf));
	}

	/* add suffix */
	strncat(buf, suffix, BUFSIZ - strlen(buf));

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

	switch (s->token) {
	case SQL_UNIQUE:
	case SQL_PRIMARY_KEY: {
		key_type kt = (s->token == SQL_UNIQUE) ? ukey : pkey;
		sql_key *k;

		if (kt == pkey && t->pkey) {
			(void) sql_error(sql, 02, "CONSTRAINT PRIMARY KEY: a table can have only one PRIMARY KEY\n");
			return res;
		}
		if (name && mvc_bind_key(sql, ss, name)) {
			(void) sql_error(sql, 02, "CONSTRAINT PRIMARY KEY: key %s already exists", name);
			return res;
		}
		k = (sql_key*)mvc_create_ukey(sql, t, name, kt);

		mvc_create_kc(sql, k, cs);
		mvc_create_ukey_done(sql, k);
		res = SQL_OK;
	} 	break;
	case SQL_FOREIGN_KEY: {
		dnode *n = s->data.lval->h;
		char *rtname = qname_table(n->data.lval);
		int ref_actions = n->next->next->next->data.i_val; 
		sql_table *rt;
		sql_fkey *fk;
		list *cols;
		sql_key *rk = NULL;

		assert(n->next->next->next->type == type_int);
/*
		if (isTempTable(t)) {
			(void) sql_error(sql, 02, "CONSTRAINT: constraints on temporary tables are not supported\n");
			return res;
		}
*/
		rt = _bind_table(t, ss, cur_schema(sql), rtname);
		if (!rt) {
			(void) sql_error(sql, 02, "CONSTRAINT FOREIGN KEY: no such table table '%s'\n", rtname);
			return res;
		}
		if (name && mvc_bind_key(sql, ss, name)) {
			(void) sql_error(sql, 02, "CONSTRAINT FOREIGN KEY: key %s already exists", name);
			return res;
		}

		/* find unique referenced key */
		if (n->next->data.lval) {	
			char *rcname = n->next->data.lval->h->data.sval;
			cols = list_append(list_create(NULL), rcname);
			rk = mvc_bind_ukey(rt, cols);
			list_destroy(cols);
		} else if (rt->pkey) {
			/* no columns specified use rt.pkey */
			rk = &rt->pkey->k;
		}
		if (!rk) {
			(void) sql_error(sql, 02, "CONSTRAINT FOREIGN KEY: could not find referenced PRIMARY KEY in table %s\n", rtname);
			return res;
		}
		fk = mvc_create_fkey(sql, t, name, fkey, rk, ref_actions & 255, (ref_actions>>8) & 255);
		mvc_create_fkc(sql, fk, cs);
		res = SQL_OK;
	} 	break;
	case SQL_NOT_NULL:
	case SQL_NULL: {
		int null = (s->token == SQL_NOT_NULL) ? 0 : 1;

		mvc_null(sql, cs, null);
		res = SQL_OK;
	} 	break;
	}
	if (res == SQL_ERR) {
		(void) sql_error(sql, 02, "unknown constraint (" PTRFMT ")->token = %s\n", PTRFMTCAST s, token2string(s->token));
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
		char *err = NULL, *r = symbol2string(sql, s->data.sym, &err);

		if (!r) {
			(void) sql_error(sql, 02, "incorrect default value '%s'\n", err?err:"");
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
		int null = (s->token == SQL_NOT_NULL) ? 0 : 1;

		mvc_null(sql, cs, null);
		res = SQL_OK;
	} 	break;
	}
	if (res == SQL_ERR) {
		(void) sql_error(sql, 02, "unknown column option (" PTRFMT ")->token = %s\n", PTRFMTCAST s, token2string(s->token));
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
	char *rtname = qname_table(n->data.lval);
	sql_table *ft = mvc_bind_table(sql, ss, rtname);

	if (!ft) {
		sql_error(sql, 02, "CONSTRAINT FOREIGN KEY: no such table '%s'\n", rtname);
		return SQL_ERR;
	} else {
		sql_key *rk = NULL;
		sql_fkey *fk;
		dnode *nms = n->next->data.lval->h;
		node *fnms;
		int ref_actions = n->next->next->next->next->data.i_val;

		assert(n->next->next->next->next->type == type_int);
		if (name && mvc_bind_key(sql, ss, name)) {
			sql_error(sql, 02, "Create Key failed, key %s allready exists", name);
			return SQL_ERR;
		}
		if (n->next->next->data.lval) {	/* find unique referenced key */
			dnode *rnms = n->next->next->data.lval->h;
			list *cols = list_create(NULL);

			for (; rnms; rnms = rnms->next)
				list_append(cols, rnms->data.sval);

			/* find key in ft->keys */
			rk = mvc_bind_ukey(ft, cols);
			list_destroy(cols);
		} else if (ft->pkey) {	
			/* no columns specified use ft.pkey */
			rk = &ft->pkey->k;
		}
		if (!rk) {
			sql_error(sql, 02, "CONSTRAINT FOREIGN KEY: could not find referenced PRIMARY KEY in table '%s'\n", ft->base.name);
			return SQL_ERR;
		}
		fk = mvc_create_fkey(sql, t, name, fkey, rk, ref_actions & 255, (ref_actions>>8) & 255);

		for (fnms = rk->columns->h; nms && fnms; nms = nms->next, fnms = fnms->next) {
			char *nm = nms->data.sval;
			sql_column *c = mvc_bind_column(sql, t, nm);

			if (!c) {
				sql_error(sql, 02, "CONSTRAINT FOREIGN KEY: no such column '%s' in table '%s'\n", nm, t->base.name);
				return SQL_ERR;
			}
			mvc_create_fkc(sql, fk, c);
		}
		if (nms || fnms) {
			sql_error(sql, 02, "CONSTRAINT FOREIGN KEY: not all columns are handled\n");
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
			sql_error(sql, 02, "CONSTRAINT PRIMARY KEY: a table can have only one PRIMARY KEY\n");
			return SQL_ERR;
		}
		if (name && mvc_bind_key(sql, ss, name)) {
			sql_error(sql, 02, "CONSTRAINT %s: key %s already exists",
					kt == pkey ? "PRIMARY KEY" : "UNIQUE", name);
			return SQL_ERR;
		}
			
 		k = (sql_key*)mvc_create_ukey(sql, t, name, kt);
		for (; nms; nms = nms->next) {
			char *nm = nms->data.sval;
			sql_column *c = mvc_bind_column(sql, t, nm);

			if (!c) {
				sql_error(sql, 02, "CONSTRAINT %s: no such column '%s' for table '%s'",
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
	if (!res) {
		sql_error(sql, 02, "table constraint type: wrong token (" PTRFMT ") = %s\n", PTRFMTCAST s, token2string(s->token));
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
	}

	if (!res) {
		sql_error(sql, 02, "table constraint: wrong token (" PTRFMT ") = %s\n", PTRFMTCAST s, token2string(s->token));
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
		sql_error(sql, 02, "ALTER TABLE: cannot add column to VIEW '%s'\n", t->base.name);
		return SQL_ERR;
	}
	if (l->h->next->next)
		opt_list = l->h->next->next->data.lval;

	if (cname && ctype) {
		sql_column *cs = NULL;

		cs = find_sql_column(t, cname);
		if (cs) {
			sql_error(sql, 02, "%s TABLE: a column named '%s' already exists\n", (alter)?"ALTER":"CREATE", cname);
			return SQL_ERR;
		}
		cs = mvc_create_column(sql, t, cname, ctype);
		if (column_options(sql, opt_list, ss, t, cs) == SQL_ERR)
			return SQL_ERR;
	}

	if (res == SQL_ERR) 
		sql_error(sql, 02, "CREATE: column type or name");
	return res;
}

static int 
table_element(mvc *sql, symbol *s, sql_schema *ss, sql_table *t, int alter)
{
	int res = SQL_OK;

	if (alter && (isView(t) || (isMergeTable(t) && s->token != SQL_TABLE && s->token != SQL_DROP_TABLE) || (isTable(t) && (s->token == SQL_TABLE || s->token == SQL_DROP_TABLE)) )){
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
		sql_error(sql, 02, "ALTER TABLE: cannot %s %s '%s'\n",
				msg, 
				isMergeTable(t)?"MERGE TABLE":"VIEW",
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
			sql_error(sql, 02, "ALTER TABLE: no such column '%s'\n", cname);
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
			sql_error(sql, 02, "ALTER TABLE: no such column '%s'\n", cname);
			return SQL_ERR;
		}
		r = symbol2string(sql, sym, &err);
		if (!r) {
			(void) sql_error(sql, 02, "incorrect default value '%s'\n", err?err:"");
			if (err) _DELETE(err);
			return SQL_ERR;
		}
		mvc_default(sql, c, r);
		_DELETE(r);
	}
	break;
	case SQL_NOT_NULL:
	case SQL_NULL:
	{
		dnode *n = s->data.lval->h;
		char *cname = n->data.sval;
		sql_column *c = mvc_bind_column(sql, t, cname);
		int null = (s->token == SQL_NOT_NULL) ? 0 : 1;

		if (!c) {
			sql_error(sql, 02, "ALTER TABLE: no such column '%s'\n", cname);
			return SQL_ERR;
		}
		mvc_null(sql, c, null);
	} 	break;
	case SQL_DROP_DEFAULT:
	{
		char *cname = s->data.sval;
		sql_column *c = mvc_bind_column(sql, t, cname);
		if (!c) {
			sql_error(sql, 02, "ALTER TABLE: no such column '%s'\n", cname);
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
			sql_error(sql, 02, "CREATE TABLE: no such schema '%s'", sname);
			return SQL_ERR;
		}
		if (!os)
			os = ss;
	       	ot = mvc_bind_table(sql, os, name);
		if (!ot)
			return SQL_ERR;
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
			sql_error(sql, 02, "ALTER TABLE: no such column '%s'\n", cname);
			return SQL_ERR;
		}
		if (cs_size(&t->columns) <= 1) {
			sql_error(sql, 02, "ALTER TABLE: cannot drop column '%s': table needs at least one column\n", cname);
			return SQL_ERR;
		}
		if (t->system) {
			sql_error(sql, 02, "ALTER TABLE: cannot drop column '%s': table is a system table\n", cname);
			return SQL_ERR;
		}
		if (isView(t)) {
			sql_error(sql, 02, "ALTER TABLE: cannot drop column '%s': '%s' is a view\n", cname, t->base.name);
			return SQL_ERR;
		}
		if (!drop_action && mvc_check_dependency(sql, col->base.id, COLUMN_DEPENDENCY, NULL)) {
			sql_error(sql, 02, "ALTER TABLE: cannot drop column '%s': there are database objects which depend on it\n", cname);
			return SQL_ERR;
		}
		if (!drop_action  && t->keys.set) {
			node *n, *m;

			for (n = t->keys.set->h; n; n = n->next) {
				sql_key *k = n->data;
				for (m = k->columns->h; m; m = m->next) {
					sql_kc *kc = m->data;
					if (strcmp(kc->c->base.name, cname) == 0) {
						sql_error(sql, 02, "ALTER TABLE: cannot drop column '%s': there are constraints which depend on it\n", cname);
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
		sql_error(sql, 02, "unknown table element (" PTRFMT ")->token = %s\n", PTRFMTCAST s, token2string(s->token));
		return SQL_ERR;
	}
	return res;
}

sql_rel *
rel_create_table(mvc *sql, sql_schema *ss, int temp, char *sname, char *name, symbol *table_elements_or_subquery, int commit_action)
{
	sql_schema *s = NULL;

	int instantiate = (sql->emode == m_instantiate);
	int deps = (sql->emode == m_deps);
	int create = (!instantiate && !deps);
	int tt = (temp == SQL_STREAM)?tt_stream:
	         ((temp == SQL_MERGE_TABLE)?tt_merge_table:tt_table);

	(void)create;
	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, "CREATE TABLE: no such schema '%s'", sname);

	if (temp != SQL_PERSIST && tt == tt_table && 
			commit_action == CA_COMMIT)
		commit_action = CA_DELETE;
	
	if (temp != SQL_DECLARED_TABLE) {
		if (temp != SQL_PERSIST && tt == tt_table) {
			s = mvc_bind_schema(sql, "tmp");
		} else if (s == NULL) {
			s = ss;
		}
	}

	if (temp != SQL_DECLARED_TABLE && s)
		sname = s->base.name;

	if (mvc_bind_table(sql, s, name)) {
		char *cd = (temp == SQL_DECLARED_TABLE)?"DECLARE":"CREATE";
		return sql_error(sql, 02, "%s TABLE: name '%s' already in use", cd, name);
	} else if (temp != SQL_DECLARED_TABLE &&!schema_privs(sql->role_id, s)){
		return sql_error(sql, 02, "CREATE TABLE: insufficient privileges for user '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	} else if (table_elements_or_subquery->token == SQL_CREATE_TABLE) { 
		/* table element list */
		sql_table *t = mvc_create_table(sql, s, name, tt, 0, SQL_DECLARED_TABLE, commit_action, -1);
		dnode *n;
		dlist *columns = table_elements_or_subquery->data.lval;

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
		if (create && (t = mvc_create_table_as_subquery( sql, sq, s, name, column_spec, temp, commit_action)) == NULL) { 
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
		return sql_error(sql, 02, "CREATE VIEW: no such schema '%s'", sname);
	if (s == NULL)
		s = cur_schema(sql);

	if (create && mvc_bind_table(sql, s, name) != NULL) {
		return sql_error(sql, 02, "CREATE VIEW: name '%s' already in use", name);
	} else if (create && !schema_privs(sql->role_id, s)) {
		return sql_error(sql, 02, "CREATE VIEW: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	} else if (query) {
		char emode = sql->emode;
		sql_rel *sq = NULL;
		char *q = QUERY(sql->scanner);

		if (query->token == SQL_SELECT) {
			SelectNode *sn = (SelectNode *) query;

			if (sn->limit)
				return sql_error(sql, 01, "CREATE VIEW: LIMIT not supported");
			if (sn->orderby)
				return sql_error(sql, 01, "CREATE VIEW: ORDER BY not supported");
		}

		if (create) /* for subtable we only need direct dependencies */
			sql->emode = m_deps;
		sq = rel_selects(sql, query);
		sql->emode = emode;
		if (!sq)
			return NULL;

		if (!create)
			rel_add_intern(sql, sq);

		if (create) {
			char *n;

			t = mvc_create_view(sql, s, name, SQL_DECLARED_TABLE, q, 0);
			if ((n = as_subquery( sql, t, sq, column_spec)) != NULL) {
				sql_error(sql, 01, "CREATE VIEW: duplicate column name %s", n);
				rel_destroy(sq);
				return NULL;
			}
			return rel_table(sql, DDL_CREATE_VIEW, s->base.name, t, SQL_PERSIST);
		} else {
			t = mvc_bind_table(sql, s, name);
		}

		if (!persistent && column_spec) 
			sq = view_rename_columns( sql, name, sq, column_spec);

		if (deps && sq && persistent) {
			stmt *sqs = rel_bin(sql, sq);
			list *view_id_l = stmt_list_dependencies(sql->sa, sqs, VIEW_DEPENDENCY);
			list *id_l = stmt_list_dependencies(sql->sa, sqs, COLUMN_DEPENDENCY);
			list *func_id_l = stmt_list_dependencies(sql->sa, sqs, FUNC_DEPENDENCY);
			mvc_create_dependencies(sql, id_l, t->base.id, VIEW_DEPENDENCY);
			mvc_create_dependencies(sql, view_id_l, t->base.id, VIEW_DEPENDENCY);
			mvc_create_dependencies(sql, func_id_l, t->base.id, VIEW_DEPENDENCY);
			rel_destroy(sq);
			return rel_project(sql->sa, NULL, NULL);
		}
		return sq;
	}
	return NULL;
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
rel_create_schema(mvc *sql, dlist *auth_name, dlist *schema_elements)
{
	char *name = dlist_get_schema_name(auth_name);
	char *auth = schema_auth(auth_name);
	int auth_id = sql->role_id;

	if (auth && (auth_id = sql_find_auth(sql, auth)) < 0) {
		sql_error(sql, 02, "CREATE SCHEMA: no such authorization '%s'", auth);
		return NULL;
	}
	if (sql->user_id != USER_MONETDB && sql->role_id != ROLE_SYSADMIN) {
		sql_error(sql, 02, "CREATE SCHEMA: insufficient privileges for user '%s'", stack_get_string(sql, "current_user"));
		return NULL;
	}
	if (mvc_bind_schema(sql, name)) {
		sql_error(sql, 02, "CREATE SCHEMA: name '%s' already in use", name);
		return NULL;
	} else {
		sql_schema *os = sql->session->schema;
		dnode *n;
		sql_schema *ss = ZNEW(sql_schema);
		sql_rel *ret;

		ret = rel_schema(sql->sa, DDL_CREATE_SCHEMA, 
			   dlist_get_schema_name(auth_name),
			   schema_auth(auth_name), 0);

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
rel_alter_table(mvc *sql, dlist *qname, symbol *te)
{
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL;

	if (sname && !(s=mvc_bind_schema(sql, sname))) {
		(void) sql_error(sql, 02, "ALTER TABLE: no such schema '%s'", sname);
		return NULL;
	}
	if (!s)
		s = cur_schema(sql);

	if ((t = mvc_bind_table(sql, s, tname)) == NULL) {
		return sql_error(sql, 02, "ALTER TABLE: no such table '%s'", tname);
	} else {
		node *n;
		sql_rel *res = NULL, *r;
		sql_table *nt = dup_sql_table(sql->sa, t);
		sql_exp ** updates, *e;

		if (nt && te && te->token == SQL_DROP_CONSTRAINT) {
			dlist *l = te->data.lval;
			char *kname = l->h->data.sval;
			int drop_action = l->h->next->data.i_val;
			
			sname = get_schema_name(sql, sname, tname);
			return rel_schema(sql->sa, DDL_DROP_CONSTRAINT, sname, kname, drop_action);
		}

		if (!nt || (te && table_element(sql, te, s, nt, 1) == SQL_ERR)) 
			return NULL;

		if (t->persistence != SQL_DECLARED_TABLE && s)
			sname = s->base.name;

		if (t->s && !nt->s)
			nt->s = t->s;

		if (!te) /* Set Read only */
			nt = mvc_readonly(sql, nt, 1);
		res = rel_table(sql, DDL_ALTER_TABLE, sname, nt, 0);
		if (!te) /* Set Read only */
			return res;
		/* table add table */
		if (te->token == SQL_TABLE) {
			char *ntname = te->data.lval->h->data.sval;
			sql_table *nnt = mvc_bind_table(sql, s, ntname);

			if (nnt)
				cs_add(&nt->tables, nnt, TR_NEW); 
		}
		/* table drop table */
		if (te->token == SQL_DROP_TABLE) {
			char *ntname = te->data.lval->h->data.sval;
			int drop_action = te->data.lval->h->next->data.i_val;
			node *n = cs_find_name(&nt->tables, ntname);

			if (n) {
				sql_table *ntt = n->data;

				ntt->drop_action = drop_action;
				cs_del(&nt->tables, n, ntt->base.flag); 
			}
		}

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
rel_role(sql_allocator *sa, char *grantee, char *auth, int type)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	assert(type == DDL_GRANT_ROLES || type == DDL_REVOKE_ROLES);
	append(exps, exp_atom_clob(sa, grantee));
	append(exps, exp_atom_clob(sa, auth));
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
	(void) grant;
	(void) grantor;		/* Stefan: unused!? */

	for (r = roles->h; r; r = r->next) {
		char *role = r->data.sval;

		for (g = grantees->h; g; g = g->next) {
			char *grantee = g->data.sval;

			if ((res = rel_list(sql->sa, res, rel_role(sql->sa, grantee, role, DDL_GRANT_ROLES))) == NULL) {
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
	(void) admin;
	(void) grantor;		/* Stefan: unused!? */

	for (r = roles->h; r; r = r->next) {
		char *role = r->data.sval;

		for (g = grantees->h; g; g = g->next) {
			char *grantee = g->data.sval;

			if ((res = rel_list(sql->sa, res, rel_role(sql->sa, grantee, role, DDL_REVOKE_ROLES))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_priv(sql_allocator *sa, char *name, char *grantee, int privs, char *cname, int grant, int grantor, int type)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	assert(type == DDL_GRANT || type == DDL_REVOKE);
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
rel_grant_table(mvc *sql, sql_schema *cur, dlist *privs, char *tname, dlist *grantees, int grant, int grantor)
{
	sql_rel *res = NULL;
	dnode *gn;
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE;

	(void)cur;
	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		if (!privs) {
			if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, tname, grantee, all, NULL, grant, grantor, DDL_GRANT))) == NULL) {
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
				return sql_error(sql, 02, "Cannot GRANT EXECUTE on table name %s", tname);
			}

			if ((op->token == SQL_SELECT || op->token == SQL_UPDATE) && op->data.lval) {
				dnode *cn;

				for (cn = op->data.lval->h; cn; cn = cn->next) {
					char *cname = cn->data.sval;
					if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, tname, grantee, priv, cname, grant, grantor, DDL_GRANT))) == NULL) {
						rel_destroy(res);
						return NULL;
					}
				}
			} else if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, tname, grantee, priv, NULL, grant, grantor, DDL_GRANT))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_grant_func(mvc *sql, sql_schema *cur, dlist *privs, char *fname, dlist *grantees, int grant, int grantor)
{
/* todo */
	(void) sql;
	(void) cur;
	(void) privs;
	(void) fname;
	(void) grantees;
	(void) grant;
	(void) grantor;
	return sql_error(sql, 02, "GRANT Table/Function name %s doesn't exist", fname);
}


static sql_rel *
rel_grant_privs(mvc *sql, sql_schema *cur, dlist *privs, dlist *grantees, int grant, int grantor)
{
	dlist *obj_privs = privs->h->data.lval;
	symbol *obj = privs->h->next->data.sym;
	int token = obj->token;

	if (token == SQL_NAME)
		if (mvc_bind_table(sql, cur, obj->data.sval) != NULL)
			token = SQL_TABLE;

	switch (token) {
	case SQL_TABLE:
		return rel_grant_table(sql, cur, obj_privs, obj->data.sval, grantees, grant, grantor);
	case SQL_NAME:
		return rel_grant_func(sql, cur, obj_privs, obj->data.sval, grantees, grant, grantor);
	default:
		return sql_error(sql, 02, "Grant: unknown token %d", token);
	}
}

static sql_rel *
rel_revoke_table(mvc *sql, sql_schema *cur, dlist *privs, char *tname, dlist *grantees, int grant, int grantor)
{
	dnode *gn;
	sql_rel *res = NULL;
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE;

	(void)cur;
	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		if (!privs) {
			if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, tname, grantee, all, NULL, grant, grantor, DDL_REVOKE))) == NULL) {
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
				return sql_error(sql, 02, "Cannot GRANT EXECUTE on table name %s", tname);
			}

			if ((op->token == SQL_SELECT || op->token == SQL_UPDATE) && op->data.lval) {
				dnode *cn;

				for (cn = op->data.lval->h; cn; cn = cn->next) {
					char *cname = cn->data.sval;
					if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, tname, grantee, priv, cname, grant, grantor, DDL_REVOKE))) == NULL) {
						rel_destroy(res);
						return NULL;
					}
				}
			} else if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, tname, grantee, priv, NULL, grant, grantor, DDL_REVOKE))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_revoke_func(mvc *sql, sql_schema *cur, dlist *privs, char *fname, dlist *grantees, int grant, int grantor)
{
/* todo */
	(void) sql;
	(void) cur;
	(void) privs;
	(void) fname;
	(void) grantees;
	(void) grant;
	(void) grantor;
	return NULL;
}

static sql_rel *
rel_revoke_privs(mvc *sql, sql_schema *cur, dlist *privs, dlist *grantees, int grant, int grantor)
{
	dlist *obj_privs = privs->h->data.lval;
	symbol *obj = privs->h->next->data.sym;
	int token = obj->token;

	if (token == SQL_NAME)
		if (mvc_bind_table(sql, cur, obj->data.sval) != NULL)
			token = SQL_TABLE;

	switch (token) {
	case SQL_TABLE:
		return rel_revoke_table(sql, cur, obj_privs, obj->data.sval, grantees, grant, grantor);
	case SQL_NAME:
		return rel_revoke_func(sql, cur, obj_privs, obj->data.sval, grantees, grant, grantor);
	default:
		return sql_error(sql, 02, "Grant: unknown token %d", token);
	}
}

/* iname, itype, sname.tname (col1 .. coln) */
static sql_rel *
rel_create_index(mvc *sql, sql_schema *ss, char *iname, int itype, dlist *qname, dlist *column_list)
{
	sql_allocator *sa = sql->sa;
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);
	char *tname = qname_table(qname);
	char *sname = qname_schema(qname);
	dnode *n = column_list->h;

	if (!sname && ss)
		sname = ss->base.name;

	append(exps, exp_atom_clob(sa, iname));
	append(exps, exp_atom_int(sa, itype));
	append(exps, exp_atom_clob(sa, sname));
	append(exps, exp_atom_clob(sa, tname));

	for (; n; n = n->next) {
		char *cname = n->data.sval;

		append(exps, exp_atom_clob(sa, cname));
	}
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = DDL_CREATE_INDEX;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
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

	if (s->token != SQL_CREATE_TABLE && s->token != SQL_CREATE_VIEW && STORE_READONLY(active_store_type)) 
		return sql_error(sql, 06, "schema statements cannot be executed on a readonly database.");

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
		ret = rel_create_table(sql, cur_schema(sql), temp, sname, name, l->h->next->next->data.sym, l->h->next->next->next->data.i_val);
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

		ret = rel_alter_table(sql, 
			l->h->data.lval,	/* table name */
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
				  l->h->next->next->next->data.i_val ? sql->user_id : sql->role_id);
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
				  l->h->next->next->next->data.i_val ? sql->user_id : sql->role_id);
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
				  l->h->next->next->next->data.i_val ? sql->user_id : sql->role_id);
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
				   l->h->next->next->next->data.i_val ? sql->user_id : sql->role_id);
		/* grantor ? */
	} 	break;
	case SQL_CREATE_ROLE:
	{
		dlist *l = s->data.lval;
		char *rname = l->h->data.sval;
		ret = rel_schema2(sql->sa, DDL_CREATE_ROLE, rname, NULL,
				 l->h->next->data.i_val);
	} 	break;
	case SQL_DROP_ROLE:
	{
		char *rname = s->data.sval;
		ret = rel_schema2(sql->sa, DDL_DROP_ROLE, rname, NULL, 0);
	} 	break;
	case SQL_CREATE_INDEX: {
		dlist *l = s->data.lval;

		assert(l->h->next->type == type_int);
		ret = rel_create_index(sql, cur_schema(sql), l->h->data.sval, l->h->next->data.i_val, l->h->next->next->data.lval, l->h->next->next->next->data.lval);
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

		ret = rel_schema2(sql->sa, DDL_CREATE_TYPE, 
				l->h->data.sval, l->h->next->data.sval, 0);
	} 	break;
	case SQL_DROP_TYPE: {
		ret = rel_schema2(sql->sa, DDL_DROP_TYPE, s->data.sval, NULL, 0);
	} 	break;
	default:
		return sql_error(sql, 01, "schema statement unknown symbol(" PTRFMT ")->token = %s", PTRFMTCAST s, token2string(s->token));
	}


	sql->last = NULL;
	sql->type = Q_SCHEMA;
	return ret;
}

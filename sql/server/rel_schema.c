/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "rel_propagate.h"
#include "sql_parser.h"
#include "sql_privileges.h"
#include "sql_partition.h"

#include "mal_authorize.h"

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

sql_rel *
rel_table(mvc *sql, int cat_type, const char *sname, sql_table *t, int nr)
{
	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);
	if (!rel || !exps)
		return NULL;

	append(exps, exp_atom_int(sql->sa, nr));
	append(exps, exp_atom_str(sql->sa, sname, sql_bind_localtype("str") ));
	append(exps, exp_atom_str(sql->sa, t->base.name, sql_bind_localtype("str") ));
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
	if (!rel || !exps)
		return NULL;

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
	if (!rel)
		return NULL;
	if (!l)
		return r;
	rel->l = l;
	rel->r = r;
	rel->op = op_ddl;
	rel->flag = ddl_list;
	return rel;
}

static sql_rel *
view_rename_columns(mvc *sql, const char *name, sql_rel *sq, dlist *column_spec)
{
	dnode *n = column_spec->h;
	node *m = sq->exps->h, *p = m;

	assert(is_project(sq->op));
	for (; n && m; n = n->next, p = m, m = m->next) {
		char *cname = n->data.sval;
		sql_exp *e = m->data;
		sql_exp *n = e;

		exp_setname(sql->sa, n, name, cname);
		set_basecol(n);
	}
	/* skip any intern columns */
	for (; m; m = m->next) {
		sql_exp *e = m->data;
		if (!is_intern(e))
			break;
	}
	if (p)
		p->next = 0;
	if (n || m)
		return sql_error(sql, 02, SQLSTATE(M0M03) "Column lists do not match");
	set_processed(sq);
	return sq;
}

static int
as_subquery(mvc *sql, sql_table *t, table_types tt, sql_rel *sq, dlist *column_spec, const char *msg)
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

			if (tt != tt_view && cname && cname[0] == '%') {
				sql_error(sql, 01, SQLSTATE(42000) "%s: generated labels not allowed in column names, use an alias instead", msg);
				return -1;
			} else if (mvc_bind_column(sql, t, cname)) {
				sql_error(sql, 01, SQLSTATE(42S21) "%s: duplicate column name %s", msg, cname);
				return -1;
			}
			mvc_create_column(sql, t, cname, tp);
		}
		if (n || m) {
			sql_error(sql, 01, SQLSTATE(21S02) "%s: number of columns does not match", msg);
			return -1;
		}
	} else {
		node *m;

		for (m = r->exps->h; m; m = m->next) {
			sql_exp *e = m->data;
			const char *cname = exp_name(e);
			sql_subtype *tp = exp_subtype(e);

			if (tt != tt_view && cname && cname[0] == '%') {
				sql_error(sql, 01, SQLSTATE(42000) "%s: generated labels not allowed in column names, use an alias instead", msg);
				return -1;
			}
			if (!cname)
				cname = "v";
			if (mvc_bind_column(sql, t, cname)) {
				sql_error(sql, 01, SQLSTATE(42S21) "%s: duplicate column name %s", msg, cname);
				return -1;
			}
			mvc_create_column(sql, t, cname, tp);
		}
	}
	return 0;
}

sql_table *
mvc_create_table_as_subquery(mvc *sql, sql_rel *sq, sql_schema *s, const char *tname, dlist *column_spec, int temp, int commit_action, const char *action)
{
	table_types tt =(temp == SQL_REMOTE)?tt_remote:
		(temp == SQL_STREAM)?tt_stream:
		(temp == SQL_MERGE_TABLE)?tt_merge_table:
		(temp == SQL_REPLICA_TABLE)?tt_replica_table:tt_table;

	sql_table *t = mvc_create_table(sql, s, tname, tt, 0, SQL_DECLARED_TABLE, commit_action, -1, 0);
	if (as_subquery(sql, t, tt, sq, column_spec, action) != 0)
		return NULL;
	return t;
}

static char *
table_constraint_name(mvc *sql, symbol *s, sql_table *t)
{
	/* create a descriptive name like table_col_pkey */
	char *suffix;		/* stores the type of this constraint */
	dnode *nms = NULL;
	char *buf;
	size_t buflen, len, slen;

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
		case SQL_CHECK:
			suffix = "_check";
			nms = s->data.lval->h;	/* list of check constraint conditions */
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
	buf = SA_NEW_ARRAY(sql->ta, char, buflen);
	strcpy(buf, t->base.name);

	/* add column name(s) */
	for (; nms; nms = nms->next) {
		slen = strlen(nms->data.sval);
		while (len + slen + 1 >= buflen) {
			size_t nbuflen = buflen + BUFSIZ;
			char *nbuf = SA_RENEW_ARRAY(sql->ta, char, buf, nbuflen, buflen);
			buf = nbuf;
			buflen = nbuflen;
		}
		snprintf(buf + len, buflen - len, "_%s", nms->data.sval);
		len += slen + 1;
	}

	/* add suffix */
	slen = strlen(suffix);
	while (len + slen >= buflen) {
		size_t nbuflen = buflen + BUFSIZ;
		char *nbuf = SA_RENEW_ARRAY(sql->ta, char, buf, nbuflen, buflen);
		buf = nbuf;
		buflen = nbuflen;
	}
	snprintf(buf + len, buflen - len, "%s", suffix);
	return buf;
}

static char *
column_constraint_name(mvc *sql, symbol *s, sql_column *sc, sql_table *t)
{
	/* create a descriptive name like table_col_pkey */
	char *suffix /* stores the type of this constraint */, *buf;
	size_t buflen;

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
		case SQL_CHECK:
			suffix = "check";
			break;
		default:
			suffix = "?";
	}

	buflen = strlen(t->base.name) + strlen(sc->base.name) + strlen(suffix) + 3;
	buf = SA_NEW_ARRAY(sql->ta, char, buflen);
	snprintf(buf, buflen, "%s_%s_%s", t->base.name, sc->base.name, suffix);
	return buf;
}

#define COL_NULL	0
#define COL_DEFAULT 1

static bool
foreign_key_check_types(sql_subtype *lt, sql_subtype *rt)
{
	if (lt->type->eclass == EC_EXTERNAL && rt->type->eclass == EC_EXTERNAL)
		return lt->type->localtype == rt->type->localtype;
	return lt->type->eclass == rt->type->eclass || (EC_VARCHAR(lt->type->eclass) && EC_VARCHAR(rt->type->eclass));
}

static int
column_constraint_type(mvc *sql, const char *name, symbol *s, sql_schema *ss, sql_table *t, sql_column *cs, bool isDeclared, int *used)
{
	int res = SQL_ERR;

	if (isDeclared && (s->token != SQL_NULL && s->token != SQL_NOT_NULL)) {
		(void) sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT: constraints on declared tables are not supported\n");
		return res;
	}
	switch (s->token) {
	case SQL_UNIQUE:
	case SQL_PRIMARY_KEY: {
		key_type kt = (s->token == SQL_UNIQUE) ? ukey : pkey;
		sql_key *k;

		if (kt == pkey && t->pkey) {
			(void) sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT PRIMARY KEY: a table can have only one PRIMARY KEY\n");
			return res;
		}
		if (name && (list_find_name(t->keys.set, name) || mvc_bind_key(sql, ss, name))) {
			(void) sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT %s: key %s already exists", (kt == pkey) ? "PRIMARY KEY" : "UNIQUE", name);
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
		char *rtname = qname_schema_object(n->data.lval);
		int ref_actions = n->next->next->next->data.i_val;
		sql_schema *rs = cur_schema(sql);
		sql_table *rt;
		sql_fkey *fk;
		list *cols;
		sql_key *rk = NULL;
		sql_kc *kc;

		assert(n->next->next->next->type == type_int);
/*
		if (isTempTable(t)) {
			(void) sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT: constraints on temporary tables are not supported\n");
			return res;
		}
*/

		if (rsname && !(rs = mvc_bind_schema(sql, rsname))) {
			(void) sql_error(sql, 02, SQLSTATE(3F000) "CONSTRAINT FOREIGN KEY: no such schema '%s'", rsname);
			return res;
		}
		if (!(rt = _bind_table(t, ss, rs, rtname))) {
			(void) sql_error(sql, 02, SQLSTATE(42S02) "CONSTRAINT FOREIGN KEY: no such table '%s'\n", rtname);
			return res;
		}
		if (name && (list_find_name(t->keys.set, name) || mvc_bind_key(sql, ss, name))) {
			(void) sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT FOREIGN KEY: key '%s' already exists", name);
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
			(void) sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT FOREIGN KEY: could not find referenced PRIMARY KEY in table %s.%s\n", rsname, rtname);
			return res;
		}
		if (list_length(rk->columns) != 1) {
			(void) sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT FOREIGN KEY: not all columns are handled\n");
			return res;
		}
		kc = rk->columns->h->data;
		if (!foreign_key_check_types(&cs->type, &kc->c->type)) {
			str tp1 = sql_subtype_string(sql->ta, &cs->type), tp2 = sql_subtype_string(sql->ta, &kc->c->type);

			if (!tp1 || !tp2)
				(void) sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			else
				(void) sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT FOREIGN KEY: the type of the FOREIGN KEY column '%s' %s is not compatible with the referenced %s KEY column type %s\n",
								 cs->base.name, tp1, rk->type == pkey ? "PRIMARY" : "UNIQUE", tp2);
			return res;
		}
		fk = mvc_create_fkey(sql, t, name, fkey, rk, ref_actions & 255, (ref_actions>>8) & 255);
		mvc_create_fkc(sql, fk, cs);
		res = SQL_OK;
	} 	break;
	case SQL_NOT_NULL:
	case SQL_NULL: {
		int null = (s->token != SQL_NOT_NULL);

		if (((*used)&(1<<COL_NULL))) {
			(void) sql_error(sql, 02, SQLSTATE(42000) "NULL constraint for a column may be specified at most once");
			return SQL_ERR;
		}
		*used |= (1<<COL_NULL);

		mvc_null(sql, cs, null);
		res = SQL_OK;
	} 	break;
	case SQL_CHECK: {
		(void) sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT CHECK: check constraints not supported\n");
		return SQL_ERR;
	} 	break;
	default:{
		res = SQL_ERR;
	}
	}
	if (res == SQL_ERR) {
		(void) sql_error(sql, 02, SQLSTATE(M0M03) "Unknown constraint (%p)->token = %s\n", s, token2string(s->token));
	}
	return res;
}

static int
column_options(sql_query *query, dlist *opt_list, sql_schema *ss, sql_table *t, sql_column *cs, bool isDeclared)
{
	mvc *sql = query->sql;
	int res = SQL_OK, used = 0;
	assert(cs);

	if (opt_list) {
		for (dnode *n = opt_list->h; n && res == SQL_OK; n = n->next) {
			symbol *s = n->data.sym;

			switch (s->token) {
				case SQL_CONSTRAINT: {
					dlist *l = s->data.lval;
					char *opt_name = l->h->data.sval, *default_name = NULL;
					symbol *sym = l->h->next->data.sym;

					if (!opt_name && !(default_name = column_constraint_name(sql, sym, cs, t)))
						return SQL_ERR;

					res = column_constraint_type(sql, opt_name ? opt_name : default_name, sym, ss, t, cs, isDeclared, &used);
				} 	break;
				case SQL_DEFAULT: {
					symbol *sym = s->data.sym;
					char *err = NULL, *r;

					if ((used&(1<<COL_DEFAULT))) {
						(void) sql_error(sql, 02, SQLSTATE(42000) "A default value for a column may be specified at most once");
						return SQL_ERR;
					}
					used |= (1<<COL_DEFAULT);

					if (sym->token == SQL_COLUMN || sym->token == SQL_IDENT) {
						exp_kind ek = {type_value, card_value, FALSE};
						sql_exp *e = rel_logical_value_exp(query, NULL, sym, sql_sel, ek);

						if (e && is_atom(e->type)) {
							atom *a = exp_value(sql, e);

							if (atom_null(a)) {
								mvc_default(sql, cs, NULL);
								break;
							}
						}
						/* reset error */
						sql->session->status = 0;
						sql->errstr[0] = '\0';
					}
					r = symbol2string(sql, s->data.sym, 0, &err);
					if (!r) {
						(void) sql_error(sql, 02, SQLSTATE(42000) "Incorrect default value '%s'\n", err?err:"");
						return SQL_ERR;
					} else {
						mvc_default(sql, cs, r);
					}
				} 	break;
				case SQL_NOT_NULL:
				case SQL_NULL: {
					int null = (s->token != SQL_NOT_NULL);

					if ((used&(1<<COL_NULL))) {
						(void) sql_error(sql, 02, SQLSTATE(42000) "NULL constraint for a column may be specified at most once");
						return SQL_ERR;
					}
					used |= (1<<COL_NULL);

					mvc_null(sql, cs, null);
				} 	break;
				default: {
					(void) sql_error(sql, 02, SQLSTATE(M0M03) "Unknown column option (%p)->token = %s\n", s, token2string(s->token));
					return SQL_ERR;
				}
			}
		}
	}
	return res;
}

static int
table_foreign_key(mvc *sql, char *name, symbol *s, sql_schema *ss, sql_table *t)
{
	dnode *n = s->data.lval->h;
	char *rsname = qname_schema(n->data.lval);
	char *rtname = qname_schema_object(n->data.lval);
	sql_schema *fs = ss;
	sql_table *ft = NULL;

	if (rsname && !(fs = mvc_bind_schema(sql, rsname))) {
		(void) sql_error(sql, 02, SQLSTATE(3F000) "CONSTRAINT FOREIGN KEY: no such schema '%s'", rsname);
		return SQL_ERR;
	}
	ft = find_table_on_scope(sql, &fs, rsname, rtname);
	/* self referenced table */
	if (!ft && t->s == fs && strcmp(t->base.name, rtname) == 0)
		ft = t;
	if (!ft) {
		sql_error(sql, 02, SQLSTATE(42S02) "CONSTRAINT FOREIGN KEY: no such table '%s'\n", rtname);
		return SQL_ERR;
	} else {
		sql_key *rk = NULL;
		sql_fkey *fk;
		dnode *nms = n->next->data.lval->h;
		node *fnms;
		int ref_actions = n->next->next->next->next->data.i_val;

		assert(n->next->next->next->next->type == type_int);
		if (name && (list_find_name(t->keys.set, name) || mvc_bind_key(sql, ss, name))) {
			sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT FOREIGN KEY: key '%s' already exists", name);
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
			sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT FOREIGN KEY: could not find referenced PRIMARY KEY in table '%s'\n", ft->base.name);
			return SQL_ERR;
		}
		fk = mvc_create_fkey(sql, t, name, fkey, rk, ref_actions & 255, (ref_actions>>8) & 255);

		for (fnms = rk->columns->h; nms && fnms; nms = nms->next, fnms = fnms->next) {
			char *nm = nms->data.sval;
			sql_column *cs = mvc_bind_column(sql, t, nm);
			sql_kc *kc = fnms->data;

			if (!cs) {
				sql_error(sql, 02, SQLSTATE(42S22) "CONSTRAINT FOREIGN KEY: no such column '%s' in table '%s'\n", nm, t->base.name);
				return SQL_ERR;
			}
			if (!foreign_key_check_types(&cs->type, &kc->c->type)) {
				str tp1 = sql_subtype_string(sql->ta, &cs->type), tp2 = sql_subtype_string(sql->ta, &kc->c->type);

				if (!tp1 || !tp2)
					(void) sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				else
					(void) sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT FOREIGN KEY: the type of the FOREIGN KEY column '%s' %s is not compatible with the referenced %s KEY column type %s\n",
									 cs->base.name, tp1, rk->type == pkey ? "PRIMARY" : "UNIQUE", tp2);
				return SQL_ERR;
			}
			mvc_create_fkc(sql, fk, cs);
		}
		if (nms || fnms) {
			sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT FOREIGN KEY: not all columns are handled\n");
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
			sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT PRIMARY KEY: a table can have only one PRIMARY KEY\n");
			return SQL_ERR;
		}
		if (name && (list_find_name(t->keys.set, name) || mvc_bind_key(sql, ss, name))) {
			sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT %s: key '%s' already exists",
					kt == pkey ? "PRIMARY KEY" : "UNIQUE", name);
			return SQL_ERR;
		}

		k = (sql_key*)mvc_create_ukey(sql, t, name, kt);
		for (; nms; nms = nms->next) {
			char *nm = nms->data.sval;
			sql_column *c = mvc_bind_column(sql, t, nm);

			if (!c) {
				sql_error(sql, 02, SQLSTATE(42S22) "CONSTRAINT %s: no such column '%s' for table '%s'",
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
	case SQL_CHECK: {
		(void) sql_error(sql, 02, SQLSTATE(42000) "CONSTRAINT CHECK: check constraints not supported\n");
		return SQL_ERR;
	} 	break;
	default:
		res = SQL_ERR;
	}
	if (res != SQL_OK) {
		sql_error(sql, 02, SQLSTATE(M0M03) "Table constraint type: wrong token (%p) = %s\n", s, token2string(s->token));
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
			opt_name = table_constraint_name(sql, sym, t);
		if (opt_name == NULL)
			return SQL_ERR;
		res = table_constraint_type(sql, opt_name, sym, ss, t);
	}

	if (res != SQL_OK) {
		sql_error(sql, 02, SQLSTATE(M0M03) "Table constraint: wrong token (%p) = %s\n", s, token2string(s->token));
		return SQL_ERR;
	}
	return res;
}

static int
create_column(sql_query *query, symbol *s, sql_schema *ss, sql_table *t, int alter, bool isDeclared)
{
	mvc *sql = query->sql;
	dlist *l = s->data.lval;
	char *cname = l->h->data.sval;
	sql_subtype *ctype = &l->h->next->data.typeval;
	dlist *opt_list = NULL;
	int res = SQL_OK;

	(void) ss;
	if (alter && !(isTable(t) || (isMergeTable(t) && list_empty(t->members)))) {
		sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: cannot add column to %s '%s'%s\n",
				  isMergeTable(t)?"MERGE TABLE":
				  isRemote(t)?"REMOTE TABLE":
				  isStream(t)?"STREAM TABLE":
				  isReplicaTable(t)?"REPLICA TABLE":"VIEW",
				  t->base.name, (isMergeTable(t) && !list_empty(t->members)) ? " while it has partitions" : "");
		return SQL_ERR;
	}
	if (l->h->next->next)
		opt_list = l->h->next->next->data.lval;

	if (cname && ctype) {
		sql_column *cs = NULL;

		if (!isView(t) && cname && cname[0] == '%') {
			sql_error(sql, 01, SQLSTATE(42000) "%s TABLE: generated labels not allowed in column names, use an alias instead", (alter)?"ALTER":"CREATE");
			return SQL_ERR;
		} else if ((cs = find_sql_column(t, cname))) {
			sql_error(sql, 02, SQLSTATE(42S21) "%s TABLE: a column named '%s' already exists\n", (alter)?"ALTER":"CREATE", cname);
			return SQL_ERR;
		}
		cs = mvc_create_column(sql, t, cname, ctype);
		if (column_options(query, opt_list, ss, t, cs, isDeclared) == SQL_ERR)
			return SQL_ERR;
	}
	return res;
}

static int
table_element(sql_query *query, symbol *s, sql_schema *ss, sql_table *t, int alter, bool isDeclared, const char *action)
{
	mvc *sql = query->sql;
	int res = SQL_OK;

	if (alter &&
		(isView(t) ||
		((isMergeTable(t) || isReplicaTable(t)) && (s->token != SQL_TABLE && s->token != SQL_DROP_TABLE && !list_empty(t->members))) ||
		(isTable(t) && (s->token == SQL_TABLE || s->token == SQL_DROP_TABLE)) ||
		(isPartition(t) && (s->token == SQL_DROP_COLUMN || s->token == SQL_COLUMN || s->token == SQL_CONSTRAINT)) ||
		(isPartition(t) &&
		(s->token == SQL_DEFAULT || s->token == SQL_DROP_DEFAULT || s->token == SQL_NOT_NULL || s->token == SQL_NULL ||
		 s->token == SQL_DROP_CONSTRAINT)))){
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
		default:
			sql_error(sql, 02, SQLSTATE(M0M03) "%s: Unknown table element (%p)->token = %s\n", action, s, token2string(s->token));
			return SQL_ERR;
		}
		sql_error(sql, 02, SQLSTATE(42000) "%s: cannot %s %s '%s'%s\n",
				action,
				msg,
				isPartition(t)?"a PARTITION of a MERGE or REPLICA TABLE":
				isMergeTable(t)?"MERGE TABLE":
				isRemote(t)?"REMOTE TABLE":
				isStream(t)?"STREAM TABLE":
				isReplicaTable(t)?"REPLICA TABLE":"VIEW",
				t->base.name, (isMergeTable(t) && !list_empty(t->members)) ? " while it has partitions" : "");
		return SQL_ERR;
	}

	switch (s->token) {
	case SQL_COLUMN:
		res = create_column(query, s, ss, t, alter, isDeclared);
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
			sql_error(sql, 02, SQLSTATE(42S22) "%s: no such column '%s'\n", action, cname);
			return SQL_ERR;
		} else {
			return column_options(query, olist, ss, t, c, isDeclared);
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
			sql_error(sql, 02, SQLSTATE(42S22) "%s: no such column '%s'\n", action, cname);
			return SQL_ERR;
		}
		r = symbol2string(sql, sym, 0, &err);
		if (!r) {
			(void) sql_error(sql, 02, SQLSTATE(42000) "%s: incorrect default value '%s'\n", action, err?err:"");
			return SQL_ERR;
		}
		mvc_default(sql, c, r);
	}
	break;
	case SQL_STORAGE:
	{
		dlist *l = s->data.lval;
		char *cname = l->h->data.sval;
		char *storage_type = l->h->next->data.sval;
		sql_column *c = mvc_bind_column(sql, t, cname);

		if (!c) {
			sql_error(sql, 02, SQLSTATE(42S22) "%s: no such column '%s'\n", action, cname);
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
			sql_error(sql, 02, SQLSTATE(42S22) "%s: no such column '%s'\n", action, cname);
			return SQL_ERR;
		}
		mvc_null(sql, c, null);
	} 	break;
	case SQL_DROP_DEFAULT:
	{
		char *cname = s->data.sval;
		sql_column *c = mvc_bind_column(sql, t, cname);
		if (!c) {
			sql_error(sql, 02, SQLSTATE(42S22) "%s: no such column '%s'\n", action, cname);
			return SQL_ERR;
		}
		mvc_drop_default(sql,c);
	} 	break;
	case SQL_LIKE:
	{
		char *sname = qname_schema(s->data.lval);
		char *name = qname_schema_object(s->data.lval);
		sql_schema *os = ss;
		sql_table *ot = NULL;
		node *n;

		if (sname && !(os = mvc_bind_schema(sql, sname))) {
			sql_error(sql, 02, SQLSTATE(3F000) "%s: no such schema '%s'", action, sname);
			return SQL_ERR;
		}
		if (!(ot = find_table_on_scope(sql, &os, sname, name))) {
			sql_error(sql, 02, SQLSTATE(3F000) "%s: no such table '%s'", action, name);
			return SQL_ERR;
		}
		for (n = ot->columns.set->h; n; n = n->next) {
			sql_column *oc = n->data;

			if (!isView(t) && oc->base.name && oc->base.name[0] == '%') {
				sql_error(sql, 02, SQLSTATE(42000) "%s: generated labels not allowed in column names, use an alias instead", action);
				return SQL_ERR;
			} else if (mvc_bind_column(sql, t, oc->base.name)) {
				sql_error(sql, 02, SQLSTATE(42S21) "%s: a column named '%s' already exists\n", action, oc->base.name);
				return SQL_ERR;
			}
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
			sql_error(sql, 02, SQLSTATE(42S22) "%s: no such column '%s'\n", action, cname);
			return SQL_ERR;
		}
		if (cs_size(&t->columns) <= 1) {
			sql_error(sql, 02, SQLSTATE(42000) "%s: cannot drop column '%s': table needs at least one column\n", action, cname);
			return SQL_ERR;
		}
		if (t->system) {
			sql_error(sql, 02, SQLSTATE(42000) "%s: cannot drop column '%s': table is a system table\n", action, cname);
			return SQL_ERR;
		}
		if (isView(t)) {
			sql_error(sql, 02, SQLSTATE(42000) "%s: cannot drop column '%s': '%s' is a view\n", action, cname, t->base.name);
			return SQL_ERR;
		}
		if (!drop_action && mvc_check_dependency(sql, col->base.id, COLUMN_DEPENDENCY, NULL)) {
			sql_error(sql, 02, SQLSTATE(2BM37) "%s: cannot drop column '%s': there are database objects which depend on it\n", action, cname);
			return SQL_ERR;
		}
		if (!drop_action  && t->keys.set) {
			node *n, *m;

			for (n = t->keys.set->h; n; n = n->next) {
				sql_key *k = n->data;
				for (m = k->columns->h; m; m = m->next) {
					sql_kc *kc = m->data;
					if (strcmp(kc->c->base.name, cname) == 0) {
						sql_error(sql, 02, SQLSTATE(2BM37) "%s: cannot drop column '%s': there are constraints which depend on it\n", action, cname);
						return SQL_ERR;
					}
				}
			}
		}
		if (isPartitionedByColumnTable(t) && t->part.pcol->base.id == col->base.id) {
			sql_error(sql, 02, SQLSTATE(42000) "%s: cannot drop column '%s': is the partitioned column on the table '%s'\n", action, cname, t->base.name);
			return SQL_ERR;
		}
		if (isPartitionedByExpressionTable(t)) {
			for (node *n = t->part.pexp->cols->h; n; n = n->next) {
				int next = *(int*) n->data;
				if (next == col->colnr) {
					sql_error(sql, 02, SQLSTATE(42000) "%s: cannot drop column '%s': the expression used in '%s' depends on it\n", action, cname, t->base.name);
					return SQL_ERR;
				}
			}
		}
		if (mvc_drop_column(sql, t, col, drop_action)) {
			sql_error(sql, 02, SQLSTATE(42000) "%s: %s\n", action, MAL_MALLOC_FAIL);
			return SQL_ERR;
		}
	} 	break;
	case SQL_DROP_CONSTRAINT:
		res = SQL_OK;
		break;
	default:
		res = SQL_ERR;
	}
	if (res == SQL_ERR) {
		sql_error(sql, 02, SQLSTATE(M0M03) "%s: Unknown table element (%p)->token = %s\n", action, s, token2string(s->token));
		return SQL_ERR;
	}
	return res;
}

static int
create_partition_definition(mvc *sql, sql_table *t, symbol *partition_def)
{
	char *err = NULL;

	if (partition_def) {
		dlist *list = partition_def->data.lval;
		symbol *type = list->h->next->data.sym;
		dlist *list2 = type->data.lval;
		if (isPartitionedByColumnTable(t)) {
			str colname = list2->h->data.sval;
			node *n;
			sql_class sql_ec;
			for (n = t->columns.set->h; n ; n = n->next) {
				sql_column *col = n->data;
				if (!strcmp(col->base.name, colname)) {
					t->part.pcol = col;
					break;
				}
			}
			if (!t->part.pcol) {
				sql_error(sql, 02, SQLSTATE(42000) "CREATE MERGE TABLE: the partition column '%s' is not part of the table", colname);
				return SQL_ERR;
			}
			sql_ec = t->part.pcol->type.type->eclass;
			if (!(sql_ec == EC_BIT || EC_VARCHAR(sql_ec) || EC_TEMP(sql_ec) || sql_ec == EC_POS || sql_ec == EC_NUM ||
				 EC_INTERVAL(sql_ec)|| sql_ec == EC_DEC || sql_ec == EC_BLOB)) {
				err = sql_subtype_string(sql->ta, &(t->part.pcol->type));
				if (!err) {
					sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				} else {
					sql_error(sql, 02, SQLSTATE(42000) "CREATE MERGE TABLE: column type %s not yet supported for the partition column", err);
				}
				return SQL_ERR;
			}
		} else if (isPartitionedByExpressionTable(t)) {
			char *query = symbol2string(sql, list2->h->data.sym, 1, &err);
			if (!query) {
				(void) sql_error(sql, 02, SQLSTATE(42000) "CREATE MERGE TABLE: error compiling expression '%s'", err?err:"");
				return SQL_ERR;
			}
			t->part.pexp = SA_ZNEW(sql->sa, sql_expression);
			t->part.pexp->exp = query;
			t->part.pexp->type = *sql_bind_localtype("void");
		}
	}
	return SQL_OK;
}

sql_rel *
rel_create_table(sql_query *query, int temp, const char *sname, const char *name, bool global, symbol *table_elements_or_subquery,
				 int commit_action, const char *loc, const char *username, const char *password, bool pw_encrypted,
				 symbol* partition_def, int if_not_exists)
{
	mvc *sql = query->sql;
	int tt = (temp == SQL_REMOTE)?tt_remote:
		 (temp == SQL_STREAM)?tt_stream:
		 (temp == SQL_MERGE_TABLE)?tt_merge_table:
		 (temp == SQL_REPLICA_TABLE)?tt_replica_table:tt_table;
	bit properties = partition_def ? (bit) partition_def->data.lval->h->next->next->data.i_val : 0;
	sql_table *t = NULL;
	const char *action = (temp == SQL_DECLARED_TABLE)?"DECLARE":"CREATE";
	sql_schema *s = cur_schema(sql);

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, SQLSTATE(3F000) "%s TABLE: no such schema '%s'", action, sname);

	if ((temp != SQL_PERSIST && tt == tt_table && commit_action == CA_COMMIT) || temp == SQL_DECLARE)
		commit_action = CA_DELETE;

	if (temp != SQL_DECLARED_TABLE) {
		if (temp != SQL_PERSIST && tt == tt_table) {
			if (temp == SQL_LOCAL_TEMP || temp == SQL_GLOBAL_TEMP) {
				if (sname && strcmp(sname, "tmp") != 0)
					return sql_error(sql, 02, SQLSTATE(3F000) "%s TABLE: %s temporary tables should be stored in the 'tmp' schema",
									 action, (temp == SQL_LOCAL_TEMP) ? "local" : "global");
				s = mvc_bind_schema(sql, "tmp");
			}
		}
	}

	if (global && mvc_bind_table(sql, s, name)) {
		if (if_not_exists)
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		return sql_error(sql, 02, SQLSTATE(42S01) "%s TABLE: name '%s' already in use", action, name);
	} else if (!global && frame_find_table(sql, name)) {
		assert(temp == SQL_DECLARED_TABLE);
		if (if_not_exists)
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		return sql_error(sql, 02, SQLSTATE(42S01) "%s TABLE: name '%s' already declared", action, name);
	} else if (temp != SQL_DECLARED_TABLE && (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && temp == SQL_LOCAL_TEMP))){
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE TABLE: insufficient privileges for user '%s' in schema '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), s->base.name);
	} else if (temp == SQL_PERSIST && isTempSchema(s)){
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE TABLE: cannot create persistent table '%s' in the schema '%s'", name, s->base.name);
	} else if (table_elements_or_subquery->token == SQL_CREATE_TABLE) {
		/* table element list */
		dnode *n;
		dlist *columns = table_elements_or_subquery->data.lval;

		if (tt == tt_remote) {
			char *local_user = sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user"));
			char *local_table = sa_strconcat(sql->sa, sa_strconcat(sql->sa, s->base.name, "."), name);
			if (!local_table) {
				return sql_error(sql, 02, SQLSTATE(HY013) "%s TABLE: " MAL_MALLOC_FAIL, action);
			}
			if (!mapiuri_valid(loc))
				return sql_error(sql, 02, SQLSTATE(42000) "%s TABLE: incorrect uri '%s' for remote table '%s'", action, loc, name);

			const char *remote_uri = mapiuri_uri(loc, sql->sa);
			if (remote_uri == NULL) {
				return sql_error(sql, 02, SQLSTATE(HY013) "%s TABLE: " MAL_MALLOC_FAIL, action);
			}
			char *reg_credentials = AUTHaddRemoteTableCredentials(local_table, local_user, remote_uri, username, password, pw_encrypted);
			if (reg_credentials != 0) {
				return sql_error(sql, 02, SQLSTATE(42000) "%s TABLE: cannot register credentials for remote table '%s' in vault: %s", action, name, reg_credentials);
			}
			t = mvc_create_remote(sql, s, name, SQL_DECLARED_TABLE, loc);
		} else {
			t = mvc_create_table(sql, s, name, tt, 0, SQL_DECLARED_TABLE, commit_action, -1, properties);
		}
		if (!t)
			return NULL;

		for (n = columns->h; n; n = n->next) {
			symbol *sym = n->data.sym;
			int res = table_element(query, sym, s, t, 0, (temp == SQL_DECLARED_TABLE), (temp == SQL_DECLARED_TABLE)?"DECLARE TABLE":"CREATE TABLE");

			if (res == SQL_ERR)
				return NULL;
		}

		if (create_partition_definition(sql, t, partition_def) != SQL_OK)
			return NULL;

		temp = (tt == tt_table)?temp:SQL_PERSIST;
		return rel_table(sql, ddl_create_table, s->base.name, t, temp);
	} else { /* [col name list] as subquery with or without data */
		sql_rel *sq = NULL, *res = NULL;
		dlist *as_sq = table_elements_or_subquery->data.lval;
		dlist *column_spec = as_sq->h->data.lval;
		symbol *subquery = as_sq->h->next->data.sym;
		int with_data = as_sq->h->next->next->data.i_val;

		assert(as_sq->h->next->next->type == type_int);
		sq = rel_selects(query, subquery);
		if (!sq)
			return NULL;

		if ((tt == tt_merge_table || tt == tt_remote || tt == tt_replica_table) && with_data)
			return sql_error(sql, 02, SQLSTATE(42000) "%s TABLE: cannot create %s 'with data'", action,
							 TABLE_TYPE_DESCRIPTION(tt, properties));

		/* create table */
		if ((t = mvc_create_table_as_subquery(sql, sq, s, name, column_spec, temp, commit_action, (temp == SQL_DECLARED_TABLE)?"DECLARE TABLE":"CREATE TABLE")) == NULL) {
			rel_destroy(sq);
			return NULL;
		}

		/* insert query result into this table */
		temp = (tt == tt_table)?temp:SQL_PERSIST;
		res = rel_table(sql, ddl_create_table, s->base.name, t, temp);
		if (with_data) {
			res = rel_insert(query->sql, res, sq);
		} else {
			rel_destroy(sq);
		}
		return res;
	}
	/*return NULL;*/ /* never reached as all branches of the above if () end with return ... */
}

static sql_rel *
rel_create_view(sql_query *query, sql_schema *ss, dlist *qname, dlist *column_spec, symbol *ast, int check, int persistent, int replace)
{
	mvc *sql = query->sql;
	const char *name = qname_schema_object(qname);
	const char *sname = qname_schema(qname);
	sql_schema *s = cur_schema(sql);
	sql_table *t = NULL;
	int instantiate = (sql->emode == m_instantiate || !persistent);
	int deps = (sql->emode == m_deps);
	int create = (!instantiate && !deps);
	const char *base = replace ? "CREATE OR REPLACE" : "CREATE";

	(void) ss;
	(void) check;		/* Stefan: unused!? */
	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, SQLSTATE(3F000) "CREATE VIEW: no such schema '%s'", sname);

	if (create && (!mvc_schema_privs(sql, s) && !(isTempSchema(s) && persistent == SQL_LOCAL_TEMP)))
		return sql_error(sql, 02, SQLSTATE(42000) "%s VIEW: access denied for %s to schema '%s'", base, sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), s->base.name);

	if (create) {
		if ((t = find_table_on_scope(sql, &s, sname, name))) {
			if (replace) {
				if (!isView(t)) {
					return sql_error(sql, 02, SQLSTATE(42000) "%s VIEW: unable to drop view '%s': is a table", base, name);
				} else if (t->system) {
					return sql_error(sql, 02, SQLSTATE(42000) "%s VIEW: cannot replace system view '%s'", base, name);
				} else if (mvc_check_dependency(sql, t->base.id, VIEW_DEPENDENCY, NULL)) {
					return sql_error(sql, 02, SQLSTATE(42000) "%s VIEW: cannot replace view '%s', there are database objects which depend on it", base, t->base.name);
				} else {
					str output;
					if ((output = mvc_drop_table(sql, s, t, 0)) != MAL_SUCCEED) {
						sql_error(sql, 02, SQLSTATE(42000) "%s", output);
						freeException(output);
						return NULL;
					}
				}
			} else {
				return sql_error(sql, 02, SQLSTATE(42S01) "%s VIEW: name '%s' already in use", base, name);
			}
		}
	}
	if (ast) {
		sql_rel *sq = NULL;
		char *q = QUERY(sql->scanner);

		if (ast->token == SQL_SELECT) {
			SelectNode *sn = (SelectNode *) ast;

			if (sn->limit || sn->sample)
				return sql_error(sql, 01, SQLSTATE(42000) "%s VIEW: %s not supported", base, sn->limit ? "LIMIT" : "SAMPLE");
		}

		sq = schema_selects(query, s, ast);
		if (!sq)
			return NULL;

		if (!create) {
			if (column_spec) {
				dnode *n = column_spec->h;
				node *m = sq->exps->h;

				for (; n && m; n = n->next, m = m->next)
					;
				if (n || m) {
					sql_error(sql, 01, SQLSTATE(21S02) "WITH CLAUSE: number of columns does not match");
					rel_destroy(sq);
					return NULL;
				}
			}
		}

		if (create) {
			q = query_cleaned(sql->ta, q);
			t = mvc_create_view(sql, s, name, SQL_DECLARED_TABLE, q, 0);
			if (as_subquery(sql, t, tt_view, sq, column_spec, "CREATE VIEW") != 0) {
				rel_destroy(sq);
				return NULL;
			}
			return rel_table(sql, ddl_create_view, s->base.name, t, SQL_PERSIST);
		}
		t = find_table_on_scope(sql, &s, sname, name);
		if (!persistent && column_spec)
			sq = view_rename_columns(sql, name, sq, column_spec);
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
	if (!rel || !exps)
		return NULL;

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
	if (!rel || !exps)
		return NULL;

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
	char *name = qname_schema_object(qname);
	char *sname = qname_schema(qname);
	sql_schema *s = cur_schema(sql);

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, SQLSTATE(3F000) "DROP TYPE: no such schema '%s'", sname);

	if (schema_bind_type(sql, s, name) == NULL) {
		return sql_error(sql, 02, SQLSTATE(42S01) "DROP TYPE: type '%s' does not exist", name);
	} else if (!mvc_schema_privs(sql, s)) {
		return sql_error(sql, 02, SQLSTATE(42000) "DROP TYPE: access denied for %s to schema '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), s->base.name);
	}
	return rel_schema2(sql->sa, ddl_drop_type, s->base.name, name, drop_action);
}

static sql_rel *
rel_create_type(mvc *sql, dlist *qname, char *impl)
{
	char *name = qname_schema_object(qname);
	char *sname = qname_schema(qname);
	sql_schema *s = cur_schema(sql);

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, SQLSTATE(3F000) "CREATE TYPE: no such schema '%s'", sname);

	if (schema_bind_type(sql, s, name) != NULL) {
		return sql_error(sql, 02, SQLSTATE(42S01) "CREATE TYPE: name '%s' already in use", name);
	} else if (!mvc_schema_privs(sql, s)) {
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE TYPE: access denied for %s to schema '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), s->base.name);
	}
	return rel_schema3(sql->sa, ddl_create_type, s->base.name, name, impl);
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
rel_drop(sql_allocator *sa, int cat_type, char *sname, char *first_val, char *second_val, int nr, int exists_check)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	append(exps, exp_atom_int(sa, nr));
	append(exps, exp_atom_clob(sa, sname));
	if (first_val)
		append(exps, exp_atom_clob(sa, first_val));
	if (second_val)
		append(exps, exp_atom_clob(sa, second_val));
	append(exps, exp_atom_int(sa, exists_check));
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
rel_create_schema_dll(sql_allocator *sa, char *sname, char *auth, int nr)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);
	if (!rel || !exps)
		return NULL;

	append(exps, exp_atom_int(sa, nr));
	append(exps, exp_atom_clob(sa, sname));
	if (auth)
		append(exps, exp_atom_clob(sa, auth));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = ddl_create_schema;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_create_schema(sql_query *query, dlist *auth_name, dlist *schema_elements, int if_not_exists)
{
	mvc *sql = query->sql;
	char *name = dlist_get_schema_name(auth_name);
	char *auth = schema_auth(auth_name);
	sqlid auth_id = sql->role_id;

	if (auth && (auth_id = sql_find_auth(sql, auth)) < 0)
		return sql_error(sql, 02, SQLSTATE(28000) "CREATE SCHEMA: no such authorization '%s'", auth);
	if (sql->user_id != USER_MONETDB && sql->role_id != ROLE_SYSADMIN)
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE SCHEMA: insufficient privileges for user '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")));
	if (!name)
		name = auth;
	assert(name);
	if (mvc_bind_schema(sql, name)) {
		if (!if_not_exists)
			return sql_error(sql, 02, SQLSTATE(3F000) "CREATE SCHEMA: name '%s' already in use", name);
		return rel_psm_block(sql->sa, new_exp_list(sql->sa));
	} else {
		sql_schema *os = cur_schema(sql);
		dnode *n = schema_elements->h;
		sql_schema *ss = SA_ZNEW(sql->sa, sql_schema);
		sql_rel *ret = rel_create_schema_dll(sql->sa, name, auth, 0);

		ss->base.name = name;
		ss->auth_id = auth_id;
		ss->owner = sql->user_id;

		sql->session->schema = ss;
		while (n) {
			sql_rel *res = rel_semantic(query, n->data.sym);
			if (!res) {
				rel_destroy(ret);
				sql->session->schema = os;
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
get_schema_name(mvc *sql, char *sname, char *tname)
{
	if (!sname) {
		sql_schema *s = cur_schema(sql);
		sql_table *t = find_table_on_scope(sql, &s, sname, tname);

		if (t && t->s)
			return t->s->base.name;
		return s->base.name;
	}
	return sname;
}

static sql_rel *
sql_alter_table(sql_query *query, dlist *dl, dlist *qname, symbol *te, int if_exists)
{
	mvc *sql = query->sql;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_schema *s = cur_schema(sql);
	sql_table *t = NULL, *nt = NULL;
	sql_rel *res = NULL, *r;
	sql_exp **updates, *e;

	if (sname && !(s = mvc_bind_schema(sql, sname))) {
		if (if_exists)
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER TABLE: no such schema '%s'", sname);
	}
	if (!mvc_schema_privs(sql, s))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), s->base.name);

	if (!(t = find_table_on_scope(sql, &s, sname, tname))) {
		if (if_exists)
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", tname, s->base.name);
	}
	if (isDeclaredTable(t))
		return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: can't alter declared table '%s'", tname);
	if (isTempSchema(t->s))
		return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: can't alter temporary table '%s'", tname);

	assert(te);
	if (t->persistence != SQL_DECLARED_TABLE)
		sname = s->base.name;

	if ((te->token == SQL_TABLE || te->token == SQL_DROP_TABLE)) {
		dlist *nqname = te->data.lval->h->data.lval;
		sql_schema *spt = NULL;
		sql_table *pt = NULL;
		char *nsname = qname_schema(nqname);
		char *ntname = qname_schema_object(nqname);

		/* partition sname */
		if (!nsname)
			nsname = sname;

		if (nsname && !(spt = mvc_bind_schema(sql, nsname)))
			return sql_error(sql, 02, SQLSTATE(3F000) "ALTER TABLE: no such schema '%s'", sname);

		if (!(pt = find_table_on_scope(sql, &spt, nsname, ntname)))
			return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", ntname, spt->base.name);
		if (isView(pt))
			return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: can't add/drop a view into a %s",
								TABLE_TYPE_DESCRIPTION(t->type, t->properties));
		if (isDeclaredTable(pt))
			return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: can't add/drop a declared table into a %s",
								TABLE_TYPE_DESCRIPTION(t->type, t->properties));
		if (isTempSchema(pt->s))
			return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: can't add/drop a temporary table into a %s",
								TABLE_TYPE_DESCRIPTION(t->type, t->properties));
		if (strcmp(sname, nsname) != 0)
			return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: all children tables of '%s.%s' must be "
								"part of schema '%s'", sname, tname, sname);

		if (te->token == SQL_TABLE) {
			symbol *extra = dl->h->next->next->next->data.sym;

			if (!extra) {
				if (isRangePartitionTable(t)) {
					return sql_error(sql, 02,SQLSTATE(42000) "ALTER TABLE: a range partition is required while adding under a %s",
									 TABLE_TYPE_DESCRIPTION(t->type, t->properties));
				} else if (isListPartitionTable(t)) {
					return sql_error(sql, 02,SQLSTATE(42000) "ALTER TABLE: a value partition is required while adding under a %s",
									 TABLE_TYPE_DESCRIPTION(t->type, t->properties));
				}
				return rel_alter_table(sql->sa, ddl_alter_table_add_table, sname, tname, nsname, ntname, 0);
			}
			if ((isMergeTable(pt) || isReplicaTable(pt)) && list_empty(pt->members))
				return sql_error(sql, 02, SQLSTATE(42000) "The %s %s.%s should have at least one table associated",
								 TABLE_TYPE_DESCRIPTION(pt->type, pt->properties), spt->base.name, pt->base.name);

			if (extra->token == SQL_MERGE_PARTITION) { /* partition to hold null values only */
				dlist* ll = extra->data.lval;
				int update = ll->h->next->next->next->data.i_val;

				if (isRangePartitionTable(t)) {
					return rel_alter_table_add_partition_range(query, t, pt, sname, tname, nsname, ntname, NULL, NULL, true, update);
				} else if (isListPartitionTable(t)) {
					return rel_alter_table_add_partition_list(query, t, pt, sname, tname, nsname, ntname, NULL, true, update);
				} else {
					return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: cannot add a partition into a %s",
									 TABLE_TYPE_DESCRIPTION(t->type, t->properties));
				}
			} else if (extra->token == SQL_PARTITION_RANGE) {
				dlist* ll = extra->data.lval;
				symbol* min = ll->h->data.sym, *max = ll->h->next->data.sym;
				int nills = ll->h->next->next->data.i_val, update = ll->h->next->next->next->data.i_val;

				if (!isRangePartitionTable(t)) {
					return sql_error(sql, 02,SQLSTATE(42000) "ALTER TABLE: cannot add a range partition into a %s",
									 TABLE_TYPE_DESCRIPTION(t->type, t->properties));
				}

				assert(nills == 0 || nills == 1);
				return rel_alter_table_add_partition_range(query, t, pt, sname, tname, nsname, ntname, min, max, (bit) nills, update);
			} else if (extra->token == SQL_PARTITION_LIST) {
				dlist* ll = extra->data.lval, *values = ll->h->data.lval;
				int nills = ll->h->next->data.i_val, update = ll->h->next->next->data.i_val;

				if (!isListPartitionTable(t)) {
					return sql_error(sql, 02,SQLSTATE(42000) "ALTER TABLE: cannot add a value partition into a %s",
									 TABLE_TYPE_DESCRIPTION(t->type, t->properties));
				}

				assert(nills == 0 || nills == 1);
				return rel_alter_table_add_partition_list(query, t, pt, sname, tname, nsname, ntname, values, (bit) nills, update);
			}
			assert(0);
		} else {
			int drop_action = te->data.lval->h->next->data.i_val;

			return rel_alter_table(sql->sa, ddl_alter_table_del_table, sname, tname, nsname, ntname, drop_action);
		}
	}

	/* read only or read write */
	if (te->token == SQL_ALTER_TABLE) {
		int state = te->data.i_val;

		if (state == tr_readonly)
			state = TABLE_READONLY;
		else if (state == tr_append)
			state = TABLE_APPENDONLY;
		else
			state = TABLE_WRITABLE;
		return rel_alter_table(sql->sa, ddl_alter_table_set_access, sname, tname, NULL, NULL, state);
	}

	nt = dup_sql_table(sql->sa, t);
	if (!nt || (table_element(query, te, s, nt, 1, t->persistence == SQL_DECLARED_TABLE, "ALTER TABLE") == SQL_ERR))
		return NULL;

	if (te->token == SQL_DROP_CONSTRAINT) {
		dlist *l = te->data.lval;
		char *kname = l->h->data.sval;
		int drop_action = l->h->next->data.i_val;

		sname = get_schema_name(sql, sname, tname);
		return rel_drop(sql->sa, ddl_drop_constraint, sname, tname, kname, drop_action, 0);
	}

	res = rel_table(sql, ddl_alter_table, sname, nt, 0);

	if (!isTable(nt))
		return res;

	/* New columns need update with default values. Add one more element for new column */
	updates = SA_ZNEW_ARRAY(sql->sa, sql_exp*, (list_length(nt->columns.set) + 1));
	e = exp_column(sql->sa, nt->base.name, TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
	r = rel_project(sql->sa, res, append(new_exp_list(sql->sa),e));
	if (nt->columns.nelm) {
		list *cols = new_exp_list(sql->sa);
		for (node *n = nt->columns.nelm; n; n = n->next) {
			sql_column *c = n->data;
			if (c->def) {
				e = rel_parse_val(sql, c->def, &c->type, sql->emode, NULL);
			} else {
				e = exp_atom(sql->sa, atom_general(sql->sa, &c->type, NULL));
			}
			if (!e || (e = exp_check_type(sql, &c->type, r, e, type_equal)) == NULL) {
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

static sql_rel *
rel_role(sql_allocator *sa, char *grantee, char *auth, int grantor, int admin, int type)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);
	if (!rel || !exps)
		return NULL;

	assert(type == ddl_grant_roles || type == ddl_revoke_roles);
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

			if ((res = rel_list(sql->sa, res, rel_role(sql->sa, grantee, role, grantor, grant, ddl_grant_roles))) == NULL) {
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

			if ((res = rel_list(sql->sa, res, rel_role(sql->sa, grantee, role, grantor, admin, ddl_revoke_roles))) == NULL) {
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
	if (!rel || !exps)
		return NULL;

	assert(type == ddl_grant || type == ddl_revoke);
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
	if (!rel || !exps)
		return NULL;

	assert(type == ddl_grant_func || type == ddl_revoke_func);
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

			if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, NULL, grantee, priv, NULL, grant, grantor, ddl_grant))) == NULL) {
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
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE | PRIV_TRUNCATE;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);

	if (!sname)
		sname = cur->base.name;
	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		if (!privs) {
			if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, all, NULL, grant, grantor, ddl_grant))) == NULL) {
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
			case SQL_TRUNCATE:
				priv = PRIV_TRUNCATE;
				break;
			case SQL_EXECUTE:
			default:
				return sql_error(sql, 02, SQLSTATE(42000) "Cannot GRANT EXECUTE on table name %s", tname);
			}

			if ((op->token == SQL_SELECT || op->token == SQL_UPDATE) && op->data.lval) {
				dnode *cn;

				for (cn = op->data.lval->h; cn; cn = cn->next) {
					char *cname = cn->data.sval;
					if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, priv, cname, grant, grantor, ddl_grant))) == NULL) {
						rel_destroy(res);
						return NULL;
					}
				}
			} else if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, priv, NULL, grant, grantor, ddl_grant))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_grant_func(mvc *sql, sql_schema *cur, dlist *privs, dlist *qname, dlist *typelist, sql_ftype type, dlist *grantees, int grant, int grantor)
{
	sql_rel *res = NULL;
	dnode *gn;
	char *sname = qname_schema(qname);
	char *fname = qname_schema_object(qname);
	sql_schema *s = cur;
	sql_func *func = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, SQLSTATE(3F000) "GRANT: no such schema '%s'", sname);
	func = resolve_func(sql, s, fname, typelist, type, "GRANT", 0);
	if (!func)
		return NULL;
	if (!func->s)
		return sql_error(sql, 02, SQLSTATE(42000) "Cannot GRANT EXECUTE on system function '%s'", fname);

	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		if (!privs) {
			if ((res = rel_list(sql->sa, res, rel_func_priv(sql->sa, s->base.name, func->base.id, grantee, PRIV_EXECUTE, grant, grantor, ddl_grant_func))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
			continue;
		}
		for (opn = privs->h; opn; opn = opn->next) {
			symbol *op = opn->data.sym;

			if (op->token != SQL_EXECUTE)
				return sql_error(sql, 02, SQLSTATE(42000) "Can only GRANT 'EXECUTE' on function '%s'", fname);
			if ((res = rel_list(sql->sa, res, rel_func_priv(sql->sa, s->base.name, func->base.id, grantee, PRIV_EXECUTE, grant, grantor, ddl_grant_func))) == NULL) {
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
	tokens token = obj->token;

	if (token == SQL_NAME) {
		dlist *qname = obj->data.lval;
		char *sname = qname_schema(qname);
		char *tname = qname_schema_object(qname);
		sql_schema *s = cur;
		sql_table *t = NULL;

		if (sname && !(s = mvc_bind_schema(sql, sname)))
			return sql_error(sql, 02, SQLSTATE(3F000) "GRANT: no such schema '%s'", sname);
		if ((t = find_table_on_scope(sql, &s, sname, tname)))
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
		sql_ftype type = (sql_ftype) r->h->next->next->data.i_val;

		return rel_grant_func(sql, cur, obj_privs, qname, typelist, type, grantees, grant, grantor);
	}
	default:
		return sql_error(sql, 02, SQLSTATE(M0M03) "Grant: unknown token %d", (int) token);
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

			if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, NULL, grantee, priv, NULL, grant, grantor, ddl_revoke))) == NULL) {
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
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE | PRIV_TRUNCATE;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);

	if (!sname)
		sname = cur->base.name;
	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		if (!privs) {
			if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, all, NULL, grant, grantor, ddl_revoke))) == NULL) {
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
			case SQL_TRUNCATE:
				priv = PRIV_TRUNCATE;
				break;
			case SQL_EXECUTE:
			default:
				return sql_error(sql, 02, SQLSTATE(42000) "Cannot GRANT EXECUTE on table name %s", tname);
			}

			if ((op->token == SQL_SELECT || op->token == SQL_UPDATE) && op->data.lval) {
				dnode *cn;

				for (cn = op->data.lval->h; cn; cn = cn->next) {
					char *cname = cn->data.sval;
					if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, priv, cname, grant, grantor, ddl_revoke))) == NULL) {
						rel_destroy(res);
						return NULL;
					}
				}
			} else if ((res = rel_list(sql->sa, res, rel_priv(sql->sa, sname, tname, grantee, priv, NULL, grant, grantor, ddl_revoke))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
		}
	}
	return res;
}

static sql_rel *
rel_revoke_func(mvc *sql, sql_schema *cur, dlist *privs, dlist *qname, dlist *typelist, sql_ftype type, dlist *grantees, int grant, int grantor)
{
	dnode *gn;
	sql_rel *res = NULL;
	char *sname = qname_schema(qname);
	char *fname = qname_schema_object(qname);
	sql_func *func = NULL;
	sql_schema *s = cur;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, SQLSTATE(3F000) "REVOKE: no such schema '%s'", sname);
	func = resolve_func(sql, s, fname, typelist, type, "REVOKE", 0);
	if (!func)
		return NULL;
	if (!func->s)
		return sql_error(sql, 02, SQLSTATE(42000) "Cannot REVOKE EXECUTE on system function '%s'", fname);
	for (gn = grantees->h; gn; gn = gn->next) {
		dnode *opn;
		char *grantee = gn->data.sval;

		if (!grantee)
			grantee = "public";

		if (!privs) {
			if ((res = rel_list(sql->sa, res, rel_func_priv(sql->sa, s->base.name, func->base.id, grantee, PRIV_EXECUTE, grant, grantor, ddl_revoke_func))) == NULL) {
				rel_destroy(res);
				return NULL;
			}
			continue;
		}
		for (opn = privs->h; opn; opn = opn->next) {
			symbol *op = opn->data.sym;

			if (op->token != SQL_EXECUTE)
				return sql_error(sql, 02, SQLSTATE(42000) "Can only REVOKE EXECUTE on function name %s", fname);

			if ((res = rel_list(sql->sa, res, rel_func_priv(sql->sa, s->base.name, func->base.id, grantee, PRIV_EXECUTE, grant, grantor, ddl_revoke_func))) == NULL) {
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
	tokens token = obj->token;

	if (token == SQL_NAME) {
		dlist *qname = obj->data.lval;
		char *sname = qname_schema(qname);
		char *tname = qname_schema_object(qname);
		sql_schema *s = cur;
		sql_table *t = NULL;

		if (sname && !(s = mvc_bind_schema(sql, sname)))
			return sql_error(sql, 02, SQLSTATE(3F000) "REVOKE: no such schema '%s'", sname);
		if ((t = find_table_on_scope(sql, &s, sname, tname)))
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
		sql_ftype type = (sql_ftype) r->h->next->next->data.i_val;

		return rel_revoke_func(sql, cur, obj_privs, qname, typelist, type, grantees, grant, grantor);
	}
	default:
		return sql_error(sql, 02, SQLSTATE(M0M03) "Revoke: unknown token %d", (int) token);
	}
}

/* iname, itype, sname.tname (col1 .. coln) */
static sql_rel *
rel_create_index(mvc *sql, char *iname, idx_type itype, dlist *qname, dlist *column_list)
{
	sql_schema *s = cur_schema(sql);
	sql_table *t = NULL, *nt;
	sql_rel *r, *res;
	sql_exp **updates, *e;
	sql_idx *i;
	dnode *n;
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, SQLSTATE(3F000) "CREATE INDEX: no such schema '%s'", sname);
	if (!mvc_schema_privs(sql, s))
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE INDEX: access denied for %s to schema '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), s->base.name);
	i = mvc_bind_idx(sql, s, iname);
	if (i)
		return sql_error(sql, 02, SQLSTATE(42S11) "CREATE INDEX: name '%s' already in use", iname);
	if (!(t = find_table_on_scope(sql, &s, sname, tname))) {
		return sql_error(sql, 02, SQLSTATE(42S02) "CREATE INDEX: no such table '%s'", tname);
	} else if (isView(t) || isMergeTable(t) || isRemote(t)) {
		return sql_error(sql, 02, SQLSTATE(42S02) "CREATE INDEX: cannot create index on %s '%s'", isView(t)?"view":
						isMergeTable(t)?"merge table":"remote table", tname);
	}
	sname = get_schema_name(sql, sname, tname);
	nt = dup_sql_table(sql->sa, t);

	if (t->persistence != SQL_DECLARED_TABLE)
		sname = s->base.name;

	/* add index here */
	i = mvc_create_idx(sql, nt, iname, itype);
	for (n = column_list->h; n; n = n->next) {
		sql_column *c = mvc_bind_column(sql, nt, n->data.sval);

		if (!c)
			return sql_error(sql, 02, SQLSTATE(42S22) "CREATE INDEX: no such column '%s'", n->data.sval);
		mvc_create_ic(sql, i, c);
	}

	/* new columns need update with default values */
	updates = SA_ZNEW_ARRAY(sql->sa, sql_exp*, list_length(nt->columns.set));
	e = exp_column(sql->sa, nt->base.name, TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1);
	res = rel_table(sql, ddl_alter_table, sname, nt, 0);
	r = rel_project(sql->sa, res, append(new_exp_list(sql->sa),e));
	res = rel_update(sql, res, r, updates, NULL);
	return res;
}

static sql_rel *
rel_create_user(sql_allocator *sa, char *user, char *passwd, int enc, char *fullname, char *schema)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);
	if (!rel || !exps)
		return NULL;

	append(exps, exp_atom_clob(sa, user));
	append(exps, exp_atom_clob(sa, passwd));
	append(exps, exp_atom_int(sa, enc));
	append(exps, exp_atom_clob(sa, schema));
	append(exps, exp_atom_clob(sa, fullname));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = ddl_create_user;
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
	if (!rel || !exps)
		return NULL;

	append(exps, exp_atom_clob(sa, user));
	append(exps, exp_atom_clob(sa, passwd));
	append(exps, exp_atom_int(sa, enc));
	append(exps, exp_atom_clob(sa, schema));
	append(exps, exp_atom_clob(sa, oldpasswd));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = ddl_alter_user;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sqlid
rel_find_designated_schema(mvc *sql, symbol *sym, sql_schema **schema_out) {
	char *sname;
	sql_schema *s;

	assert(sym->type == type_string);
	sname = sym->data.sval;
	if (!(s = mvc_bind_schema(sql, sname))) {
		sql_error(sql, 02, SQLSTATE(3F000) "COMMENT ON: no such schema: %s", sname);
		return 0;
	}

	*schema_out = s;
	return s->base.id;
}

static sqlid
rel_find_designated_table(mvc *sql, symbol *sym, sql_schema **schema_out) {
	dlist *qname;
	sql_schema *s = cur_schema(sql);
	char *sname, *tname;
	sql_table *t;
	int want_table = sym->token == SQL_TABLE;

	assert(sym->type == type_list);
	qname = sym->data.lval;
	sname = qname_schema(qname);
	if (sname && !(s = mvc_bind_schema(sql, sname))) {
		sql_error(sql, 02, SQLSTATE(3F000) "COMMENT ON: no such schema: %s", sname);
		return 0;
	}
	tname = qname_schema_object(qname);
	t = find_table_on_scope(sql, &s, sname, tname);
	if (t && t->s && isTempSchema(t->s)) {
		sql_error(sql, 02, SQLSTATE(42000) "COMMENT ON tmp object not allowed");
		return 0;
	}
	if (t && !want_table == !isKindOfTable(t)) {	/* comparing booleans can be tricky */
		*schema_out = s;
		return t->base.id;
	}

	sql_error(sql, 02, SQLSTATE(42S02) "COMMENT ON: no such %s: %s.%s",
		want_table ? "table" : "view",
		s->base.name, tname);
	return 0;
}

static sqlid
rel_find_designated_column(mvc *sql, symbol *sym, sql_schema **schema_out) {
	char *sname, *tname, *cname;
	dlist *colname;
	sql_schema *s = cur_schema(sql);
	sql_table *t;
	sql_column *c;

	assert(sym->type == type_list);
	colname = sym->data.lval;
	assert(colname->cnt == 2 || colname->cnt == 3);
	assert(colname->h->type == type_string);
	assert(colname->h->next->type == type_string);
	if (colname->cnt == 2) {
		sname = NULL;
		tname = colname->h->data.sval;
		cname = colname->h->next->data.sval;
	} else {
		// cnt == 3
		sname = colname->h->data.sval;
		tname = colname->h->next->data.sval;
		assert(colname->h->next->next->type == type_string);
		cname = colname->h->next->next->data.sval;
	}
	if (sname && !(s = mvc_bind_schema(sql, sname))) {
		sql_error(sql, 02, SQLSTATE(3F000) "COMMENT ON: no such schema: %s", sname);
		return 0;
	}
	if (!(t = find_table_on_scope(sql, &s, sname, tname))) {
		sql_error(sql, 02, SQLSTATE(42S02) "COMMENT ON: no such table: %s.%s", s->base.name, tname);
		return 0;
	}
	if (t && t->s && isTempSchema(t->s)) {
		sql_error(sql, 02, SQLSTATE(42000) "COMMENT ON tmp object not allowed");
		return 0;
	}
	if (!(c = mvc_bind_column(sql, t, cname))) {
		sql_error(sql, 02, SQLSTATE(42S12) "COMMENT ON: no such column: %s.%s", tname, cname);
		return 0;
	}
	*schema_out = s;
	return c->base.id;
}

static sqlid
rel_find_designated_index(mvc *sql, symbol *sym, sql_schema **schema_out) {
	dlist *qname;
	sql_schema *s = cur_schema(sql);
	char *iname, *sname;
	sql_idx *idx;

	assert(sym->type == type_list);
	qname = sym->data.lval;
	sname = qname_schema(qname);
	if (sname && !(s = mvc_bind_schema(sql, sname))) {
		sql_error(sql, 02, SQLSTATE(3F000) "COMMENT ON: no such schema: %s", sname);
		return 0;
	}
	iname = qname_schema_object(qname);
	idx = mvc_bind_idx(sql, s, iname);
	if (idx && idx->t->s && isTempSchema(idx->t->s)) {
		sql_error(sql, 02, SQLSTATE(42000) "COMMENT ON tmp object not allowed");
		return 0;
	}
	if (idx) {
		*schema_out = s;
		return idx->base.id;
	}

	sql_error(sql, 02, SQLSTATE(42S12) "COMMENT ON: no such index: %s.%s",
		s->base.name, iname);
	return 0;
}

static sqlid
rel_find_designated_sequence(mvc *sql, symbol *sym, sql_schema **schema_out) {
	(void)sql;
	(void)sym;
	dlist *qname;
	sql_schema *s = cur_schema(sql);
	char *seqname, *sname;
	sql_sequence *seq;

	assert(sym->type == type_list);
	qname = sym->data.lval;
	sname = qname_schema(qname);
	if (sname && !(s = mvc_bind_schema(sql, sname))) {
		sql_error(sql, 02, SQLSTATE(3F000) "COMMENT ON: no such schema: %s", sname);
		return 0;
	}
	seqname = qname_schema_object(qname);
	seq = find_sql_sequence(s, seqname);
	if (seq && seq->s && isTempSchema(seq->s)) {
		sql_error(sql, 02, SQLSTATE(42000) "COMMENT ON tmp object not allowed");
		return 0;
	}
	if (seq) {
		*schema_out = s;
		return seq->base.id;
	}

	sql_error(sql, 02, SQLSTATE(42000) "COMMENT ON: no such sequence: %s.%s",
		s->base.name, seqname);
	return 0;
}

static sqlid
rel_find_designated_routine(mvc *sql, symbol *sym, sql_schema **schema_out) {
	(void)sql;
	(void)sym;
	dlist *designator;
	dlist *qname;
	dlist *typelist;
	sql_ftype func_type;
	sql_schema *s = cur_schema(sql);
	char *fname, *sname;
	sql_func *func;

	assert(sym->type == type_list);
	designator = sym->data.lval;
	assert(designator->cnt == 3);
	qname = designator->h->data.lval;
	sname = qname_schema(qname);
	typelist = designator->h->next->data.lval;
	func_type = (sql_ftype) designator->h->next->next->data.i_val;

	if (sname && !(s = mvc_bind_schema(sql, sname))) {
		sql_error(sql, 02, SQLSTATE(3F000) "COMMENT ON: no such schema: %s", sname);
		return 0;
	}

	fname = qname_schema_object(qname);
	func = resolve_func(sql, s, fname, typelist, func_type, "COMMENT", 0);
	if (!func && func_type == F_FUNC) {
		// functions returning a table have a special type
		func = resolve_func(sql, s, fname, typelist, F_UNION, "COMMENT", 0);
	}
	if (func && func->s && isTempSchema(func->s)) {
		sql_error(sql, 02, SQLSTATE(42000) "COMMENT ON tmp object not allowed");
		return 0;
	}
	if (func) {
		*schema_out = s;
		return func->base.id;
	}

	if (sql->errstr[0] == '\0')
		sql_error(sql, 02, SQLSTATE(42000) "COMMENT ON: no such routine: %s.%s", s->base.name, fname);
	return 0;
}

static sqlid
rel_find_designated_object(mvc *sql, symbol *sym, sql_schema **schema_out)
{
	sql_schema *dummy = NULL;

	if (schema_out == NULL)
		schema_out = &dummy;
	switch (sym->token) {
	case SQL_SCHEMA:
		return rel_find_designated_schema(sql, sym, schema_out);
	case SQL_TABLE:
		return rel_find_designated_table(sql, sym, schema_out);
	case SQL_VIEW:
		return rel_find_designated_table(sql, sym, schema_out);
	case SQL_COLUMN:
		return rel_find_designated_column(sql, sym, schema_out);
	case SQL_INDEX:
		return rel_find_designated_index(sql, sym, schema_out);
	case SQL_SEQUENCE:
		return rel_find_designated_sequence(sql, sym, schema_out);
	case SQL_ROUTINE:
		return rel_find_designated_routine(sql, sym, schema_out);
	default:
		sql_error(sql, 2, SQLSTATE(42000) "COMMENT ON %s is not supported", token2string(sym->token));
		return 0;
	}
}

static sql_rel *
rel_comment_on(sql_allocator *sa, sqlid obj_id, const char *remark)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	if (rel == NULL || exps == NULL)
		return NULL;

	append(exps, exp_atom_int(sa, obj_id));
	append(exps, exp_atom_clob(sa, remark));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = ddl_comment_on;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static char *
credentials_username(dlist *credentials)
{
	if (credentials == NULL) {
		return NULL;
	}
	assert(credentials->h);

	if (credentials->h->data.sval != NULL) {
		return credentials->h->data.sval;
	}

	// No username specified.
	return NULL;
}

static char *
credentials_password(dlist *credentials)
{
	if (credentials == NULL) {
		return NULL;
	}
	assert(credentials->h);

	char *password = credentials->h->next->next->data.sval;

	return password;
}

static sql_rel *
rel_rename_schema(mvc *sql, char *old_name, char *new_name, int if_exists)
{
	sql_schema *s;
	sql_rel *rel;
	list *exps;

	assert(old_name && new_name);
	if (!(s = mvc_bind_schema(sql, old_name))) {
		if (if_exists)
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SCHEMA: no such schema '%s'", old_name);
	}
	if (!mvc_schema_privs(sql, s))
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SCHEMA: access denied for %s to schema '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), old_name);
	if (s->system)
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SCHEMA: cannot rename a system schema");
	if (!list_empty(s->tables.set) || !list_empty(s->types.set) || !list_empty(s->funcs.set) || !list_empty(s->seqs.set))
		return sql_error(sql, 02, SQLSTATE(2BM37) "ALTER SCHEMA: unable to rename schema '%s' (there are database objects which depend on it)", old_name);
	if (strNil(new_name) || *new_name == '\0')
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SCHEMA: invalid new schema name");
	if (mvc_bind_schema(sql, new_name))
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SCHEMA: there is a schema named '%s' in the database", new_name);

	rel = rel_create(sql->sa);
	exps = new_exp_list(sql->sa);
	append(exps, exp_atom_clob(sql->sa, old_name));
	append(exps, exp_atom_clob(sql->sa, new_name));
	rel->op = op_ddl;
	rel->flag = ddl_rename_schema;
	rel->exps = exps;
	return rel;
}

static sql_rel *
rel_rename_table(mvc *sql, char *schema_name, char *old_name, char *new_name, int if_exists)
{
	sql_schema *s = cur_schema(sql);
	sql_table *t;
	sql_rel *rel;
	list *exps;

	assert(old_name && new_name);

	if (schema_name && !(s = mvc_bind_schema(sql, schema_name))) {
		if (if_exists)
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: no such schema '%s'", schema_name);
	}
	if (!mvc_schema_privs(sql, s))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), s->base.name);
	if (!(t = find_table_on_scope(sql, &s, schema_name, old_name))) {
		if (if_exists)
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", old_name, s->base.name);
	}
	if (t->system)
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: cannot rename a system table");
	if (isView(t))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: cannot rename a view");
	if (isDeclaredTable(t))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: cannot rename a declared table");
	if (mvc_check_dependency(sql, t->base.id, TABLE_DEPENDENCY, NULL))
		return sql_error(sql, 02, SQLSTATE(2BM37) "ALTER TABLE: unable to rename table '%s' (there are database objects which depend on it)", old_name);
	if (strNil(new_name) || *new_name == '\0')
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER TABLE: invalid new table name");
	if (find_table_on_scope(sql, &s, s->base.name, new_name))
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER TABLE: there is a table named '%s' in schema '%s'", new_name, s->base.name);

	rel = rel_create(sql->sa);
	exps = new_exp_list(sql->sa);
	append(exps, exp_atom_clob(sql->sa, s->base.name));
	append(exps, exp_atom_clob(sql->sa, s->base.name));
	append(exps, exp_atom_clob(sql->sa, old_name));
	append(exps, exp_atom_clob(sql->sa, new_name));
	rel->op = op_ddl;
	rel->flag = ddl_rename_table;
	rel->exps = exps;
	return rel;
}

static sql_rel *
rel_rename_column(mvc *sql, char *schema_name, char *table_name, char *old_name, char *new_name, int if_exists)
{
	sql_schema *s = cur_schema(sql);
	sql_table *t;
	sql_column *col;
	sql_rel *rel;
	list *exps;

	assert(table_name && old_name && new_name);

	if (schema_name && !(s = mvc_bind_schema(sql, schema_name))) {
		if (if_exists)
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: no such schema '%s'", schema_name);
	}
	if (!mvc_schema_privs(sql, s))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), s->base.name);
	if (!(t = find_table_on_scope(sql, &s, schema_name, table_name))) {
		if (if_exists)
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", table_name, s->base.name);
	}
	if (t->system)
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: cannot rename a column in a system table");
	if (isView(t))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: cannot rename column '%s': '%s' is a view", old_name, table_name);
	if (isDeclaredTable(t))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: cannot rename a column in a declared table");
	if (!(col = mvc_bind_column(sql, t, old_name)))
		return sql_error(sql, 02, SQLSTATE(42S22) "ALTER TABLE: no such column '%s' in table '%s'", old_name, table_name);
	if (mvc_check_dependency(sql, col->base.id, COLUMN_DEPENDENCY, NULL))
		return sql_error(sql, 02, SQLSTATE(2BM37) "ALTER TABLE: cannot rename column '%s' (there are database objects which depend on it)", old_name);
	if (strNil(new_name) || *new_name == '\0')
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER TABLE: invalid new column name");
	if (mvc_bind_column(sql, t, new_name))
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER TABLE: there is a column named '%s' in table '%s'", new_name, table_name);

	rel = rel_create(sql->sa);
	exps = new_exp_list(sql->sa);
	append(exps, exp_atom_clob(sql->sa, s->base.name));
	append(exps, exp_atom_clob(sql->sa, table_name));
	append(exps, exp_atom_clob(sql->sa, old_name));
	append(exps, exp_atom_clob(sql->sa, new_name));
	rel->op = op_ddl;
	rel->flag = ddl_rename_column;
	rel->exps = exps;
	return rel;
}

static sql_rel *
rel_set_table_schema(sql_query *query, char *old_schema, char *tname, char *new_schema, int if_exists)
{
	mvc *sql = query->sql;
	sql_schema *os = cur_schema(sql), *ns;
	sql_table *ot;
	sql_rel *rel;
	list *exps;

	assert(tname && new_schema);

	if (old_schema && !(os = mvc_bind_schema(sql, old_schema))) {
		if (if_exists)
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: no such schema '%s'", old_schema);
	}
	if (!mvc_schema_privs(sql, os))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: access denied for %s to schema '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), os->base.name);
	if (!(ot = find_table_on_scope(sql, &os, old_schema, tname))) {
		if (if_exists)
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: no such table '%s' in schema '%s'", tname, os->base.name);
	}
	if (ot->system)
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: cannot set schema of a system table");
	if (isTempSchema(os))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: not possible to change a temporary table schema");
	if (isView(ot))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: not possible to change schema of a view");
	if (isDeclaredTable(ot))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: not possible to change schema of a declared table");
	if (mvc_check_dependency(sql, ot->base.id, TABLE_DEPENDENCY, NULL) || !list_empty(ot->members) || !list_empty(ot->triggers.set))
		return sql_error(sql, 02, SQLSTATE(2BM37) "ALTER TABLE: unable to set schema of table '%s' (there are database objects which depend on it)", tname);
	if (!(ns = mvc_bind_schema(sql, new_schema)))
		return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: no such schema '%s'", new_schema);
	if (!mvc_schema_privs(sql, ns))
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: access denied for '%s' to schema '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), new_schema);
	if (isTempSchema(ns))
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER TABLE: not possible to change table's schema to temporary");
	if (find_table_on_scope(sql, &ns, new_schema, tname))
		return sql_error(sql, 02, SQLSTATE(42S02) "ALTER TABLE: table '%s' on schema '%s' already exists", tname, new_schema);

	rel = rel_create(sql->sa);
	exps = new_exp_list(sql->sa);
	append(exps, exp_atom_clob(sql->sa, os->base.name));
	append(exps, exp_atom_clob(sql->sa, new_schema));
	append(exps, exp_atom_clob(sql->sa, tname));
	append(exps, exp_atom_clob(sql->sa, tname));
	rel->op = op_ddl;
	rel->flag = ddl_rename_table;
	rel->exps = exps;
	return rel;
}

sql_rel *
rel_schemas(sql_query *query, symbol *s)
{
	mvc *sql = query->sql;
	sql_rel *ret = NULL;

	if (s->token != SQL_CREATE_TABLE && s->token != SQL_CREATE_VIEW && STORE_READONLY)
		return sql_error(sql, 06, SQLSTATE(25006) "Schema statements cannot be executed on a readonly database.");

	switch (s->token) {
	case SQL_CREATE_SCHEMA:
	{
		dlist *l = s->data.lval;

		ret = rel_create_schema(query, l->h->data.lval,
				l->h->next->next->next->data.lval,
				l->h->next->next->next->next->data.i_val); /* if not exists */
	} 	break;
	case SQL_DROP_SCHEMA:
	{
		dlist *l = s->data.lval;
		dlist *auth_name = l->h->data.lval;

		assert(l->h->next->type == type_int);
		ret = rel_drop(sql->sa, ddl_drop_schema,
			   dlist_get_schema_name(auth_name),
			   NULL,
			   NULL,
			   l->h->next->data.i_val, 	/* drop_action */
			   l->h->next->next->data.i_val); /* if exists */
	} 	break;
	case SQL_DECLARE_TABLE:
		return sql_error(sql, 02, SQLSTATE(42000) "Tables cannot be declared on the global scope");
	case SQL_CREATE_TABLE:
	{
		dlist *l = s->data.lval;
		dlist *qname = l->h->next->data.lval;
		char *sname = qname_schema(qname);
		char *name = qname_schema_object(qname);
		int temp = l->h->data.i_val;
		dlist *credentials = l->h->next->next->next->next->next->data.lval;
		char *username = credentials_username(credentials);
		char *password = credentials_password(credentials);
		bool pw_encrypted = credentials == NULL || credentials->h->next->data.i_val == SQL_PW_ENCRYPTED;
		if (username == NULL) {
			// No username specified, get the current username
			username = sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user"));
		}

		assert(l->h->type == type_int);
		assert(l->h->next->next->next->type == type_int);
		ret = rel_create_table(query, temp, sname, name, true,
				       l->h->next->next->data.sym,                   /* elements or subquery */
				       l->h->next->next->next->data.i_val,           /* commit action */
				       l->h->next->next->next->next->data.sval,      /* location */
				       username, password, pw_encrypted,
				       l->h->next->next->next->next->next->next->next->data.sym,
				       l->h->next->next->next->next->next->next->data.i_val); /* if not exists */
	} 	break;
	case SQL_CREATE_VIEW:
	{
		dlist *l = s->data.lval;

		assert(l->h->next->next->next->type == type_int);
		assert(l->h->next->next->next->next->type == type_int);
		ret = rel_create_view(query, NULL, l->h->data.lval,
							  l->h->next->data.lval,
							  l->h->next->next->data.sym,
							  l->h->next->next->next->data.i_val,
							  l->h->next->next->next->next->data.i_val,
							  l->h->next->next->next->next->next->data.i_val); /* or replace */
	} 	break;
	case SQL_DROP_TABLE:
	{
		dlist *l = s->data.lval;
		char *sname = qname_schema(l->h->data.lval);
		char *tname = qname_schema_object(l->h->data.lval);

		assert(l->h->next->type == type_int);
		sname = get_schema_name(sql, sname, tname);

		ret = rel_drop(sql->sa, ddl_drop_table, sname, tname, NULL,
						 l->h->next->data.i_val,
						 l->h->next->next->data.i_val); /* if exists */
	} 	break;
	case SQL_DROP_VIEW:
	{
		dlist *l = s->data.lval;
		char *sname = qname_schema(l->h->data.lval);
		char *tname = qname_schema_object(l->h->data.lval);

		assert(l->h->next->type == type_int);
		sname = get_schema_name(sql, sname, tname);
		ret = rel_drop(sql->sa, ddl_drop_view, sname, tname, NULL,
						 l->h->next->data.i_val,
						 l->h->next->next->data.i_val); /* if exists */
	} 	break;
	case SQL_ALTER_TABLE:
	{
		dlist *l = s->data.lval;

		ret = sql_alter_table(query, l,
			l->h->data.lval,      /* table name */
			l->h->next->data.sym, /* table element */
			l->h->next->next->data.i_val); /* if exists */
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
		ret = rel_schema2(sql->sa, ddl_create_role, rname, NULL,
				 l->h->next->data.i_val  == cur_user? sql->user_id : sql->role_id);
	} 	break;
	case SQL_DROP_ROLE:
	{
		char *rname = s->data.sval;
		ret = rel_schema2(sql->sa, ddl_drop_role, rname, NULL, 0);
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
		ret = rel_schema2(sql->sa, ddl_drop_index, sname, qname_schema_object(l), 0);
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
		ret = rel_schema2(sql->sa, ddl_drop_user, s->data.sval, NULL, 0);
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

		ret = rel_schema2(sql->sa, ddl_rename_user, l->h->data.sval, l->h->next->data.sval, 0);
	} 	break;
	case SQL_RENAME_SCHEMA: {
		dlist *l = s->data.lval;
		ret = rel_rename_schema(sql, l->h->data.sval, l->h->next->data.sval, l->h->next->next->data.i_val);
	} 	break;
	case SQL_RENAME_TABLE: {
		dlist *l = s->data.lval;
		char *sname = qname_schema(l->h->data.lval);
		char *tname = qname_schema_object(l->h->data.lval);
		ret = rel_rename_table(sql, sname, tname, l->h->next->data.sval, l->h->next->next->data.i_val);
	} 	break;
	case SQL_RENAME_COLUMN: {
		dlist *l = s->data.lval;
		char *sname = qname_schema(l->h->data.lval);
		char *tname = qname_schema_object(l->h->data.lval);
		ret = rel_rename_column(sql, sname, tname, l->h->next->data.sval, l->h->next->next->data.sval, l->h->next->next->next->data.i_val);
	} 	break;
	case SQL_SET_TABLE_SCHEMA: {
		dlist *l = s->data.lval;
		char *sname = qname_schema(l->h->data.lval);
		char *tname = qname_schema_object(l->h->data.lval);
		ret = rel_set_table_schema(query, sname, tname, l->h->next->data.sval, l->h->next->next->data.i_val);
	} 	break;
	case SQL_CREATE_TYPE: {
		dlist *l = s->data.lval;

		ret = rel_create_type(sql, l->h->data.lval, l->h->next->data.sval);
	} 	break;
	case SQL_DROP_TYPE: {
		dlist *l = s->data.lval;
		ret = rel_drop_type(sql, l->h->data.lval, l->h->next->data.i_val);
	} 	break;
	case SQL_COMMENT:
	{
		dlist *l = s->data.lval;
		symbol *catalog_object = l->h->data.sym;
		char *remark;
		sql_schema *s;
		sqlid id;

		assert(l->cnt == 2);
		remark = l->h->next->data.sval;

		id = rel_find_designated_object(sql, catalog_object, &s);
		if (!id) {
			/* rel_find_designated_object has already set the error message so we don't have to */
			return NULL;
		}

		// Check authorization
		if (!mvc_schema_privs(sql, s)) {
			return sql_error(sql, 02, SQLSTATE(42000) "COMMENT ON: insufficient privileges for user '%s' in schema '%s'", sqlvar_get_string(find_global_var(sql, mvc_bind_schema(sql, "sys"), "current_user")), s->base.name);
		}

		return rel_comment_on(sql->sa, id, remark);
	}
	default:
		return sql_error(sql, 01, SQLSTATE(M0M03) "Schema statement unknown symbol(%p)->token = %s", s, token2string(s->token));
	}

	sql->type = Q_SCHEMA;
	return ret;
}

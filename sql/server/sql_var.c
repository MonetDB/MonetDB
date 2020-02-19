/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_mvc.h"
#include "sql_scan.h"
#include "sql_list.h"
#include "sql_types.h"
#include "sql_catalog.h"
#include "sql_atom.h"
#include "rel_rel.h"

/* variable management */
static sql_var*
stack_set(mvc *sql, int var, const char *name, sql_subtype *type, sql_rel *rel, sql_table *t, dlist *wdef, sql_groupby_expression *exp, int view, int frame)
{
	sql_var *v, *nvars;
	int nextsize = sql->sizevars;
	if (var == nextsize) {
		nextsize <<= 1;
		nvars = RENEW_ARRAY(sql_var,sql->vars,nextsize);
		if(!nvars) {
			return NULL;
		} else {
			sql->vars = nvars;
			sql->sizevars = nextsize;
		}
	}
	v = sql->vars+var;

	v->name = NULL;
	atom_init( &v->a );
	v->rel = rel;
	v->t = t;
	v->view = view;
	v->frame = frame;
	v->visited = 0;
	v->wdef = wdef;
	v->exp = exp;
	if (type) {
		int tpe = type->type->localtype;
		VALset(&sql->vars[var].a.data, tpe, (ptr) ATOMnilptr(tpe));
		v->a.tpe = *type;
	}
	if (name) {
		v->name = _STRDUP(name);
		if(!v->name)
			return NULL;
	}
	return v;
}

sql_var*
stack_push_var(mvc *sql, const char *name, sql_subtype *type)
{
	sql_var* res = stack_set(sql, sql->topvars, name, type, NULL, NULL, NULL, NULL, 0, 0);
	if(res)
		sql->topvars++;
	return res;
}

sql_var*
stack_push_rel_var(mvc *sql, const char *name, sql_rel *var, sql_subtype *type)
{
	sql_var* res = stack_set(sql, sql->topvars, name, type, var, NULL, NULL, NULL, 0, 0);
	if(res)
		sql->topvars++;
	return res;
}

sql_var*
stack_push_table(mvc *sql, const char *name, sql_rel *var, sql_table *t)
{
	sql_var* res = stack_set(sql, sql->topvars, name, NULL, var, t, NULL, NULL, 0, 0);
	if(res)
		sql->topvars++;
	return res;
}

sql_var*
stack_push_rel_view(mvc *sql, const char *name, sql_rel *var)
{
	sql_var* res = stack_set(sql, sql->topvars, name, NULL, var, NULL, NULL, NULL, 1, 0);
	if(res)
		sql->topvars++;
	return res;
}

sql_var*
stack_push_window_def(mvc *sql, const char *name, dlist *wdef)
{
	sql_var* res = stack_set(sql, sql->topvars, name, NULL, NULL, NULL, wdef, NULL, 0, 0);
	if(res)
		sql->topvars++;
	return res;
}

dlist *
stack_get_window_def(mvc *sql, const char *name, int *pos)
{
	for (int i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && sql->vars[i].wdef && sql->vars[i].name && strcmp(sql->vars[i].name, name)==0) {
			if(pos)
				*pos = i;
			return sql->vars[i].wdef;
		}
	}
	return NULL;
}

sql_var*
stack_push_groupby_expression(mvc *sql, symbol *def, sql_exp *exp)
{
	sql_var* res = NULL;
	sql_groupby_expression *sge = MNEW(sql_groupby_expression);

	if(sge) {
		sge->sdef = def;
		sge->token = def->token;
		sge->exp = exp;

		res = stack_set(sql, sql->topvars, NULL, NULL, NULL, NULL, NULL, sge, 0, 0);
		if(res)
			sql->topvars++;
	}
	return res;
}

sql_exp*
stack_get_groupby_expression(mvc *sql, symbol *def)
{
	for (int i = sql->topvars-1; i >= 0; i--)
		if (!sql->vars[i].frame && sql->vars[i].exp && sql->vars[i].exp->token == def->token && symbol_cmp(sql, sql->vars[i].exp->sdef, def)==0)
			return sql->vars[i].exp->exp;
	return NULL;
}

/* There could a possibility that this is vulnerable to a time-of-check, time-of-use race condition.
 * However this should never happen in the SQL compiler */
char
stack_check_var_visited(mvc *sql, int i)
{
	if(i < 0 || i >= sql->topvars)
		return 0;
	return sql->vars[i].visited;
}

void
stack_set_var_visited(mvc *sql, int i)
{
	if(i < 0 || i >= sql->topvars)
		return;
	sql->vars[i].visited = 1;
}

void
stack_clear_frame_visited_flag(mvc *sql)
{
	for (int i = sql->topvars-1; i >= 0 && !sql->vars[i].frame; i--)
		sql->vars[i].visited = 0;
}

atom *
stack_set_var(mvc *sql, const char *name, ValRecord *v)
{
	int i;
	atom *res = NULL;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && sql->vars[i].name && strcmp(sql->vars[i].name, name)==0) {
			VALclear(&sql->vars[i].a.data);
			if(VALcopy(&sql->vars[i].a.data, v) == NULL)
				return NULL;
			sql->vars[i].a.isnull = VALisnil(v);
			if (v->vtype == TYPE_flt)
				sql->vars[i].a.d = v->val.fval;
			else if (v->vtype == TYPE_dbl)
				sql->vars[i].a.d = v->val.dval;
			res = &sql->vars[i].a;
		}
	}
	return res;
}

atom *
stack_get_var(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && sql->vars[i].name && strcmp(sql->vars[i].name, name)==0) {
			return &sql->vars[i].a;
		}
	}
	return NULL;
}

sql_var*
stack_push_frame(mvc *sql, const char *name)
{
	sql_var* res = stack_set(sql, sql->topvars, name, NULL, NULL, NULL, NULL, NULL, 0, 1);
	if (res) {
		sql->topvars++;
		sql->frame++;
	}
	return res;
}

void
stack_pop_until(mvc *sql, int top) 
{
	while (sql->topvars > top) {
		sql_var *v = &sql->vars[--sql->topvars];

		c_delete(v->name);
		VALclear(&v->a.data);
		v->a.data.vtype = 0;
		if (v->exp)
			_DELETE(v->exp);
		v->wdef = NULL;
	}
}

void 
stack_pop_frame(mvc *sql)
{
	while (!sql->vars[--sql->topvars].frame) {
		sql_var *v = &sql->vars[sql->topvars];

		c_delete(v->name);
		VALclear(&v->a.data);
		v->a.data.vtype = 0;
		if (v->t && v->view)
			table_destroy(v->t);
		else if (v->rel)
			rel_destroy(v->rel);
		else if(v->exp)
			_DELETE(v->exp);
		v->wdef = NULL;
	}
	if (sql->vars[sql->topvars].name)  
		c_delete(sql->vars[sql->topvars].name);
	sql->frame--;
}

sql_subtype *
stack_find_type(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && !sql->vars[i].view && sql->vars[i].name && strcmp(sql->vars[i].name, name)==0)
			return &sql->vars[i].a.tpe;
	}
	return NULL;
}

sql_table *
stack_find_table(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && !sql->vars[i].view && sql->vars[i].t
			&& sql->vars[i].name && strcmp(sql->vars[i].name, name)==0)
			return sql->vars[i].t;
	}
	return NULL;
}

sql_rel *
stack_find_rel_view(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && sql->vars[i].view &&
		    sql->vars[i].rel && sql->vars[i].name && strcmp(sql->vars[i].name, name)==0)
			return rel_dup(sql->vars[i].rel);
	}
	return NULL;
}

void 
stack_update_rel_view(mvc *sql, const char *name, sql_rel *view)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && sql->vars[i].view &&
		    sql->vars[i].rel && sql->vars[i].name && strcmp(sql->vars[i].name, name)==0) {
			rel_destroy(sql->vars[i].rel);
			sql->vars[i].rel = view;
		}
	}
}

int 
stack_find_var(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && !sql->vars[i].view && sql->vars[i].name && strcmp(sql->vars[i].name, name)==0)
			return 1;
	}
	return 0;
}

sql_rel *
stack_find_rel_var(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (!sql->vars[i].frame && !sql->vars[i].view &&
		    sql->vars[i].rel && sql->vars[i].name && strcmp(sql->vars[i].name, name)==0)
			return rel_dup(sql->vars[i].rel);
	}
	return NULL;
}

int 
frame_find_var(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0 && !sql->vars[i].frame; i--) {
		if (sql->vars[i].name && strcmp(sql->vars[i].name, name)==0)
			return 1;
	}
	return 0;
}

int
stack_find_frame(mvc *sql, const char *name)
{
	int i, frame = sql->frame;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (sql->vars[i].frame) 
			frame--;
		else if (sql->vars[i].name && strcmp(sql->vars[i].name, name)==0)
			return frame;
	}
	return 0;
}

int
stack_has_frame(mvc *sql, const char *name)
{
	int i;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (sql->vars[i].frame && sql->vars[i].name && strcmp(sql->vars[i].name, name)==0)
			return 1;
	}
	return 0;
}

int
stack_nr_of_declared_tables(mvc *sql)
{
	int i, dt = 0;

	for (i = sql->topvars-1; i >= 0; i--) {
		if (sql->vars[i].rel && !sql->vars[i].view) {
			sql_var *v = &sql->vars[i];
			if (v->t)
				dt++;
		}
	}
	return dt;
}

str
stack_set_string(mvc *sql, const char *name, const char *val)
{
	atom *a = stack_get_var(sql, name);
	str new_val = _STRDUP(val);

	if (a != NULL && new_val != NULL) {
		ValRecord *v = &a->data;

		if (v->val.sval)
			_DELETE(v->val.sval);
		v->val.sval = new_val;
		return new_val;
	} else if(new_val) {
		_DELETE(new_val);
	}
	return NULL;
}

str
stack_get_string(mvc *sql, const char *name)
{
	atom *a = stack_get_var(sql, name);

	if (!a || a->data.vtype != TYPE_str)
		return NULL;
	return a->data.val.sval;
}

void
#ifdef HAVE_HGE
stack_set_number(mvc *sql, const char *name, hge val)
#else
stack_set_number(mvc *sql, const char *name, lng val)
#endif
{
	atom *a = stack_get_var(sql, name);

	if (a != NULL) {
		ValRecord *v = &a->data;
#ifdef HAVE_HGE
		if (v->vtype == TYPE_hge) 
			v->val.hval = val;
#endif
		if (v->vtype == TYPE_lng) 
			v->val.lval = val;
		if (v->vtype == TYPE_int) 
			v->val.lval = (int) val;
		if (v->vtype == TYPE_sht) 
			v->val.lval = (sht) val;
		if (v->vtype == TYPE_bte) 
			v->val.lval = (bte) val;
		if (v->vtype == TYPE_bit) {
			if (val)
				v->val.btval = 1;
			else 
				v->val.btval = 0;
		}
	}
}

#ifdef HAVE_HGE
hge
#else
lng
#endif
val_get_number(ValRecord *v) 
{
	if (v != NULL) {
#ifdef HAVE_HGE
		if (v->vtype == TYPE_hge) 
			return v->val.hval;
#endif
		if (v->vtype == TYPE_lng) 
			return v->val.lval;
		if (v->vtype == TYPE_int) 
			return v->val.ival;
		if (v->vtype == TYPE_sht) 
			return v->val.shval;
		if (v->vtype == TYPE_bte) 
			return v->val.btval;
		if (v->vtype == TYPE_bit) 
			if (v->val.btval)
				return 1;
		return 0;
	}
	return 0;
}

#ifdef HAVE_HGE
hge
#else
lng
#endif
stack_get_number(mvc *sql, const char *name)
{
	atom *a = stack_get_var(sql, name);
	return val_get_number(a?&a->data:NULL);
}

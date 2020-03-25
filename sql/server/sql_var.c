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

static void
destroy_sql_var(void *data)
{
	sql_var *svar = (sql_var*) data;
	VALclear(&(svar->var.data));
	svar->var.data.vtype = 0;
	_DELETE(svar->name);
	_DELETE(svar);
}

sql_var*
stack_push_var(mvc *sql, sql_schema *s, const char *name, sql_subtype *type)
{
	(void) s; /* for now ignore sname */
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_var *svar = MNEW(sql_var);

	if (!svar)
		return NULL;
	if (!(svar->name = _STRDUP(name))) {
		_DELETE(svar);
		return NULL;
	}
	atom_init(&(svar->var));
	if (type) {
		int tpe = type->type->localtype;
		VALset(&(svar->var.data), tpe, (ptr) ATOMnilptr(tpe));
		svar->var.tpe = *type;
	}
	if (!f->vars && !(f->vars = list_create(destroy_sql_var))) {
		_DELETE(svar->name);
		_DELETE(svar);
		return NULL;
	}
	if (!list_append(f->vars, svar)) {
		_DELETE(svar->name);
		_DELETE(svar);
		return NULL;
	}
	return svar;
}

static void
destroy_sql_local_table(void *data)
{
	sql_local_table *slt = (sql_local_table*) data;
	table_destroy(slt->table);
	_DELETE(slt->name);
	_DELETE(slt);
}

sql_local_table*
stack_push_table(mvc *sql, sql_schema *s, const char *name, sql_table *t)
{
	(void) s; /* for now ignore sname */
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_local_table *slt = MNEW(sql_local_table);

	if (!slt)
		return NULL;
	if (!(slt->name = _STRDUP(name))) {
		_DELETE(slt);
		return NULL;
	}
	slt->table = t;
	if (!f->tables && !(f->tables = list_create(destroy_sql_local_table))) {
		_DELETE(slt->name);
		_DELETE(slt);
		return NULL;
	}
	if (!list_append(f->tables, slt)) {
		_DELETE(slt->name);
		_DELETE(slt);
		return NULL;
	}
	return slt;
}

static void
destroy_sql_rel_view(void *data)
{
	sql_rel_view *srv = (sql_rel_view*) data;
	rel_destroy(srv->rel_view);
	_DELETE(srv->name);
	_DELETE(srv);
}

sql_rel_view*
stack_push_rel_view(mvc *sql, const char *name, sql_rel *var)
{
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_rel_view *srv = MNEW(sql_rel_view);

	if (!srv)
		return NULL;
	if (!(srv->name = _STRDUP(name))) {
		_DELETE(srv);
		return NULL;
	}
	srv->rel_view = var;
	if (!f->rel_views && !(f->rel_views = list_create(destroy_sql_rel_view))) {
		_DELETE(srv->name);
		_DELETE(srv);
		return NULL;
	}
	if (!list_append(f->rel_views, srv)) {
		_DELETE(srv->name);
		_DELETE(srv);
		return NULL;
	}
	return srv;
}

static void
destroy_sql_window_definition(void *data)
{
	sql_window_definition *swd = (sql_window_definition*) data;
	_DELETE(swd->name);
	_DELETE(swd);
}

sql_window_definition*
stack_push_window_def(mvc *sql, const char *name, dlist *wdef)
{
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_window_definition *swd = MNEW(sql_window_definition);

	if (!swd)
		return NULL;
	if (!(swd->name = _STRDUP(name))) {
		_DELETE(swd);
		return NULL;
	}
	swd->wdef = wdef;
	swd->visited = false;
	if (!f->windows && !(f->windows = list_create(destroy_sql_window_definition))) {
		_DELETE(swd->name);
		_DELETE(swd);
		return NULL;
	}
	if (!list_append(f->windows, swd)) {
		_DELETE(swd->name);
		_DELETE(swd);
		return NULL;
	}
	return swd;
}

static void
destroy_sql_groupby_expression(void *data)
{
	sql_groupby_expression *sge = (sql_groupby_expression*) data;
	_DELETE(sge);
}

sql_groupby_expression*
stack_push_groupby_expression(mvc *sql, symbol *def, sql_exp *exp)
{
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_groupby_expression *sge = MNEW(sql_groupby_expression);

	if (!sge)
		return NULL;
	sge->sdef = def;
	sge->token = def->token;
	sge->exp = exp;
	if (!f->group_expressions && !(f->group_expressions = list_create(destroy_sql_groupby_expression))) {
		_DELETE(sge);
		return NULL;
	}
	if (!list_append(f->group_expressions, sge)) {
		_DELETE(sge);
		return NULL;
	}
	return sge;
}

dlist *
stack_get_window_def(mvc *sql, const char *name, int *pos)
{
	sql_frame *f = sql->frames[sql->topframes - 1];
	if (f->windows) {
		int i = 0;
		for (node *n = f->windows->h; n ; n = n->next, i++) {
			sql_window_definition *var = (sql_window_definition*) n->data;
			if (var->name && !strcmp(var->name, name)) {
				if (pos)
					*pos = i;
				return var->wdef;
			}
		}
	}
	return NULL;
}

sql_exp*
stack_get_groupby_expression(mvc *sql, symbol *def)
{
	sql_frame *f = sql->frames[sql->topframes - 1];
	if (f->group_expressions) {
		for (node *n = f->group_expressions->h; n ; n = n->next) {
			sql_groupby_expression *var = (sql_groupby_expression*) n->data;
			if (var->token == def->token && !symbol_cmp(sql, var->sdef, def))
				return var->exp;
		}
	}
	return NULL;
}

/* There could a possibility that this is vulnerable to a time-of-check, time-of-use race condition.
 * However this should never happen in the SQL compiler */
bool
stack_check_var_visited(mvc *sql, int i)
{
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_window_definition *win;

	if (i < 0 || i >= list_length(f->windows))
		return false;

	win = (sql_window_definition*) list_fetch(f->windows, i);
	return win->visited;
}

void
stack_set_var_visited(mvc *sql, int i)
{
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_window_definition *win;

	if (i < 0 || i >= list_length(f->windows))
		return;

	win = (sql_window_definition*) list_fetch(f->windows, i);
	win->visited = true;
}

void
stack_clear_frame_visited_flag(mvc *sql)
{
	sql_frame *f = sql->frames[sql->topframes - 1];
	if (f->windows) {
		for (node *n = f->windows->h; n ; n = n->next) {
			sql_window_definition *var = (sql_window_definition*) n->data;
			var->visited = false;
		}
	}
}

atom *
stack_set_var(mvc *sql, sql_schema *s, const char *name, ValRecord *v)
{
	(void) s; /* for now ignore sname */
	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->vars) {
			for (node *n = f->vars->h; n ; n = n->next) {
				sql_var *var = (sql_var*) n->data;
				if (var->name && !strcmp(var->name, name)) {
					VALclear(&(var->var.data));
					if (VALcopy(&(var->var.data), v) == NULL)
						return NULL;
					var->var.isnull = VALisnil(v);
					if (v->vtype == TYPE_flt)
						var->var.d = v->val.fval;
					else if (v->vtype == TYPE_dbl)
						var->var.d = v->val.dval;
					return &(var->var);
				}
			}
		}
	}
	return NULL;
}

atom *
stack_get_var(mvc *sql, sql_schema *s, const char *name)
{
	(void) s; /* for now ignore sname */
	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->vars) {
			for (node *n = f->vars->h; n ; n = n->next) {
				sql_var *var = (sql_var*) n->data;
				if (var->name && !strcmp(var->name, name))
					return &(var->var);
			}
		}
	}
	return NULL;
}

sql_frame*
stack_push_frame(mvc *sql, const char *name)
{
	sql_frame *v, **nvars;
	int nextsize = sql->sizeframes;

	if (sql->topframes == nextsize) {
		nextsize <<= 1;
		if (!(nvars = RENEW_ARRAY(sql_frame*, sql->frames, nextsize)))
			return NULL;
		sql->frames = nvars;
		sql->sizeframes = nextsize;
	}
	if (!(v = ZNEW(sql_frame)))
		return NULL;
	if (name && !(v->name = _STRDUP(name))) {
		_DELETE(v);
		return NULL;
	}
	v->frame_number = sql->frame++;
	sql->frames[sql->topframes++] = v;
	return v;
}

void
clear_frame(mvc *sql, sql_frame *frame)
{
	list_destroy(frame->group_expressions);
	list_destroy(frame->windows);
	list_destroy(frame->tables);
	list_destroy(frame->rel_views);
	list_destroy(frame->vars);
	_DELETE(frame->name);
	_DELETE(frame);
	sql->frame--;
}

void
stack_pop_until(mvc *sql, int frame) 
{
	while (sql->topframes > frame) {
		assert(sql->topframes >= 0);
		sql_frame *f = sql->frames[--sql->topframes];
		clear_frame(sql, f);
	}
}

void
stack_pop_frame(mvc *sql)
{
	sql_frame *f = sql->frames[--sql->topframes];
	assert(sql->topframes >= 0);
	clear_frame(sql, f);
}

sql_subtype *
stack_find_type(mvc *sql, const char *name)
{
	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->vars) {
			for (node *n = f->vars->h; n ; n = n->next) {
				sql_var *var = (sql_var*) n->data;
				if (var->name && !strcmp(var->name, name))
					return &(var->var.tpe);
			}
		}
	}
	return NULL;
}

sql_table *
stack_find_table(mvc *sql, sql_schema *s, const char *name)
{
	(void) s; /* for now ignore sname */
	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->tables) {
			for (node *n = f->tables->h; n ; n = n->next) {
				sql_local_table *var = (sql_local_table*) n->data;
				if (var->name && !strcmp(var->name, name))
					return var->table;
			}
		}
	}
	return NULL;
}

sql_rel *
stack_find_rel_view(mvc *sql, const char *name)
{
	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->rel_views) {
			for (node *n = f->rel_views->h; n ; n = n->next) {
				sql_rel_view *var = (sql_rel_view*) n->data;
				if (var->name && !strcmp(var->name, name))
					return rel_dup(var->rel_view);
			}
		}
	}
	return NULL;
}

void 
stack_update_rel_view(mvc *sql, const char *name, sql_rel *view)
{
	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->rel_views) {
			for (node *n = f->rel_views->h; n ; n = n->next) {
				sql_rel_view *var = (sql_rel_view*) n->data;
				if (var->name && !strcmp(var->name, name)) {
					rel_destroy(var->rel_view);
					var->rel_view = view;
					return;
				}
			}
		}
	}
}

int 
stack_find_var(mvc *sql, sql_schema *s, const char *name)
{
	(void) s; /* for now ignore sname */
	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->vars) {
			for (node *n = f->vars->h; n ; n = n->next) {
				sql_var *var = (sql_var*) n->data;
				if (var->name && !strcmp(var->name, name))
					return 1;
			}
		}
	}
	return 0;
}

int 
frame_find_var(mvc *sql, sql_schema *s, const char *name)
{
	(void) s; /* for now ignore sname */
	sql_frame *f = sql->frames[sql->topframes - 1];
	if (f->vars) {
		for (node *n = f->vars->h; n ; n = n->next) {
			sql_var *var = (sql_var*) n->data;
			if (var->name && !strcmp(var->name, name))
				return 1;
		}
	}
	return 0;
}

int
stack_find_frame(mvc *sql, sql_schema *s, const char *name)
{
	(void) s; /* for now ignore sname */
	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->vars) {
			for (node *n = f->vars->h; n ; n = n->next) {
				sql_var *var = (sql_var*) n->data;
				if (var->name && !strcmp(var->name, name))
					return f->frame_number;
			}
		}
	}
	return 0;
}

int
stack_has_frame(mvc *sql, const char *name)
{
	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->name && !strcmp(f->name, name))
			return 1;
	}
	return 0;
}

int
stack_nr_of_declared_tables(mvc *sql)
{
	int dt = 0;

	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		dt += list_length(f->tables);
	}
	return dt;
}

str
stack_set_string(mvc *sql, sql_schema *s, const char *name, const char *val)
{
	atom *a = stack_get_var(sql, s, name);
	str new_val = _STRDUP(val);

	if (a != NULL && new_val != NULL) {
		ValRecord *v = &a->data;

		if (v->val.sval)
			_DELETE(v->val.sval);
		v->val.sval = new_val;
		return new_val;
	} else if (new_val) {
		_DELETE(new_val);
	}
	return NULL;
}

str
stack_get_string(mvc *sql, sql_schema *s, const char *name)
{
	atom *a = stack_get_var(sql, s, name);

	if (!a || a->data.vtype != TYPE_str)
		return NULL;
	return a->data.val.sval;
}

void
#ifdef HAVE_HGE
stack_set_number(mvc *sql, sql_schema *s, const char *name, hge val)
#else
stack_set_number(mvc *sql, sql_schema *s, const char *name, lng val)
#endif
{
	atom *a = stack_get_var(sql, s, name);

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
stack_get_number(mvc *sql, sql_schema *s, const char *name)
{
	atom *a = stack_get_var(sql, s, name);
	return val_get_number(a?&a->data:NULL);
}

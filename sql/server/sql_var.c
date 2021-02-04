/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_mvc.h"
#include "sql_scan.h"
#include "sql_list.h"
#include "sql_types.h"
#include "sql_catalog.h"
#include "sql_datetime.h"
#include "sql_atom.h"
#include "rel_rel.h"

static void
destroy_sql_var(void *gdata, void *data)
{
	(void)gdata;
	sql_var *svar = (sql_var*) data;
	VALclear(&(svar->var.data));
	svar->var.data.vtype = 0;
	_DELETE(svar->sname);
	_DELETE(svar->name);
	_DELETE(svar);
}

#define SQLglobal(sname, name, val) \
	if (!(var = push_global_var(sql, sname, name, &ctype)) || !sqlvar_set(var, VALset(&src, ctype.type->localtype, (char*)(val)))) \
		return -1;

static int
var_key(sql_var *v)
{
	return hash_key(v->name);
}

int
init_global_variables(mvc *sql)
{
	sql_subtype ctype;
	lng sec = 0;
	ValRecord src;
	const char *opt, *sname = "sys";
	sql_var *var;

	if (!(sql->global_vars = list_create(destroy_sql_var)))
		return -1;

	sql_find_subtype(&ctype, "int", 0, 0);
	SQLglobal(sname, "debug", &sql->debug);

	sql_find_subtype(&ctype,  "varchar", 1024, 0);
	SQLglobal(sname, "current_schema", sname);
	SQLglobal(sname, "current_user", "monetdb");
	SQLglobal(sname, "current_role", "monetdb");

	/* inherit the optimizer from the server */
	opt = GDKgetenv("sql_optimizer");
	if (!opt)
		opt = "default_pipe";
	SQLglobal(sname, "optimizer", opt);

	sql_find_subtype(&ctype, "sec_interval", inttype2digits(ihour, isec), 0);
	SQLglobal(sname, "current_timezone", &sec);

	sql_find_subtype(&ctype, "bigint", 0, 0);
	SQLglobal(sname, "last_id", &sec);
	SQLglobal(sname, "rowcnt", &sec);

	/* Use hash lookup for global variables. With 9 variables in total it's worth to hash instead of */
	if (!(sql->global_vars->ht = hash_new(NULL, list_length(sql->global_vars), (fkeyvalue)&var_key)))
		return -1;

	for (node *n = sql->global_vars->h; n; n = n->next) {
		sql_var *v = n->data;
		int key = var_key(v);

		if (!hash_add(sql->global_vars->ht, key, v))
			return -1;
	}

	return 0;
}

sql_var*
push_global_var(mvc *sql, const char *sname, const char *name, sql_subtype *type)
{
	sql_var *svar = ZNEW(sql_var);

	if (!svar)
		return NULL;
	if (!(svar->name = _STRDUP(name))) {
		_DELETE(svar);
		return NULL;
	}
	if (!(svar->sname = _STRDUP(sname))) {
		_DELETE(svar->name);
		_DELETE(svar);
		return NULL;
	}
	atom_init(&(svar->var));
	if (type) {
		int tpe = type->type->localtype;
		VALset(&(svar->var.data), tpe, (ptr) ATOMnilptr(tpe));
		svar->var.tpe = *type;
	}
	if (!list_append(sql->global_vars, svar)) {
		_DELETE(svar->name);
		_DELETE(svar->sname);
		_DELETE(svar);
		return NULL;
	}
	return svar;
}

sql_var*
frame_push_var(mvc *sql, const char *name, sql_subtype *type)
{
	assert(sql->topframes > 0);
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_var *svar = ZNEW(sql_var);

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
destroy_sql_local_table(void *gdata, void *data)
{
	(void)gdata;
	sql_local_table *slt = (sql_local_table*) data;
	_DELETE(slt);
}

sql_local_table*
frame_push_table(mvc *sql, sql_table *t)
{
	assert(sql->topframes > 0);
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_local_table *slt = ZNEW(sql_local_table);

	if (!slt)
		return NULL;
	slt->table = t;
	t->s = NULL;
	if (!f->tables && !(f->tables = list_create(destroy_sql_local_table))) {
		_DELETE(slt);
		return NULL;
	}
	if (!list_append(f->tables, slt)) {
		_DELETE(slt);
		return NULL;
	}
	return slt;
}

static void
destroy_sql_rel_view(void *gdata, void *data)
{
	(void)gdata;
	sql_rel_view *srv = (sql_rel_view*) data;
	rel_destroy(srv->rel_view);
	_DELETE(srv->name);
	_DELETE(srv);
}

sql_rel_view*
stack_push_rel_view(mvc *sql, const char *name, sql_rel *var)
{
	assert(sql->topframes > 0);
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_rel_view *srv = ZNEW(sql_rel_view);

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
destroy_sql_window_definition(void *gdata, void *data)
{
	(void)gdata;
	sql_window_definition *swd = (sql_window_definition*) data;
	_DELETE(swd->name);
	_DELETE(swd);
}

sql_window_definition*
frame_push_window_def(mvc *sql, const char *name, dlist *wdef)
{
	assert(sql->topframes > 0);
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_window_definition *swd = ZNEW(sql_window_definition);

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
destroy_sql_groupby_expression(void *gdata, void *data)
{
	(void)gdata;
	sql_groupby_expression *sge = (sql_groupby_expression*) data;
	_DELETE(sge);
}

sql_groupby_expression*
frame_push_groupby_expression(mvc *sql, symbol *def, sql_exp *exp)
{
	assert(sql->topframes > 0);
	sql_frame *f = sql->frames[sql->topframes - 1];
	sql_groupby_expression *sge = ZNEW(sql_groupby_expression);

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
frame_get_window_def(mvc *sql, const char *name, int *pos)
{
	if (sql->topframes > 0) {
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
	}
	return NULL;
}

sql_exp*
frame_get_groupby_expression(mvc *sql, symbol *def)
{
	if (sql->topframes > 0) {
		sql_frame *f = sql->frames[sql->topframes - 1];
		if (f->group_expressions) {
			for (node *n = f->group_expressions->h; n ; n = n->next) {
				sql_groupby_expression *var = (sql_groupby_expression*) n->data;
				if (var->token == def->token && !symbol_cmp(sql, var->sdef, def))
					return var->exp;
			}
		}
	}
	return NULL;
}

/* There could a possibility that this is vulnerable to a time-of-check, time-of-use race condition.
 * However this should never happen in the SQL compiler */
bool
frame_check_var_visited(mvc *sql, int i)
{
	if (sql->topframes > 0) {
		sql_frame *f = sql->frames[sql->topframes - 1];
		sql_window_definition *win;

		if (i < 0 || i >= list_length(f->windows))
			return false;

		win = (sql_window_definition*) list_fetch(f->windows, i);
		return win->visited;
	}
	return false;
}

void
frame_set_var_visited(mvc *sql, int i)
{
	if (sql->topframes > 0) {
		sql_frame *f = sql->frames[sql->topframes - 1];
		sql_window_definition *win;

		if (i < 0 || i >= list_length(f->windows))
			return;

		win = (sql_window_definition*) list_fetch(f->windows, i);
		win->visited = true;
	}
}

void
frame_clear_visited_flag(mvc *sql)
{
	if (sql->topframes > 0) {
		sql_frame *f = sql->frames[sql->topframes - 1];
		if (f->windows) {
			for (node *n = f->windows->h; n ; n = n->next) {
				sql_window_definition *var = (sql_window_definition*) n->data;
				var->visited = false;
			}
		}
	}
}

atom *
sqlvar_set(sql_var *var, ValRecord *v)
{
	VALclear(&(var->var.data));
	if (VALcopy(&(var->var.data), v) == NULL)
		return NULL;
	var->var.isnull = VALisnil(v);
	return &(var->var);
}

sql_frame*
stack_push_frame(mvc *sql, const char *name)
{
	sql_frame *v, **nvars;
	int osize = sql->sizeframes, nextsize = osize;

	if (sql->topframes == nextsize) {
		nextsize <<= 1;
		if (!(nvars = SA_RENEW_ARRAY(sql->pa, sql_frame*, sql->frames, nextsize, osize)))
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
	v->frame_number = ++sql->frame; /* The frame number for varialbes on the stack start on level 1 */
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

sql_table *
frame_find_table(mvc *sql, const char *name)
{
	if (sql->topframes > 0) {
		sql_frame *f = sql->frames[sql->topframes - 1];
		if (f->tables) {
			for (node *n = f->tables->h; n ; n = n->next) {
				sql_local_table *var = (sql_local_table*) n->data;
				if (!strcmp(var->table->base.name, name))
					return var->table;
			}
		}
	}
	return NULL;
}

sql_table *
stack_find_table(mvc *sql, const char *name)
{
	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->tables) {
			for (node *n = f->tables->h; n ; n = n->next) {
				sql_local_table *var = (sql_local_table*) n->data;
				if (!strcmp(var->table->base.name, name))
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
				assert(var->name);
				if (!strcmp(var->name, name))
					return rel_dup(var->rel_view);
			}
		}
	}
	return NULL;
}

int
stack_find_rel_view_projection_columns(mvc *sql, const char *name, sql_rel **res)
{
	*res = NULL;

	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->rel_views) {
			for (node *n = f->rel_views->h; n ; n = n->next) {
				sql_rel_view *var = (sql_rel_view*) n->data;

				assert(var->name);
				 /* trigger views are basetables relations, so those may conflict */
				if (is_base(var->rel_view->op) && rel_bind_column(sql, var->rel_view, name, 0, 0)) {
					if (*res)
						return -1;
					*res = var->rel_view;
				}
			}
		}
	}
	return 0;
}

sql_rel *
frame_find_rel_view(mvc *sql, const char *name)
{
	assert(sql->topframes > 0);
	sql_frame *f = sql->frames[sql->topframes - 1];
	if (f->rel_views) {
		for (node *n = f->rel_views->h; n ; n = n->next) {
			sql_rel_view *var = (sql_rel_view*) n->data;
				assert(var->name);
				if (!strcmp(var->name, name))
					return var->rel_view;
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
				assert(var->name);
				if (!strcmp(var->name, name)) {
					rel_destroy(var->rel_view);
					var->rel_view = view;
					return;
				}
			}
		}
	}
}

sql_var*
find_global_var(mvc *sql, sql_schema *s, const char *name)
{
	const char *sname = s->base.name;
	int key = hash_key(name); /* Using hash lookup */
	sql_hash_e *he = sql->global_vars->ht->buckets[key&(sql->global_vars->ht->size-1)];

	for (; he; he = he->chain) {
		sql_var *var = (sql_var*) he->value;

		assert(var->sname && var->name);
		if (!strcmp(var->sname, sname) && !strcmp(var->name, name))
			return var;
	}
	return NULL;
}

int
frame_find_var(mvc *sql, const char *name)
{
	assert(sql->topframes > 0);
	sql_frame *f = sql->frames[sql->topframes - 1];
	if (f->vars) {
		for (node *n = f->vars->h; n ; n = n->next) {
			sql_var *var = (sql_var*) n->data;
			assert(var->name);
			if (!strcmp(var->name, name))
				return 1;
		}
	}
	return 0;
}

sql_var*
stack_find_var_frame(mvc *sql, const char *name, int *level)
{
	*level = 1; /* Level 0 is for globals */
	for (int i = sql->topframes-1; i >= 0; i--) {
		sql_frame *f = sql->frames[i];
		if (f->vars) {
			for (node *n = f->vars->h; n ; n = n->next) {
				sql_var *var = (sql_var*) n->data;
				assert(var->name);
				if (!strcmp(var->name, name)) {
					*level = f->frame_number;
					return var;
				}
			}
		}
	}
	return NULL;
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
sqlvar_set_string(sql_var *var, const char *val)
{
	atom *a = &var->var;
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
sqlvar_get_string(sql_var *var)
{
	atom *a = &var->var;

	if (!a || a->data.vtype != TYPE_str)
		return NULL;
	return a->data.val.sval;
}

void
#ifdef HAVE_HGE
sqlvar_set_number(sql_var *var, hge val)
#else
sqlvar_set_number(sql_var *var, lng val)
#endif
{
	atom *a = &var->var;

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

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
#include "sql_mem.h"
#include "sql_stack.h"
#include "sql_statement.h"
#include <string.h>

const char *
st_type2string(st_type type)
{
	switch (type) {
#define ST(TYPE) case st_##TYPE : return #TYPE
		ST(none);
		ST(var);

		ST(basetable);
		ST(table);
		ST(temp);
		ST(single);

		ST(rs_column);

		ST(bat);
		ST(dbat);
		ST(idxbat);

		ST(const);

		ST(mark);
		ST(gen_group);
		ST(reverse);
		ST(mirror);

		ST(limit);
		ST(limit2);
		ST(order);
		ST(reorder);

		ST(ordered);
		ST(output);
		ST(affected_rows);

		ST(atom);

		ST(select);
		ST(select2);
		ST(uselect);
		ST(selectN);
		ST(uselect2);
		ST(uselectN);
		ST(semijoin);
		ST(relselect);

		ST(releqjoin);
		ST(join);
		ST(join2);
		ST(joinN);
		ST(outerjoin);
		ST(diff);
		ST(union);
		ST(reljoin);

		ST(export);
		ST(append);
		ST(table_clear);
		ST(exception);
		ST(trans);
		ST(catalog);

		ST(append_col);
		ST(append_idx);
		ST(update_col);
		ST(update_idx);
		ST(delete);

		ST(group_ext);
		ST(group);

		ST(derive);
		ST(unique);
		ST(convert);
		ST(unop);
		ST(binop);
		ST(Nop);
		ST(aggr);

		ST(alias);
		ST(connection);

		ST(list);
	
		ST(cond);
		ST(control_end);
		ST(return);
		ST(assign);
	}
	return "unknown";	/* just needed for broken compilers ! */
}

/* #TODO make proper traversal operations */
stmt *
stmt_atom_string(sql_allocator *sa, char *S)
{
	char *s = sql2str(S);
	sql_subtype t; 

	sql_find_subtype(&t, "varchar", _strlen(s), 0);
	return stmt_atom(sa, atom_string(sa, &t, s));
}

stmt *
stmt_atom_string_nil(sql_allocator *sa)
{
	sql_subtype t; 

	sql_find_subtype(&t, "clob", 0, 0);
	return stmt_atom(sa, atom_string(sa, &t, NULL));
}

stmt *
stmt_atom_clob(sql_allocator *sa, char *S)
{
	char *s = sql2str(S);
	sql_subtype t; 

	sql_find_subtype(&t, "clob", _strlen(s), 0);
	return stmt_atom(sa, atom_string(sa, &t, s));
}

stmt *
stmt_atom_int(sql_allocator *sa, int i)
{
	sql_subtype t;

	sql_find_subtype(&t, "int", 32, 0);
	return stmt_atom(sa, atom_int(sa, &t, i));
}

stmt *
stmt_atom_wrd(sql_allocator *sa, wrd i)
{
	sql_subtype t;

	if (sizeof(wrd) == sizeof(int))
		sql_find_subtype(&t, "wrd", 32, 0);
	else
		sql_find_subtype(&t, "wrd", 64, 0);
	return stmt_atom(sa, atom_int(sa, &t, i));
}

stmt *
stmt_atom_wrd_nil(sql_allocator *sa)
{
	sql_subtype t;

	if (sizeof(wrd) == sizeof(int))
		sql_find_subtype(&t, "wrd", 32, 0);
	else
		sql_find_subtype(&t, "wrd", 64, 0);
	return stmt_atom(sa, atom_general(sa, &t, NULL));
}

stmt *
stmt_atom_lng(sql_allocator *sa, lng l)
{
	sql_subtype t;

	sql_find_subtype(&t, "bigint", 64, 0);
	return stmt_atom(sa, atom_int(sa, &t, l));
}

stmt *
stmt_bool(sql_allocator *sa, int b)
{
	sql_subtype t;

	sql_find_subtype(&t, "boolean", 0, 0);
	if (b) {
		return stmt_atom(sa, atom_bool(sa, &t, TRUE));
	} else {
		return stmt_atom(sa, atom_bool(sa, &t, FALSE));
	}
}

static stmt *
stmt_atom_oid(sql_allocator *sa, oid i)
{
	sql_subtype t;

	sql_find_subtype(&t, "oid", 0, 0);
	return stmt_atom(sa, atom_int(sa, &t, i));
}

static stmt *
stmt_create(sql_allocator *sa, st_type type)
{
	stmt *s = SA_NEW(sa, stmt);

	s->type = type;
	s->op1 = NULL;
	s->op2 = NULL;
	s->op3 = NULL;
	s->op4.lval = NULL;
	s->flag = 0;
	s->nrcols = 0;
	s->key = 0;
	s->aggr = 0;
	s->nr = 0;
	s->h = NULL;
	s->t = NULL;
	s->optimized = -1;
	s->rewritten = NULL;
	return s;
}

static stmt *
stmt_ext(sql_allocator *sa, stmt *grp)
{
	stmt *ns = stmt_create(sa, st_group_ext);

	ns->op1 = grp;
	ns->nrcols = grp->nrcols;
	ns->key = 1;
	ns->h = grp->h;
	ns->t = grp->t;
	return ns;
}

stmt *
stmt_group(sql_allocator *sa, stmt *s)
{
	stmt *ns = stmt_create(sa, st_group);

	ns->op1 = s;
	ns->nrcols = s->nrcols;
	ns->key = 0;
	ns->h = s->h;
	ns->t = s->t;
	return ns;
}

stmt *
stmt_derive(sql_allocator *sa, stmt *s, stmt *t)
{
	stmt *ns = stmt_create(sa, st_derive);

	ns->op1 = s;
	ns->op2 = t;
	ns->nrcols = s->nrcols;
	ns->key = 0;
	ns->h = s->h;
	ns->t = s->t;
	return ns;
}

group *
grp_create(sql_allocator *sa, stmt *s, group *og)
{
	group *g = SA_NEW(sa, group);

	if (og) {
		g->grp = stmt_derive(sa, og->grp, s);
	} else {
		g->grp = stmt_group(sa, s);
	}
	g->ext = stmt_ext(sa, g->grp);
	return g;
}

void 
grp_done(group *g)
{
	if (g && g->grp)
		g->grp->flag = GRP_DONE;
}

static void stmt_deps( list *dep_list, stmt *s, int depend_type, int dir);

static int
id_cmp(int *id1, int *id2)
{
	if (*id1 == *id2)
		return 0;
	return -1;
}

static list *
cond_append( list *l, int *id )
{
	if (!list_find(l, id, (fcmp)&id_cmp))
		list_append(l, id);
	return l;
}

static void 
list_deps(list *dep_list, list *l, int depend_type, int dir)
{
	if (l) {
		node *n;
		for (n = l->h; n; n = n->next) 
			stmt_deps(dep_list, n->data, depend_type, dir);
	}
}

#define push(s) stack[top++] = s
#define pop()	stack[--top]
static void 
stmt_deps(list *dep_list, stmt *s, int depend_type, int dir)
{
	stmt **stack;
	int top = 0, sz = 1024;
 
	stack = NEW_ARRAY(stmt*, sz+1);
	if (stack == NULL)
		return;
	push(NULL);
	push(s);
	while((s=pop()) != NULL) {
	   if ((dir < 0 && s->optimized < 0) || (dir >=0 && s->optimized >= 0)){
		switch (s->type) {
		case st_list:
			list_deps(dep_list, s->op4.lval, depend_type, dir);
			break;
		/* simple case of statements of only statements */
		case st_relselect:
		case st_releqjoin: 
		case st_reljoin:
		case st_diff:
		case st_alias:
		case st_union:
		case st_join:
		case st_join2:
		case st_joinN:
		case st_outerjoin:
		case st_derive:
		case st_unique:
		case st_append:
		case st_rs_column:

		case st_cond:
		case st_control_end:
		case st_return:
		case st_assign:
		case st_exception:
		case st_table:
		case st_export:
		case st_convert:
		case st_const:
		case st_mark:
		case st_gen_group:
		case st_reverse:
		case st_mirror:
		case st_limit:
		case st_limit2:
		case st_order:
		case st_reorder:
		case st_ordered:
		case st_output:
		case st_affected_rows:

		case st_group:
		case st_group_ext:

		case st_select:
		case st_select2:
		case st_selectN:
		case st_uselect:
		case st_uselect2:
		case st_uselectN:
		case st_semijoin:
		case st_connection:
			if (s->op1)
				push(s->op1);
			if (s->op2)
				push(s->op2);
			if (s->op3)
				push(s->op3);
			break;

		/* special cases */
		case st_basetable:
			if (depend_type == COLUMN_DEPENDENCY) {
				dep_list = cond_append(dep_list, &s->op4.tval->base.id);	
			}
			break;
		case st_table_clear:
			if(depend_type == TRIGGER_DEPENDENCY) {
				dep_list = cond_append(dep_list, &s->op4.tval->base.id);
			}
			break;
		case st_bat:
		case st_append_col:
		case st_update_col:
			if (depend_type == COLUMN_DEPENDENCY) { 
			    	if (isTable(s->op4.cval->t)) 
					dep_list = cond_append(dep_list, &s->op4.cval->base.id);
				dep_list = cond_append(dep_list, &s->op4.cval->t->base.id);
			}
			break;
		case st_aggr:
			if (s->op1)
				push(s->op1);
			if (s->op2)
				push(s->op2);
			if (s->op3)
				push(s->op3);
			if (depend_type == FUNC_DEPENDENCY) {
				dep_list = cond_append(dep_list, &s->op4.aggrval->aggr->base.id);	
			}
			break;
		case st_unop:
		case st_binop:
		case st_Nop:
			if (s->op1)
				push(s->op1);
			if (s->op2)
				push(s->op2);
			if (s->op3)
				push(s->op3);
			if (depend_type == FUNC_DEPENDENCY) {
				dep_list = cond_append(dep_list, &s->op4.funcval->func->base.id);	
			}
			break;
		/* skip */
		case st_append_idx:
		case st_update_idx:
		case st_delete:
		case st_dbat:
		case st_idxbat:
		case st_none:
		case st_var:
		case st_temp:
		case st_single:
		case st_atom:
		case st_trans:
		case st_catalog:
			break;
		}
	    }
	    if (top+10 >= sz) {
		sz *= 2;
		stack = RENEW_ARRAY(stmt*, stack, sz);
	    }
	    if (dir < 0)
		s->optimized = 0;
	    else
		s->optimized = -1;
	}
	_DELETE(stack);
}

list* stmt_list_dependencies(sql_allocator *sa, stmt *s, int depend_type)
{
	list *dep_list = list_new(sa);

	stmt_deps(dep_list, s, depend_type, s->optimized);
	return dep_list;
}

stmt *
stmt_none(sql_allocator *sa)
{
	return stmt_create(sa, st_none);
}

stmt *
stmt_var(sql_allocator *sa, char *varname, sql_subtype *t, int declare, int level)
{
	stmt *s = stmt_create(sa, st_var);

	s->op1 = stmt_atom_string(sa, varname);
	if (t)
		s->op4.typeval = *t;
	else
		s->op4.typeval.type = NULL;
	s->flag = declare + (level<<1);
	s->key = 1;
	return s;
}

stmt *
stmt_varnr(sql_allocator *sa, int nr, sql_subtype *t)
{
	stmt *s = stmt_create(sa, st_var);

	s->op1 = NULL;
	if (t)
		s->op4.typeval = *t;
	else
		s->op4.typeval.type = NULL;
	s->flag = nr;
	s->key = 1;
	return s;
}

stmt *
stmt_table(sql_allocator *sa, stmt *cols, int temp)
{
	stmt *s = stmt_create(sa, st_table);

	s->op1 = cols;
	s->flag = temp;
	return s;
}

stmt *
stmt_basetable(sql_allocator *sa, sql_table *t, char *name)
{
	stmt *s = stmt_create(sa, st_basetable);

	s->op1 = stmt_atom_string(sa, name);
	s->op4.tval = t;
	return s;
}

stmt *
stmt_temp(sql_allocator *sa, sql_subtype *t)
{
	stmt *s = stmt_create(sa, st_temp);

	s->op4.typeval = *t;
	s->nrcols = 1;
	return s;
}

stmt *
stmt_bat(sql_allocator *sa, sql_column *c, stmt *basetable, int access )
{
	stmt *s = stmt_create(sa, st_bat);

	s->op4.cval = c;
	s->nrcols = 1;
	s->flag = access;
	s->h = basetable;	/* oid's used from this basetable */
	return s;
}

static stmt *
stmt_tbat(sql_allocator *sa, sql_table *t, int access)
{
	stmt *s = stmt_create(sa, st_dbat);

	assert(access == RD_INS);

	s->nrcols = 0;
	s->flag = access;
	s->op4.tval = t;
	return s;
}

stmt *
stmt_delta_table_bat(sql_allocator *sa, sql_column *c, stmt *basetable, int access )
{
	stmt *s = stmt_bat(sa, c, basetable, access );

	if (c->t->readonly)
		return s;

	if (isTable(c->t) &&
	   (c->base.flag != TR_NEW || c->t->base.flag != TR_NEW /* alter */) &&
	    access == RDONLY && c->t->persistence == SQL_PERSIST && !c->t->commit_action) {
		stmt *i = stmt_bat(sa, c, basetable, RD_INS );
		stmt *u = stmt_bat(sa, c, basetable, RD_UPD );

		s = stmt_diff(sa, s, u);
		s = stmt_union(sa, s, u);
		s = stmt_union(sa, s, i);
	} 
	/* even temp tables have deletes because we like to keep void heads */
	if (access == RDONLY && isTable(c->t)) {
		stmt *d = stmt_tbat(sa, c->t, RD_INS);
		s = stmt_diff(sa, s, stmt_reverse(sa, d));
	}
	return s;
}

stmt *
stmt_idxbat(sql_allocator *sa, sql_idx * i, int access)
{
	stmt *s = stmt_create(sa, st_idxbat);

	s->op4.idxval = i;
	s->nrcols = 1;
	s->flag = access;
	return s;
}

stmt *
stmt_delta_table_idxbat(sql_allocator *sa, sql_idx * idx, int access)
{
	stmt *s = stmt_idxbat(sa, idx, access);

	if (idx->t->readonly)
		return s;

	if (isTable(idx->t) &&
	   (idx->base.flag != TR_NEW || idx->t->base.flag != TR_NEW /* alter */) && 
	    access == RDONLY && idx->t->persistence == SQL_PERSIST && !idx->t->commit_action) {
		stmt *i = stmt_idxbat(sa, idx, RD_INS);
		stmt *u = stmt_idxbat(sa, idx, RD_UPD);

		s = stmt_diff(sa, s, u);
		s = stmt_union(sa, s, u);
		s = stmt_union(sa, s, i);
	} 
	/* even temp tables have deletes because we like to keep void heads */
	if (access == RDONLY && isTable(idx->t)) {
		stmt *d = stmt_tbat(sa, idx->t, RD_INS);
		s = stmt_diff(sa, s, stmt_reverse(sa, d));
	}
	return s;
}

stmt *
stmt_append_col(sql_allocator *sa, sql_column *c, stmt *b)
{
	stmt *s = stmt_create(sa, st_append_col);

	s->op1 = b;
	s->op4.cval = c;
	return s;
}

stmt *
stmt_append_idx(sql_allocator *sa, sql_idx *i, stmt *b)
{
	stmt *s = stmt_create(sa, st_append_idx);

	s->op1 = b;
	s->op4.idxval = i;
	return s;
}

stmt *
stmt_update_col(sql_allocator *sa, sql_column *c, stmt *b)
{
	stmt *s = stmt_create(sa, st_update_col);

	s->op1 = b;
	s->op4.cval = c;
	return s;
}

stmt *
stmt_update_idx(sql_allocator *sa, sql_idx *i, stmt *b)
{
	stmt *s = stmt_create(sa, st_update_idx);

	s->op1 = b;
	s->op4.idxval = i;
	return s;
}

stmt *
stmt_delete(sql_allocator *sa, sql_table *t, stmt *b)
{
	stmt *s = stmt_create(sa, st_delete);

	s->op1 = b;
	s->op4.tval = t;
	return s;
}

static stmt *
stmt_const_(sql_allocator *sa, stmt *s, stmt *val)
{
	stmt *ns = stmt_create(sa, st_const);

	ns->op1 = s;
	ns->op2 = val;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	ns->h = s->h;
	return ns;
}

static stmt *
push_project(sql_allocator *sa, stmt *rows, stmt *val) 
{
	switch (val->type) {
	case st_convert:
		val->op1 = push_project(sa, rows, val->op1);
		break;
	case st_Nop:
		if (val->op4.funcval->func->side_effect) {
			stmt *l = val->op1;
			node *n = l->op4.lval->h;
			if (n) {
				n->data = stmt_const_(sa, rows, n->data);
			} else {  /* no args, ie. change into a st_unop */
				val->type = st_unop;
				val->op1 = stmt_const_(sa, rows, stmt_atom_int(sa, 0));
			}
		} else {
			/* push through arguments of Nop */
			node *n;
			stmt *l = val->op1;

			for(n = l->op4.lval->h; n; n = n->next) 
				n->data = push_project(sa, rows, n->data);
		}
		break;
	case st_binop:
		if (val->op4.funcval->func->side_effect) {
			val->op1 = stmt_const_(sa, rows, val->op1);
		} else {
			val->op1 = push_project(sa, rows, val->op1);
			val->op2 = push_project(sa, rows, val->op2);
		}
		break;
	case st_unop:
		if (val->op4.funcval->func->side_effect) {
			val->op1 = stmt_const_(sa, rows, val->op1);
		} else {
			val->op1 = push_project(sa, rows, val->op1);
		}
		break;
	default:
		if (!val->nrcols)
			val = stmt_const_(sa, rows, val);
		return val;
	}
	val->nrcols = rows->nrcols;
	return val;
}

static int
has_side_effect(stmt *val)
{
	int se = 0;

	switch (val->type) {
	case st_convert:
		se = has_side_effect(val->op1);
		break;
	case st_Nop:
		se = val->op4.funcval->func->side_effect;
		if (!se) {
			stmt *l = val->op1;
			node *n;
			for (n=l->op4.lval->h; n; n = n->next) 
				se += has_side_effect(n->data);
		}
		break;
	case st_binop:
		se = val->op4.funcval->func->side_effect;
		if (!se) 
			se = has_side_effect(val->op1) + 
		     	     has_side_effect(val->op2);
		break;
	case st_unop:
		se = val->op4.funcval->func->side_effect;
		if (!se) 
			se = has_side_effect(val->op1);
		break;
	default:
		return se;
	}
	return se;
}

stmt *
stmt_const(sql_allocator *sa, stmt *rows, stmt *val) 
{
	if (val && has_side_effect(val)) {
		stmt *x = push_project(sa, rows, val);
		return x;
	} else {
		return stmt_const_(sa, rows, val);
	}
}

/* BEWARE stmt_mark marks the head, this while the mil mark is a mark tail
 * Current implementation adds the reverses in stmt_mark nolonger in
 * the generated code.
*/
stmt *
stmt_mark(sql_allocator *sa, stmt *s, int id)
{
	stmt *ns = stmt_create(sa, st_mark);

	ns->op1 = stmt_reverse(sa, s);
	ns->op2 = stmt_atom_oid(sa, id);

	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	ns->h = s->t;
	return stmt_reverse(sa, ns);
}

stmt *
stmt_mark_tail(sql_allocator *sa, stmt *s, int id)
{
	stmt *ns = stmt_create(sa, st_mark);

	ns->op1 = s;
	ns->op2 = stmt_atom_oid(sa, id);

	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	ns->h = s->h;
	return ns;
}

stmt *
stmt_gen_group(sql_allocator *sa, stmt *s)
{
	stmt *ns = stmt_create(sa, st_gen_group);

	ns->op1 = s;

	ns->nrcols = s->nrcols;
	ns->key = 0;
	ns->aggr = 0;
	ns->h = s->h;
	return ns;
}

stmt *
stmt_reverse(sql_allocator *sa, stmt *s)
{
	stmt *ns = stmt_create(sa, st_reverse);

	ns->op1 = s;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	ns->h = s->t;
	ns->t = s->h;
	return ns;
}

stmt *
stmt_mirror(sql_allocator *sa, stmt *s)
{
	stmt *ns = stmt_create(sa, st_mirror);

	ns->op1 = s;
	ns->nrcols = 2;
	ns->key = s->key;
	ns->aggr = s->aggr;
	ns->h = s->h;
	ns->t = s->h;
	return ns;
}

/* limit maybe atom nil */
stmt *
stmt_limit(sql_allocator *sa, stmt *s, stmt *offset, stmt *limit, int direction)
{
	stmt *ns = stmt_create(sa, st_limit);

	ns->op1 = s;
	ns->op2 = offset;
	ns->op3 = limit;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	ns->t = s->t;
	ns->flag = direction;
	return ns;
}

stmt *
stmt_limit2(sql_allocator *sa, stmt *a, stmt *b, stmt *offset, stmt *limit, int direction)
{
	stmt *ns = stmt_create(sa, st_limit2);

	ns->op1 = stmt_list(sa, list_append(list_append(list_new(sa),b), a));
	ns->op2 = offset;
	ns->op3 = limit;
	ns->nrcols = b->nrcols;
	ns->key = b->key;
	ns->aggr = b->aggr;
	ns->t = b->t;
	ns->flag = direction;
	return ns;
}

stmt *
stmt_order(sql_allocator *sa, stmt *s, int direction)
{
	stmt *ns = stmt_create(sa, st_order);

	ns->op1 = s;
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	ns->t = s->t;
	return ns;
}

stmt *
stmt_reorder(sql_allocator *sa, stmt *s, stmt *t, int direction)
{
	stmt *ns = stmt_create(sa, st_reorder);

	ns->op1 = s;
	ns->op2 = t;
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	ns->t = s->t;
	return ns;
}

stmt *
stmt_unique(sql_allocator *sa, stmt *s, group *g)
{
	stmt *ns = stmt_create(sa, st_unique);

	ns->op1 = s;
	if (g) 
		ns->op2 = g->grp;
	ns->nrcols = s->nrcols;
	ns->key = 1;		/* ?? maybe change key to unique ? */
	ns->aggr = s->aggr;
	ns->t = s->t;
	return ns;
}

stmt *
stmt_atom(sql_allocator *sa, atom *op1)
{
	stmt *s = stmt_create(sa, st_atom);

	s->op4.aval = op1;
	s->key = 1;		/* values are also unique */
	return s;
}

stmt *
stmt_relselect_init(sql_allocator *sa)
{
	stmt *s = stmt_create(sa, st_relselect);

	s->op1 = stmt_list(sa, list_new(sa));
	s->nrcols = 0;
	return s;
}

void
stmt_relselect_fill(stmt *rs, stmt *sel)
{
	list_append(rs->op1->op4.lval, sel);
	if (sel->nrcols > rs->nrcols)
		rs->nrcols = sel->nrcols;
	if (!rs->h)
		rs->h = ((stmt *) (rs->op1->op4.lval->h->data))->h;
}

stmt *
stmt_select(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype)
{
	stmt *s = stmt_create(sa, st_select);

	s->op1 = op1;
	s->op2 = op2;
	s->flag = cmptype;
        s->nrcols = (op1->nrcols==2)?2:1;
	s->h = s->op1->h;
	s->t = s->op1->t;
	return s;
}

stmt *
stmt_likeselect(sql_allocator *sa, stmt *op1, stmt *op2, stmt *op3, comp_type cmptype)
{
	stmt *s = stmt_create(sa, st_select);

	s->op1 = op1;
	s->op2 = op2;
	s->op3 = op3;
	s->flag = cmptype;
        s->nrcols = (op1->nrcols==2)?2:1;
	s->h = s->op1->h;
	s->t = s->op1->t;
	return s;
}

stmt *
stmt_select2(sql_allocator *sa, stmt *op1, stmt *op2, stmt *op3, int cmp)
{
	stmt *s = stmt_create(sa, st_select2);

	s->op1 = op1;
	s->op2 = op2;
	s->op3 = op3;
	s->flag = cmp;
        s->nrcols = (op1->nrcols==2)?2:1;
	s->h = s->op1->h;
	s->t = s->op1->t;
	return s;
}

stmt *
stmt_selectN(sql_allocator *sa, stmt *op1, stmt *op2, sql_subfunc *op)
{
	stmt *s = stmt_create(sa, st_selectN);

	s->op1 = op1;
	s->op2 = op2;
	s->op4.funcval = op;
        s->nrcols = (op1->nrcols==2)?2:1;
	s->h = s->op1->h;
	s->t = s->op1->t;
	return s;
}

stmt *
stmt_uselect(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype)
{
	stmt *s = stmt_create(sa, st_uselect);

	assert(cmptype != cmp_like && cmptype != cmp_notlike &&
	       cmptype != cmp_ilike && cmptype != cmp_notilike);
	s->op1 = op1;
	s->op2 = op2;
	s->flag = cmptype;
        s->nrcols = (op1->nrcols==2)?2:1;
	s->h = s->op1->h;
	return s;
}

stmt *
stmt_uselect2(sql_allocator *sa, stmt *op1, stmt *op2, stmt *op3, int cmp)
{
	stmt *s = stmt_create(sa, st_uselect2);

	s->op1 = op1;
	s->op2 = op2;
	s->op3 = op3;
	s->flag = cmp;
        s->nrcols = (op1->nrcols==2)?2:1;
	s->h = s->op1->h;
	return s;
}

stmt *
stmt_uselectN(sql_allocator *sa, stmt *op1, stmt *op2, sql_subfunc *op)
{
	stmt *s = stmt_create(sa, st_uselectN);

	s->op1 = op1;
	s->op2 = op2;
	s->op4.funcval = op;
        s->nrcols = (op1->nrcols==2)?2:1;
	s->h = s->op1->h;
	return s;
}

stmt *
stmt_semijoin(sql_allocator *sa, stmt *op1, stmt *op2)
{
	stmt *s = stmt_create(sa, st_semijoin);

	s->op1 = op1;
	s->op2 = op2;
	/* assert( op1->h == op2->h ); */
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

stmt *
stmt_reljoin(sql_allocator *sa, stmt *op1, list *neqjoins )
{
	stmt *s = stmt_create(sa, st_reljoin);

	s->op1 = op1;
	s->op2 = stmt_list(sa, neqjoins);
	s->nrcols = 2;
	if (!op1)
		op1 = neqjoins->h->data;
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

stmt *
stmt_releqjoin_init(sql_allocator *sa)
{
	stmt *s = stmt_create(sa, st_releqjoin);

	s->op1 = stmt_list(sa, list_new(sa));
	s->op2 = stmt_list(sa, list_new(sa));
	s->nrcols = 2;
	return s;
}

void
stmt_releqjoin_fill(stmt *rj, stmt *lc, stmt *rc)
{
	list_append(rj->op1->op4.lval, lc);
	list_append(rj->op2->op4.lval, rc);
	if (!rj->h)
		rj->h = ((stmt *) (rj->op1->op4.lval->h->data))->h;
	if (!rj->t)
		rj->t = ((stmt *) (rj->op2->op4.lval->h->data))->h;
}

stmt *
stmt_releqjoin2(sql_allocator *sa, list *l1, list *l2)
{
	stmt *s = stmt_create(sa, st_releqjoin);

	s->op1 = stmt_list(sa, l1);
	s->op2 = stmt_list(sa, l2);
	s->nrcols = 2;
	s->h = ((stmt *) (s->op1->op4.lval->h->data))->h;
	s->t = ((stmt *) (s->op2->op4.lval->h->data))->h;
	return s;
}

stmt *
stmt_releqjoin1(sql_allocator *sa, list *joins)
{
	list *l1 = list_new(sa);
	list *l2 = list_new(sa);
	stmt *L = NULL;
	node *n = NULL;

	for (n = joins->h; n; n = n->next) {
		stmt *l = ((stmt *) (n->data))->op1;
		stmt *r = ((stmt *) (n->data))->op2;

		while (l->type == st_reverse) 
			l = l->op1;
		while (r->type == st_reverse) 
			r = r->op1;
		if (l->t != r->t) 
			r = stmt_reverse(sa, r);
		if (L == NULL) {
			L = l;
		} else if (L->h != l->h) {
			stmt *t = l;

			l = r;
			r = t;
		}
		l1 = list_append(l1, l);
		l2 = list_append(l2, r);
	}
	return stmt_releqjoin2(sa, l1, l2);
}

stmt *
stmt_join(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype)
{
	stmt *s = stmt_create(sa, st_join);

	s->op1 = op1;
	s->op2 = op2;
	s->flag = cmptype;
	s->key = op1->key;
	s->nrcols = 2;
	s->h = op1->h;
	s->t = op2->t;
	return s;
}

stmt *
stmt_project(sql_allocator *sa, stmt *op1, stmt *op2)
{
	return stmt_join(sa, op1, op2, cmp_project);
}

stmt *
stmt_join2(sql_allocator *sa, stmt *l, stmt *ra, stmt *rb, int cmp)
{
	stmt *s = stmt_create(sa, st_join2);

	s->op1 = l;
	s->op2 = ra;
	s->op3 = rb;
	s->flag = cmp;
	s->nrcols = 2;
	s->h = l->h;
	s->t = ra->h;
	return s;
}

stmt *
stmt_joinN(sql_allocator *sa, stmt *l, stmt *r, sql_subfunc *op)
{
	stmt *s = stmt_create(sa, st_joinN);

	s->op1 = l;
	s->op2 = r;
	s->op4.funcval = op;
	s->nrcols = 2;
	s->h = l->h;
	s->t = r->h;
	return s;
}

stmt *
stmt_outerjoin(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype)
{
	stmt *s = stmt_create(sa, st_outerjoin);

	s->op1 = op1;
	s->op2 = op2;
	s->flag = cmptype;
	s->nrcols = 2;
	s->h = op1->h;
	s->t = op2->t;
	return s;
}

stmt *
stmt_diff(sql_allocator *sa, stmt *op1, stmt *op2)
{
	stmt *s = stmt_create(sa, st_diff);

	s->op1 = op1;
	s->op2 = op2;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

stmt *
stmt_union(sql_allocator *sa, stmt *op1, stmt *op2)
{
	stmt *s = stmt_create(sa, st_union);

	s->op1 = op1;
	s->op2 = op2;
	s->nrcols = op1->nrcols;
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

stmt *
stmt_rs_column(sql_allocator *sa, stmt *rs, stmt *name, sql_subtype *tpe)
{
	stmt *s = stmt_create(sa, st_rs_column);

	s->op1 = rs;
	s->op2 = name;
	s->op4.typeval = *tpe;
	s->nrcols = 1;
	s->key = 0;
	s->h = NULL;
	s->t = NULL;
	return s;
}

stmt *
stmt_export(sql_allocator *sa, stmt *t, char *sep, char *rsep, char *ssep, char *null_string, stmt *file)
{
	stmt *s = stmt_create(sa, st_export);

	s->op1 = t;
	s->op2 = file;
	s->op4.lval = list_new(sa);
	list_append(s->op4.lval, sep);
	list_append(s->op4.lval, rsep);
	list_append(s->op4.lval, ssep);
	list_append(s->op4.lval, null_string);
	return s;
}

stmt *
stmt_trans(sql_allocator *sa, int type, stmt *chain, stmt *name)
{
	stmt *s = stmt_create(sa, st_trans);

	s->op1 = chain;
	s->op2 = name;
	s->flag = type;
	return s;
}

stmt *
stmt_catalog(sql_allocator *sa, int type, stmt *args)
{
	stmt *s = stmt_create(sa, st_catalog);

	s->op1 = args;
	s->flag = type;
	return s;
}

void
stmt_set_nrcols(stmt *s)
{
	int nrcols = 0;
	int key = 1;
	node *n;
	list *l = s->op4.lval;

	assert(s->type == st_list);
	for (n = l->h; n; n = n->next) {
		stmt *f = n->data;

		if (!f)
			continue;
		if (f->nrcols > nrcols)
			nrcols = f->nrcols;
		key &= f->key;
	}
	s->nrcols = nrcols;
	s->key = key;
}

stmt *
stmt_list(sql_allocator *sa, list *l)
{
	stmt *s = stmt_create(sa, st_list);

	s->op4.lval = l;
	stmt_set_nrcols(s);
	return s;
}

stmt *
stmt_ordered(sql_allocator *sa, stmt *order, stmt *res)
{
	stmt *ns = stmt_create(sa, st_ordered);

	ns->type = st_ordered;
	ns->op1 = order;
	ns->op2 = res;
	ns->nrcols = res->nrcols;
	ns->key = res->key;
	ns->aggr = res->aggr;
	ns->t = res->t;
	return ns;
}

stmt *
stmt_output(sql_allocator *sa, stmt *l)
{
	stmt *s = stmt_create(sa, st_output);

	s->op1 = l;
	return s;
}

stmt *
stmt_affected_rows(sql_allocator *sa, stmt *l)
{
	stmt *s = stmt_create(sa, st_affected_rows);

	s->op1 = l;
	return s;
}

stmt*
stmt_connection(sql_allocator *sa, int *id, char *server, int *port, char *db, char * db_alias, char *user, char *passwd, char *lang)
{
	stmt *s = stmt_create(sa, st_connection);
	s->op4.lval = list_new(sa);

	if (*id != 0)
		list_append(s->op4.lval, id);
	if (server)
		list_append(s->op4.lval, server);
	if (*port != -1)
		list_append(s->op4.lval, port);
	if (db)
		list_append(s->op4.lval, db);
	if (db_alias)
		list_append(s->op4.lval, db_alias);
	if (user)
		list_append(s->op4.lval, user);
	if (passwd)
		list_append(s->op4.lval, passwd);
	if (lang)
		list_append(s->op4.lval, lang);
	return s;
}

stmt *
stmt_append(sql_allocator *sa, stmt *c, stmt *a)
{
	stmt *s = stmt_create(sa, st_append);

	s->op1 = c;
	s->op2 = a;
	s->h = c->h;
	s->t = c->t;
	s->nrcols = c->nrcols;
	s->key = c->key;
	return s;
}

stmt *
stmt_table_clear(sql_allocator *sa, sql_table *t)
{
	stmt *s = stmt_create(sa, st_table_clear);

	s->op4.tval = t;
	s->nrcols = 0;
	return s;
}

stmt *
stmt_exception(sql_allocator *sa, stmt *cond, char *errstr, int errcode)
{
	stmt *s = stmt_create(sa, st_exception);

	assert(cond);
	s->op1 = cond;
	s->op2 = stmt_atom_string(sa, errstr);
	s->op3 = stmt_atom_int(sa, errcode);
	s->nrcols = 0;
	return s;
}

static sql_subtype*
dup_subtype(sql_allocator *sa, sql_subtype *st)
{
	sql_subtype *res = SA_NEW(sa, sql_subtype);

	*res = *st;
	return res;
}

stmt *
stmt_convert(sql_allocator *sa, stmt *v, sql_subtype *from, sql_subtype *to, int dup)
{
	stmt *s = stmt_create(sa, st_convert);
	list *l = list_new(sa);

	if (dup) {
		from = dup_subtype(sa, from);
		to = dup_subtype(sa, to);
	}
	list_append(l, from);
	list_append(l, to);
	s->op1 = v;
	s->op4.lval = l;
	s->nrcols = 0;		/* function without arguments returns single value */
	s->h = v->h;
	s->key = v->key;
	s->nrcols = v->nrcols;
	s->aggr = v->aggr;
	return s;
}

stmt *
stmt_unop(sql_allocator *sa, stmt *op1, sql_subfunc *op)
{
	stmt *s = stmt_create(sa, st_unop);

	s->op1 = op1;
	assert(op);
	s->op4.funcval = op;
	s->h = op1->h;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;
	return s;
}

stmt *
stmt_binop(sql_allocator *sa, stmt *op1, stmt *op2, sql_subfunc *op)
{
	stmt *s = stmt_create(sa, st_binop);
	int aggr = 0;

	s->op1 = op1;
	s->op2 = op2;
	assert(op);
	s->op4.funcval = op;
	aggr = op1->aggr;
	if (!aggr)
		aggr = op2->aggr;
	if (op1->nrcols > op2->nrcols) {
		s->h = op1->h;
		s->nrcols = op1->nrcols;
		s->key = op1->key;
	} else {
		s->h = op2->h;
		s->nrcols = op2->nrcols;
		s->key = op2->key;
	}
	s->aggr = aggr;
	return s;
}

stmt *
stmt_Nop(sql_allocator *sa, stmt *ops, sql_subfunc *op)
{
	node *n;
	stmt *o = NULL, *s = stmt_create(sa, st_Nop);

	s->op1 = ops;
	assert(op);
	s->op4.funcval = op;
	if (list_length(ops->op4.lval)) {
		for (n = ops->op4.lval->h, o = n->data; n; n = n->next) {
			stmt *c = n->data;
	
			if (o->nrcols < c->nrcols)
				o = c;
		}
	}

	if (o) {
		s->h = o->h;
		s->nrcols = o->nrcols;
		s->key = o->key;
		s->aggr = o->aggr;
	} else {
		s->nrcols = 0;
		s->key = 1;
	}
	return s;
}

stmt *
stmt_aggr(sql_allocator *sa, stmt *op1, group *grp, sql_subaggr *op, int reduce)
{
	stmt *s = stmt_create(sa, st_aggr);

	s->op1 = op1;
	if (grp) {
		s->op2 = grp->grp;
		s->op3 = grp->ext;
		s->nrcols = 1;
		s->h = grp->grp->h;
	} else {
		if (!reduce)
			s->nrcols = 1;
		s->h = op1->h;
	}
	s->key = reduce;
	s->aggr = reduce;
	s->op4.aggrval = op;
	s->flag = 0;
	return s;
}

stmt *
stmt_aggr2(sql_allocator *sa, stmt *op1, stmt *op2, sql_subaggr *op)
{
	stmt *s = stmt_create(sa, st_aggr);

	s->op1 = op1;
	s->op2 = op2;
	s->nrcols = 1;
	s->nrcols = 0;
	s->h = op1->h;
	s->key = 1;
	s->aggr = 1;
	s->op4.aggrval = op;
	s->flag = 1;
	return s;
}

stmt *
stmt_alias(sql_allocator *sa, stmt *op1, char *tname, char *alias)
{
	stmt *s = stmt_create(sa, st_alias);

	s->op1 = op1;
	if (tname)
		s->op2 = stmt_atom_string(sa, tname);
	s->op3 = stmt_atom_string(sa, alias);
	s->h = op1->h;
	s->t = op1->t;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;
	return s;
}

sql_subtype *
tail_type(stmt *st)
{
	switch (st->type) {
	case st_const:
	case st_join:
	case st_outerjoin:
		return tail_type(st->op2);
	case st_join2:
	case st_joinN:
		/* The tail type of a join2 is the head of the second operant!,
		   ie should be 'oid' */
		return head_type(st->op2);
	case st_releqjoin:
		/* The tail type of a releqjoin is the head of the second list!,
		   ie should be 'oid' */
		return head_type(st->op4.lval->h->data);
	case st_reljoin:
		if (st->op1)
			return tail_type(st->op1);
		else
			return tail_type(st->op2);

	case st_diff:
	case st_select:
	case st_select2:
	case st_selectN:
	case st_uselect:
	case st_uselect2:
	case st_uselectN:
	case st_limit:
	case st_limit2:
	case st_semijoin:
	case st_unique:
	case st_union:
	case st_append:
	case st_alias:
	case st_gen_group:
	case st_order:
		return tail_type(st->op1);

	case st_list:
		return tail_type(st->op4.lval->h->data);

	case st_bat:
		return &st->op4.cval->type;
	case st_idxbat:
		if (hash_index(st->op4.idxval->type)) {
			return sql_bind_localtype("wrd");
		} else if (st->op4.idxval->type == join_idx) {
			return sql_bind_localtype("oid");
		}
	case st_mark:
	case st_reorder:
	case st_group:
	case st_derive:
	case st_group_ext:
		return sql_bind_localtype("oid");
	case st_table_clear:
		return sql_bind_localtype("lng");
	case st_mirror:
	case st_reverse:
		return head_type(st->op1);

	case st_aggr:
		return &st->op4.aggrval->res;
	case st_unop:
	case st_binop:
	case st_Nop:
		return &st->op4.funcval->res;
	case st_atom:
		return atom_type(st->op4.aval);
	case st_convert:
		return st->op4.lval->t->data;
	case st_temp:
	case st_single:
	case st_rs_column:
		return &st->op4.typeval;
	case st_var:
		if (st->op4.typeval.type)
			return &st->op4.typeval;
		/* fall through */
	case st_exception:
		return NULL;
	case st_table:
		return sql_bind_localtype("bat");
	case st_relselect:
	default:
		fprintf(stderr, "missing tail type %u: %s\n", st->type, st_type2string(st->type));
		assert(0);
		return NULL;
	}
}

sql_subtype *
head_type(stmt *st)
{
	switch (st->type) {
	case st_aggr:
	case st_convert:
	case st_unop:
	case st_binop:
	case st_Nop:
	case st_unique:
	case st_union:
	case st_alias:
	case st_diff:
	case st_join:
	case st_join2:
	case st_joinN:
	case st_outerjoin:
	case st_semijoin:
	case st_mirror:
	case st_select:
	case st_select2:
	case st_selectN:
	case st_uselect:
	case st_uselect2:
	case st_uselectN:
	case st_append:
	case st_gen_group:
	case st_group:
	case st_group_ext:
	case st_order:
	case st_mark:
	case st_relselect:
	case st_releqjoin:
		return head_type(st->op1);

	case st_reljoin:
		if (st->op1)
			return head_type(st->op1);
		else
			return head_type(st->op2);

	case st_list:
		return head_type(st->op4.lval->h->data);

	case st_temp:
	case st_single:
	case st_bat:
	case st_idxbat:
	case st_const:
	case st_rs_column:
		return sql_bind_localtype("oid");
		/* return NULL; oid */

	case st_reverse:
		return tail_type(st->op1);
	case st_atom:
		return atom_type(st->op4.aval);
	case st_var:
		if (st->op4.typeval.type)
			return &st->op4.typeval;
	default:
		fprintf(stderr, "missing head type %u: %s\n", st->type, st_type2string(st->type));
		return NULL;
	}
}

int
stmt_has_null( stmt *s )
{
	switch (s->type) {
	case st_aggr:
	case st_Nop:
	case st_select:
	case st_select2:
	case st_selectN:
	case st_uselect:
	case st_uselect2:
	case st_uselectN:
	case st_atom:
		return 0;
	case st_unop:
	case st_reverse:
	case st_mark:
		return stmt_has_null(s->op1);
	case st_binop:
		return stmt_has_null(s->op1) + stmt_has_null(s->op2);
	case st_join:
		return stmt_has_null(s->op2);
	case st_bat:
		return s->op4.cval->null;

	default:
		return 1;
	}
}

static char *
func_name(sql_allocator *sa, char *n1, char *n2)
{
	int l1 = _strlen(n1), l2; 

	if (!n2)
		return sa_strdup(sa, n1);
	l2 = _strlen(n2);

	if (l2 > 16) {		/* only support short names */
		char *ns = SA_NEW_ARRAY(sa, char, l2 + 1);

		strncpy(ns, n2, l2);
		ns[l2] = 0;
		return ns;
	} else {
		char *ns = SA_NEW_ARRAY(sa, char, l1 + l2 + 2), *s = ns;

		strncpy(ns, n1, l1);
		ns += l1;
		*ns++ = '_';
		strncpy(ns, n2, l2);
		ns += l2;
		*ns = '\0';
		return s;
	}
}

char *
column_name(sql_allocator *sa, stmt *st)
{
	switch (st->type) {
	case st_reverse:
	case st_order:
	case st_reorder:
		return column_name(sa, st->op1);
	case st_const:
	case st_join:
	case st_join2:
	case st_joinN:
	case st_outerjoin:
	case st_derive:
	case st_rs_column:
		return column_name(sa, st->op2);

	case st_mirror:
	case st_group:
	case st_group_ext:
	case st_union:
	case st_append:
	case st_mark:
	case st_gen_group:
	case st_select:
	case st_select2:
	case st_selectN:
	case st_uselect:
	case st_uselect2:
	case st_uselectN:
	case st_limit:
	case st_limit2:
	case st_semijoin:
	case st_diff:
	case st_unique:
	case st_convert:
		return column_name(sa, st->op1);

	case st_unop:
	case st_binop:
	case st_Nop:
	{
		char *cn = column_name(sa, st->op1);
		return func_name(sa, st->op4.funcval->func->base.name, cn);
	}
	case st_aggr:
	{
		char *cn = column_name(sa, st->op1);
		return func_name(sa, st->op4.aggrval->aggr->base.name, cn);
	}
	case st_alias:
		return column_name(sa, st->op3 );
	case st_bat:
		return sa_strdup(sa, st->op4.cval->base.name);
	case st_atom:
		if (st->op4.aval->data.vtype == TYPE_str)
			return atom2string(sa, st->op4.aval);
	case st_var:
	case st_temp:
	case st_single:
		return sa_strdup(sa, "single_value");

	case st_relselect:
	case st_releqjoin:
	case st_reljoin:
		return column_name(sa, st->op1);
	case st_list:
		if (list_length(st->op4.lval))
			return column_name(sa, st->op4.lval->h->data);
		return NULL;
	default:
		fprintf(stderr, "missing column name %u: %s\n", st->type, st_type2string(st->type));
		return NULL;
	}
}

char *
table_name(sql_allocator *sa, stmt *st)
{
	switch (st->type) {
	case st_reverse:
		return table_name(sa, st->op1);
	case st_const:
	case st_join:
	case st_join2:
	case st_joinN:
	case st_outerjoin:
	case st_derive:
		return table_name(sa, st->op2);
	case st_mirror:
	case st_group:
	case st_group_ext:
	case st_union:
	case st_append:
	case st_mark:
	case st_gen_group:
	case st_select:
	case st_select2:
	case st_selectN:
	case st_uselect:
	case st_uselect2:
	case st_uselectN:
	case st_limit:
	case st_limit2:
	case st_semijoin:
	case st_diff:
	case st_aggr:
	case st_unique:
		return table_name(sa, st->op1);

	case st_basetable:
		if (st->op2)
			return table_name(sa, st->op2);
	case st_table_clear:
		return sa_strdup(sa, st->op4.tval->base.name);
	case st_bat:
		return table_name(sa, st->h);
	case st_alias:
		if (st->op2)
			return table_name(sa, st->op2);
		else
			/* there are no table aliases, ie look into the base column */
			return table_name(sa, st->op1);
	case st_atom:
		if (st->op4.aval->data.vtype == TYPE_str && st->op4.aval->data.val.sval && _strlen(st->op4.aval->data.val.sval))
			return atom2string(sa, st->op4.aval);

	case st_var:
	case st_temp:
	case st_single:
	case st_relselect:
	case st_releqjoin:
	case st_reljoin:
	default:
		return NULL;
	}
}

char *
schema_name(sql_allocator *sa, stmt *st)
{
	switch (st->type) {
	case st_reverse:
		return schema_name(sa, st->op1);
	case st_const:
	case st_join:
	case st_join2:
	case st_joinN:
	case st_outerjoin:
	case st_derive:
		return schema_name(sa, st->op2);
	case st_mirror:
	case st_group:
	case st_group_ext:
	case st_union:
	case st_append:
	case st_mark:
	case st_gen_group:
	case st_select:
	case st_select2:
	case st_selectN:
	case st_uselect:
	case st_uselect2:
	case st_uselectN:
	case st_limit:
	case st_limit2:
	case st_semijoin:
	case st_diff:
	case st_unique:
	case st_convert:
	case st_unop:
	case st_binop:
	case st_Nop:
	case st_aggr:
		return schema_name(sa, st->op1);
	case st_alias:
		/* there are no schema aliases, ie look into the base column */
		return schema_name(sa, st->op1);
	case st_bat:
		return sa_strdup(sa, st->op4.cval->t->s->base.name);
	case st_atom:
		return NULL;
	case st_var:
	case st_temp:
	case st_single:
		return NULL;
	case st_relselect:
	case st_releqjoin:
	case st_reljoin:
		return schema_name(sa, st->op1);
	case st_list:
		if (list_length(st->op4.lval))
			return schema_name(sa, st->op4.lval->h->data);
		return NULL;
	default:
		return NULL;
	}
}

stmt *stmt_while(sql_allocator *sa, stmt *cond, stmt *whilestmts )
{
	/* while is a if - block true with leave statement
	 * needed because the condition needs to be inside this outer block */
	list *l = list_new(sa);
	stmt *cstmt, *wstmt;

	list_append(l, cstmt = stmt_cond(sa, stmt_bool(sa, 1), NULL, 0));
	list_append(l, cond);
	list_append(l, wstmt = stmt_cond(sa, cond, cstmt, 1));
	list_append(l, whilestmts);
	list_append(l, stmt_control_end(sa, wstmt));
	list_append(l, stmt_control_end(sa, cstmt));
	return stmt_list(sa, l);
}

stmt *stmt_cond(sql_allocator *sa, stmt *cond, stmt *outer, int loop /* 0 if, 1 while */)
{
	stmt *s = stmt_create(sa, st_cond);

	s->op1 = cond;
	s->op2 = outer;
	s->flag = loop;
	return s;
}

stmt *stmt_control_end(sql_allocator *sa, stmt *cond)
{
	stmt *s = stmt_create(sa, st_control_end);
	s->op1 = cond;
	return s;
}


stmt *stmt_if(sql_allocator *sa, stmt *cond, stmt *ifstmts, stmt *elsestmts)
{
	list *l = list_new(sa);
	stmt *cstmt;
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *not = sql_bind_func(sa, NULL, "not", bt, NULL);

	list_append(l, cstmt = stmt_cond(sa, cond, NULL, 0));
	list_append(l, ifstmts);
	list_append(l, stmt_control_end(sa, cstmt));
	if (elsestmts) {
		cond = stmt_unop(sa, cond, not);
		list_append(l, cstmt = stmt_cond(sa, cond, NULL, 0));
		list_append(l, elsestmts);
		list_append(l, stmt_control_end(sa, cstmt));
	}
	return stmt_list(sa, l);
}

stmt *stmt_return(sql_allocator *sa, stmt *val, int nr_declared_tables)
{
	stmt *s = stmt_create(sa, st_return);

	s->op1 = val;
	s->flag = nr_declared_tables;
	return s;
}

stmt *stmt_assign(sql_allocator *sa, char *varname, stmt *val, int level)
{
	stmt *s = stmt_create(sa, st_assign);

	s->op1 = stmt_atom_string(sa, sa_strdup(sa, varname));
	s->op2 = val;
	s->flag = (level<<1);
	return s;
}

stmt *const_column(sql_allocator *sa, stmt *val ) 
{
	sql_subtype *ct = tail_type(val);
/*
	stmt *temp = stmt_temp(ct);
	return stmt_append(temp, val);
*/
	stmt *s = stmt_create(sa, st_single);

	s->op1 = val;
	s->op4.typeval = *ct;
	s->nrcols = 1;
	return s;
}

void
stack_push_stmt( sql_stack *stk, stmt *s, int first )
{
	if (first && s->nr == 0) {
		sql_stack_push(stk, s);
	} else if (!first && s->nr == 0) {
		s->nr = -stk->top;
		sql_stack_push(stk, s);
	}
}

void
stack_push_list( sql_stack *stk, list *l )
{
	int top;
	node *n;

	/* Push in reverse order */
	for (n = l->h; n; n = n->next)
		sql_stack_push(stk, NULL);
	for (n = l->h, top = stk->top; n; n = n->next)
		stk->values[--top] = n->data;
}

void
stack_push_children( sql_stack *stk, stmt *s)
{
	switch( s->type) {
	case st_list:
		stack_push_list( stk, s->op4.lval);
		break;
	default:
		if (s->op1)
			stack_push_stmt(stk, s->op1, 1);
		if (s->op2)
			stack_push_stmt(stk, s->op2, 1);
		if (s->op3)
			stack_push_stmt(stk, s->op3, 1);
	}
}

void
clear_stmts( stmt ** stmts )
{
	int nr = 0;

	while (stmts[nr] ) {
		stmt *s = stmts[nr++];
		s->nr = 0;
	}
}

stmt **
stmt_array( sql_allocator *sa, stmt *s)
{
	int sz = 1024, top = 0;
	stmt **res = SA_NEW_ARRAY(sa, stmt*, sz);
	sql_stack *stk = sql_stack_new(sa, sz);

	stack_push_stmt(stk, s, 1);
	while((s = sql_stack_pop(stk)) != NULL) {
		/* not handled */
		if (s->nr == 0) {
			stack_push_stmt(stk, s, 0);
			stack_push_children(stk, s);
			/* push all children */
		} else if (s->nr < 0) {
			/* children are handled put in the array */
			s->nr = top;
			if (top+10 >= sz) {
				size_t osz = sz;
				sz *= 2;
				res = SA_RENEW_ARRAY(sa, stmt*, res, sz, osz);
				assert(res != NULL);
			}
			res[top++] = s;
		} else if (s->nr > 0) { /* ?? */
		}
	}
	res[top++] = NULL; /* mark end */
	return res;
}

void
print_stmt( sql_allocator *sa, stmt *s ) 
{
	switch(s->type) {
	case st_var:
		if (s->op1)
			printf("s%d := %s:%s\n", s->nr, s->op1->op4.aval->data.val.sval, s->op4.typeval.type->base.name);
		else
			printf("s%d := A%d:%s\n", s->nr, s->flag, s->op4.typeval.type->base.name);
		break;
	case st_atom:
		printf("s%d := '%s':%s\n", s->nr, atom2string(sa, s->op4.aval), s->op4.aval->tpe.type->base.name);
		break;
	case st_list: {
		node *n;
		printf("s%d := %s(", s->nr, st_type2string(s->type));
		for(n=s->op4.lval->h; n; n = n->next) { 
			stmt *e = n->data;
			printf("s%d%s", e->nr, n->next?", ":"");
		}
		printf(");\n");
	}	break;
	case st_basetable:
	case st_reljoin:
	case st_releqjoin:
		/*assert(0);*/
	default:
		printf("s%d := %s(", s->nr, st_type2string(s->type));
		switch(s->type) {
		case st_temp:
		case st_single:
			printf("%s", s->op4.typeval.type->base.name);
			break;
		case st_rs_column:
			printf("%s, ", s->op4.typeval.type->base.name);
			break;
		case st_bat:
		case st_append_col:
		case st_update_col:
			printf("%s.%s.%s, ", 
				s->op4.cval->t->s->base.name, 
				s->op4.cval->t->base.name, 
				s->op4.cval->base.name);
			break;
		case st_idxbat:
		case st_append_idx:
		case st_update_idx:
			printf("%s.%s.%s, ", 
				s->op4.idxval->t->s->base.name, 
				s->op4.idxval->t->base.name, 
				s->op4.idxval->base.name);
			break;
		case st_dbat:
		case st_delete:
		case st_table_clear:
			printf("%s.%s, ", 
				s->op4.tval->s->base.name, 
				s->op4.tval->base.name);
			break;
		case st_convert: {
			sql_subtype *f = s->op4.lval->h->data;
			sql_subtype *t = s->op4.lval->t->data;
			printf("%s, %s", f->type->base.name, t->type->base.name);
		 } 	break;
		case st_unop:
			printf("%s", s->op4.funcval->func->base.name);
			break;
		case st_binop:
		case st_Nop:
			printf("%s, ", s->op4.funcval->func->base.name);
			break;
		case st_aggr:
			printf("%s, ", s->op4.aggrval->aggr->base.name);
			break;
		default:
			break;
		}
		if (s->op1)
			printf("s%d", s->op1->nr);
		if (s->op2)
			printf(", s%d", s->op2->nr);
		if (s->op3)
			printf(", s%d", s->op3->nr);
		printf(");\n");
		break;
	}
}

void
print_stmts( sql_allocator *sa, stmt ** stmts )
{
	int nr = 0;

	while (stmts[nr] ) {
		stmt *s = stmts[nr++];
		print_stmt(sa, s);
	}
}

void
print_tree( sql_allocator *sa, stmt * s)
{
	stmt **stmts = stmt_array(sa, s);

	print_stmts(sa, stmts);
	clear_stmts(stmts);
}

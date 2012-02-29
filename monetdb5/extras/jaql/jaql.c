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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * JAQL is a query language for JavaScript Object Notation or JSON.
 */

#include "monetdb_config.h"
#include "jaql.h"
#include "jaqlgencode.h"
#include "json.h"
#include "gdk.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "stream.h"

#include "parser/jaql.tab.h"
#include "parser/jaql.yy.h"

extern int yyparse(jc *j);
void freetree(tree *j);
str getContext(Client c, jc **j);

/* assign the output of action (a 1 or more stage pipe) to ident, if
 * ident is NULL, the result should be outputted to the screen, if
 * action is NULL, no modification is applied */
tree *
make_json_output(char *ident)
{
	tree *t = GDKzalloc(sizeof(tree));

	if (ident != NULL) {
		t->type = j_output_var;
		t->sval = ident;
	} else {
		t->type = j_output;
	}

	return t;
}

tree *
make_json(char *s)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_json;
	res->sval = s;

	return res;
}

tree *
make_json_object(tree *obj)
{
	tree *res;

	assert(obj != NULL && (obj->type == j_pair || obj->type == j_error));
	
	if (obj->type == j_error)
		return obj;

	res = GDKzalloc(sizeof(tree));
	res->type = j_json_obj;
	res->tval1 = obj;

	return res;
}

tree *
make_json_array(tree *arr)
{
	tree *res = GDKzalloc(sizeof(tree));

	assert(arr != NULL); /* arr can be about everything */
	res->type = j_json_arr;
	res->tval1 = arr;

	return res;
}

/* append naction as next pipe after oaction */
tree *
append_jaql_pipe(tree *oaction, tree *naction)
{
	tree *t = oaction;

	/* optimise away this pipe if naction is NULL */
	if (naction == NULL)
		return oaction;
	/* propagate errors immediately */
	if (naction->type == j_error) {
		freetree(oaction);
		return naction;
	}
	if (oaction->type == j_error) {
		freetree(naction);
		return oaction;
	}

	/* find last in chain to append to */
	while (t->next != NULL)
		t = t->next;
	t = t->next = naction;

	return oaction;
}

/* recursive helper to check variable usages for validity */
static tree *
_check_exp_var(const char *func, const char **vars, tree *t)
{
	tree *res = NULL;

	if (t == NULL)
		return res;

	if (t->type == j_var) {
		const char **var;
		for (var = vars; *var != NULL; var++) {
			if (strcmp(*var, t->sval) == 0)
				break;
		}
		if (*var == NULL) {
			char buf[128];
			res = GDKzalloc(sizeof(tree));
			snprintf(buf, sizeof(buf), "%s: unknown variable: %s",
					func, t->sval);
			res->type = j_error;
			res->sval = GDKstrdup(buf);
		}
		return res;
	}

	if ((res = _check_exp_var(func, vars, t->tval1)) != NULL)
		return res;
	if ((res = _check_exp_var(func, vars, t->tval2)) != NULL)
		return res;
	if ((res = _check_exp_var(func, vars, t->tval3)) != NULL)
		return res;

	return res;
}
static tree *
_check_exp_var1(const char *func, const char *var, tree *t)
{
	const char *vars[] = {var, NULL};
	return _check_exp_var(func, vars, t);
}

/* create filter action looping over the input array as ident,
 * performing pred condition to include each element in the array. */
tree *
make_jaql_filter(tree *var, tree *pred)
{
	tree *res;

	assert(var != NULL && var->type == j_var);
	assert(pred != NULL);

	if (pred->type == j_error) {
		freetree(var);
		return pred;
	}

	assert(pred->type == j_pred);
	
	if ((res = _check_exp_var1("filter", var->sval, pred)) != NULL) {
		freetree(var);
		freetree(pred);
		return res;
	}

	res = GDKzalloc(sizeof(tree));
	res->type = j_filter;
	res->tval1 = var;
	res->tval2 = pred;

	return res;
}

/* create transform action looping over the input array as ident,
 * producing an array with elements mapped from the given template. */
tree *
make_jaql_transform(tree *var, tree *tmpl)
{
	tree *res;

	assert(var != NULL && var->type == j_var);
	assert(tmpl != NULL);

	if (tmpl->type == j_error) {
		freetree(var);
		return tmpl;
	}

	/* traverse down tmpl, searching for all variable references to
	 * check if they refer to var */
	if ((res = _check_exp_var1("transform", var->sval, tmpl)) != NULL) {
		freetree(var);
		freetree(tmpl);
		return res;
	}

	if (tmpl->type == j_var && tmpl->tval1 == NULL && tmpl->tval2 == NULL) {
		/* simple variable, hence not performing any change so null-op */
		freetree(var);
		freetree(tmpl);
		return NULL;
	}

	res = GDKzalloc(sizeof(tree));
	res->type = j_transform;
	res->tval1 = var;
	res->tval2 = tmpl;

	return res;
}

/* create expand action flattening the nested arrays of the input array
 * as ident, producing a flat array.  In its simplest form each element
 * from the nested array is promoted to its parent array. */
tree *
make_jaql_expand(tree *var, tree *expr)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_expand;
	res->tval1 = var;

	assert(var->type == j_var);
	assert(expr == NULL || expr->type == j_var || expr->type == j_unroll);

	/* make execution easier by always giving expand an argument to
	 * expand, which defaults to the var we're looping over as (usually
	 * $, but modified with "each xxx") */
	if (expr == NULL) {
		expr = GDKzalloc(sizeof(tree));
		expr->type = j_var;
		expr->sval = GDKstrdup(var->sval);
	} else {
		char *v;
		if (expr->type == j_var)
			v = expr->sval;
		if (expr->type == j_unroll && expr->tval1->type == j_var)
			v = expr->tval1->sval;
		assert(v != NULL);
		if (strcmp(var->sval, v) != 0) {
			char buf[128];
			snprintf(buf, sizeof(buf), "expand: unknown variable: %s", v);
			res->type = j_error;
			res->sval = GDKstrdup(buf);
			res->tval1 = NULL;
			freetree(expr);
			freetree(var);
			return res;
		}
	}

	if (expr->type == j_var && expr->next != NULL) {
		/* JAQL's confusing "inner pipes" feature -- most probably to
		 * steer Hadoop's map-reduce job generationi -- is just useless
		 * for us and actually making our life harder, so just pull out
		 * this inner pipe, and make it a proper top-level pipe instead */
		res->next = expr->next;
		expr->next = NULL;
	}
	
	if (expr->type == j_unroll) {
		/* since unroll is quite different from expand, promote it as
		 * top operation, forgetting about the expand */
		res->type = j_unroll;
		res->tval2 = expr->tval1;
		expr->tval1 = NULL;
		freetree(expr);

		if (res->tval2->tval1 == NULL) {
			/* unrolling an array without a parent is pointless */
			res->type = j_expand;
		}
		return res;
	}

	res->tval2 = expr;

	return res;
}

/* create a group by expression, performing group or co-grouping on
 * inputs, producing results according to the given tmpl
 * all references in inputs must be to var (which is discarded) */
tree *
make_jaql_group(tree *inputs, tree *tmpl, tree *var)
{
	tree *res;
	tree *w;

	assert(inputs == NULL || inputs->type == j_group_input);
	assert(tmpl != NULL);
	assert(var != NULL && var->type == j_var);

	if (tmpl->type == j_error) {
		freetree(inputs);
		return tmpl;
	}

	res = GDKzalloc(sizeof(tree));

	if (inputs != NULL) {
		size_t i;
		const char **vars;

		/* when multiple inputs are given, the groupkeyvar must be for
		 * each input the same, its expression may differ */
		for (i = 1, w = inputs; w->next != NULL; w = w->next, i++) {
			assert(w->sval != NULL && w->next->sval != NULL);
			if (strcmp(w->sval, w->next->sval) != 0) {
				res->type = j_error;
				res->sval = GDKstrdup("group: groupkeyvar of multiple group "
						"inputs must be equal");
				freetree(inputs);
				freetree(tmpl);
				return res;
			}
			if (strcmp(w->tval2->sval, var->sval) != 0) {
				char buf[128];
				snprintf(buf, sizeof(buf), "group: unknown variable: %s",
						w->tval2->sval);
				res->type = j_error;
				res->sval = GDKstrdup(buf);
				freetree(inputs);
				freetree(tmpl);
				return res;
			}
		}
		/* the alias of each group result must be unique, otherwise you
		 * can't reference them */
		for (w = inputs; w->next != NULL; w = w->next) {
			tree *v;
			for (v = w->next; v != NULL; v = v->next) {
				if (strcmp(w->tval3->sval, v->tval3->sval) == 0) {
					res->type = j_error;
					res->sval = GDKstrdup("group: groupvar of multiple group "
							"inputs must be unique (for use in 'into' expression)");
					freetree(inputs);
					freetree(tmpl);
					return res;
				}
			}
		}

		vars = GDKmalloc(sizeof(char *) * (i + 2 + 1));
		vars[0] = var->sval;
		vars[1] = inputs->sval;
		for (i = 2, w = inputs; w != NULL; w = w->next, i++)
			vars[i] = w->tval3->sval;
		vars[i] = NULL;

		if ((w = _check_exp_var("group", vars, tmpl)) != NULL) {
			freetree(inputs);
			freetree(tmpl);
			return w;
		}
	} else {
		if ((w = _check_exp_var1("group", var->sval, tmpl)) != NULL) {
			freetree(inputs);
			freetree(tmpl);
			return w;
		}
	}

	res->type = j_group;
	res->tval1 = inputs;
	res->tval2 = tmpl;

	return res;
}

/* create a wrapper for expand unroll */
tree *
make_unroll(tree *var)
{
	tree *res;

	assert(var != NULL && var->type == j_var);

	res = GDKzalloc(sizeof(tree));
	res->type = j_unroll;
	res->tval1 = var;

	return res;
}

/* utility to recursively check the given tree for its predicates, and
 * ensure that only equality expressions are used */
static tree *
_check_exp_equals_only(tree *t)
{
	tree *res;

	if (t == NULL)
		return NULL;

	if (t->type == j_pred && t->tval2->type == j_comp) {
		if (t->tval2->cval != j_and && t->tval2->cval != j_equals) {
			res = GDKzalloc(sizeof(tree));
			res->type = j_error;
			res->sval = GDKstrdup("join: only (conjunctions of) equality "
					"tests are allowed");
			return res;
		}
		if (t->tval2->cval == j_equals) {
			if (t->tval1->type != j_var || t->tval3->type != j_var) {
				res = GDKzalloc(sizeof(tree));
				res->type = j_error;
				res->sval = GDKstrdup("join: equality tests must be between "
						"two variables");
				return res;
			}
			if (strcmp(t->tval1->sval, t->tval3->sval) == 0) {
				res = GDKzalloc(sizeof(tree));
				res->type = j_error;
				res->sval = GDKstrdup("join: self-joins not allowed");
				return res;
			}
		}
	}

	if ((res = _check_exp_equals_only(t->tval1)) != NULL)
		return res;
	if ((res = _check_exp_equals_only(t->tval2)) != NULL)
		return res;
	if ((res = _check_exp_equals_only(t->tval3)) != NULL)
		return res;

	return NULL;
}

static tree *
_extract_equals(tree *t)
{
	tree *p, *q;

	assert(t->type == j_pred);
	assert(t->tval2->type == j_comp);

	if (t->tval2->cval == j_equals)
		return t;

	assert(t->tval2->cval == j_and);

	p = q = _extract_equals(t->tval1);
	while (p->next != NULL)
		p = p->next;
	p->next = _extract_equals(t->tval3);

	t->tval1 = t->tval3 = t->next = NULL;
	freetree(t);

	return q;
}

/* create a join operation over 2 or more inputs, applying predicates,
 * producing output defined by tmpl */
tree *
make_jaql_join(tree *inputs, tree *pred, tree *tmpl)
{
	tree *res;
	const char **vars;
	int i;

	/* docs seem to suggest a where clause (pred) is always present */
	assert(inputs != NULL && inputs->type == j_join_input);
	assert(pred != NULL && pred->type == j_pred);
	assert(tmpl != NULL);

	if (inputs->next == NULL) {
		res = GDKzalloc(sizeof(tree));
		res->type = j_error;
		res->sval = GDKstrdup("join: need two or more inputs");
		freetree(inputs);
		freetree(pred);
		freetree(tmpl);
		return res;
	}

	for (i = 0, res = inputs; res != NULL; res = res->next, i++)
		;
	vars = GDKmalloc(sizeof(char *) * (i + 1));
	for (i = 0, res = inputs; res != NULL; res = res->next, i++)
		vars[i] = res->tval2->sval;
	vars[i] = NULL;

	if ((res = _check_exp_var("join", vars, pred)) != NULL)
		return res;
	if ((res = _check_exp_var("join", vars, tmpl)) != NULL)
		return res;

	/* JAQL defines that only conjunctions of equality expressions may
	 * be used (and + ==), where self-joins are disallowed */
	if ((res = _check_exp_equals_only(pred)) != NULL)
		return res;

	/* JAQL defines that each of the inputs must be linked through a
	 * join path, collect all equality tests and put them in a simple
	 * list */
	pred = _extract_equals(pred);
	for (i = 0; vars[i] != NULL; i++) {
		for (res = pred; res != NULL; res = res->next) {
			if (strcmp(vars[i], res->tval1->sval) == 0 ||
					strcmp(vars[i], res->tval3->sval) == 0)
			{
				vars[i] = "";
				break;
			}
		}
		if (vars[i][0] != '\0') {
			char buf[128];
			res = GDKzalloc(sizeof(tree));
			res->type = j_error;
			snprintf(buf, sizeof(buf), "join: input not referenced "
					"in where: %s", vars[i]);
			res->sval = GDKstrdup(buf);
			freetree(inputs);
			freetree(pred);
			freetree(tmpl);
			return res;
		}
	}

	/* we skip the graph/path check and do it during code generation */

	res = GDKzalloc(sizeof(tree));
	res->type = j_join;
	res->tval1 = inputs;
	res->tval2 = pred;
	res->tval3 = tmpl;

	return res;
}

/* create a sort operation defined by comparator in expr */
tree *
make_jaql_sort(tree *var, tree *expr)
{
	tree *res;

	assert(var != NULL && var->type == j_var);
	assert(expr != NULL && expr->type == j_sort_arg);

	if ((res = _check_exp_var1("sort", var->sval, expr)) != NULL)
		return res;

	res = GDKzalloc(sizeof(tree));
	res->type = j_sort;
	res->tval1 = var;
	res->tval2 = expr;

	return res;
}

/* create top-N limit operator for the number of elements defined in num */
tree *
make_jaql_top(long long int num)
{
	tree *res = GDKzalloc(sizeof(tree));

	if (num < 0) {
		char buf[128];
		snprintf(buf, sizeof(buf), "top: invalid limit: %lld", num);
		res->type = j_error;
		res->sval = GDKstrdup(buf);
	} else {
		res->type = j_top;
		res->nval = num;
	}

	return res;
}

tree *
make_array_index(long long int idx, char isstar)
{
	tree *res = GDKzalloc(sizeof(tree));

	if (idx < 0) {
		res->type = j_error;
		res->sval = GDKstrdup("variable: array index must be a "
				"positive integer value");
		return res;
	}

	res->type = j_arr_idx;
	res->nval = idx;
	if (isstar)
		res->nval = -1;

	return res;
}

/* create predicate between left and right which can be predicates on
 * their own, or variables and values */
tree *
make_pred(tree *l, tree *comp, tree *r)
{
	tree *res;

	if (l != NULL && l->type == j_error) {
		freetree(comp);
		freetree(r);
		return l;
	}
	if (r->type == j_error) {
		freetree(l);
		freetree(comp);
		return r;
	}

	/* shortcut to optimize non-not constructions */
	if (comp == NULL && l == NULL)
		return r;

	/* optimise the case where comp is j_not, and r is a variable to
	 * rewrite its comp to j_nequal */
	if (comp->cval == j_not && r->type == j_pred &&
			r->tval2->cval == j_equals)
	{
		r->tval2->cval = j_nequal;
		freetree(l);
		freetree(comp);
		return r;
	}

	if (r->type == j_bool && comp->cval != j_nequal && comp->cval != j_equals)
	{
		freetree(l);
		freetree(comp);
		freetree(r);

		res = GDKzalloc(sizeof(tree));
		res->type = j_error;
		res->sval = GDKstrdup("filter: can only apply equality tests on booleans");
		return res;
	}

	res = GDKzalloc(sizeof(tree));
	res->type = j_pred;
	res->tval1 = l;
	res->tval2 = comp;
	res->tval3 = r;

	return res;
}

tree *
make_sort_arg(tree *var, char asc)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_sort_arg;
	res->tval1 = var;
	res->nval = asc;

	return res;
}

tree *
append_sort_arg(tree *osarg, tree *nsarg)
{
	tree *t = osarg;

	assert(osarg != NULL && osarg->type == j_sort_arg);
	assert(nsarg != NULL && nsarg->type == j_sort_arg);

	/* find last in chain to append to */
	while (t->next != NULL)
		t = t->next;
	t = t->next = nsarg;

	return osarg;
}

/* create a variable name from ident, with optional array indirection */
tree *
make_varname(char *ident, tree *arridx)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_var;
	res->sval = ident;
	res->tval2 = arridx;

	return res;
}

/* append an object indirection to the variable in var with the name
 * from ident, with optional array indirection */
tree *
append_varname(tree *var, char *ident, tree *arridx)
{
	tree *t = var;

	/* find last in chain to append to */
	while (t->tval1 != NULL)
		t = t->tval1;
	t = t->tval1 = GDKzalloc(sizeof(tree));
	t->type = j_var;
	t->sval = ident;
	t->tval2 = arridx;

	return var;
}

/* constructs a JSON pair '"name": val' where val can be an expression
 * or a literal value
 * if name is NULL, val must be a variable, and the pair constructed
 * will be named after the (last part of the) variable */
tree *
make_pair(char *name, tree *val)
{
	tree *res = GDKzalloc(sizeof(tree));
	tree *w;

	assert(val != NULL);
	assert(name != NULL || val->type == j_var);

	if (name == NULL) {
		if (val->type != j_var || 
				(val->tval1 == NULL && strcmp(val->sval, "$") == 0))
		{
			/* we can't do arithmetic with these */
			res->type = j_error;
			res->sval = GDKstrdup("transform: a pair needs a name");
			freetree(val);
			return res;
		}

		/* find last var in val */
		for (w = val; w->tval1 != NULL; w = w->tval1) {
			if (w->tval2 != NULL && w->tval1 == NULL) {
				/* array as last, doesn't have a name */
				res->type = j_error;
				res->sval = GDKstrdup("transform: cannot deduce pair name "
						"from array member(s)");
				freetree(val);
				return res;
			}
		}
		if (w->sval != NULL)
			name = GDKstrdup(w->sval);
	}

	res->type = j_pair;
	res->tval1 = val;
	res->sval = name;

	return res;
}

/* append npair to opair */
tree *
append_pair(tree *opair, tree *npair)
{
	tree *w = opair;

	assert(opair != NULL);
	assert(npair != NULL);

	if (opair->type == j_error) {
		freetree(npair);
		return opair;
	}
	if (npair->type == j_error) {
		freetree(opair);
		return npair;
	}

	assert(opair->type == j_pair);
	assert(npair->type == j_pair);

	while (w->next != NULL)
		w = w->next;

	w->next = npair;

	return opair;
}

/* append nelem to oelem */
tree *
append_elem(tree *oelem, tree *nelem)
{
	tree *w = oelem;

	assert(oelem != NULL);
	assert(nelem != NULL);

	while (w->next != NULL)
		w = w->next;

	w->next = nelem;

	return oelem;
}

/* creates a group by input with a variable to assign groupkeys to, a
 * variable from the input to create the groupkeys from, and a variable
 * to refer to each group, not bounded to an input variable yet */
tree *
make_group_input(char *grpkeyvar, tree *grpkey, tree *walkvar)
{
	tree *res = GDKzalloc(sizeof(tree));

	assert(grpkeyvar != NULL);
	assert(grpkey != NULL && grpkey->type == j_var);
	assert(walkvar != NULL && walkvar->type == j_var);

	res->type = j_group_input;
	res->sval = grpkeyvar;
	res->tval2 = grpkey;
	res->tval3 = walkvar;

	return res;
}

tree *
append_group_input(tree *oginp, tree *nginp)
{
	tree *w = oginp;

	assert(oginp != NULL && oginp->type == j_group_input);
	assert(nginp != NULL && nginp->type == j_group_input);

	while (w->next != NULL)
		w = w->next;

	w->next = nginp;

	return oginp;
}

tree *
set_group_input_var(tree *ginp, tree *inpvar)
{
	assert(ginp == NULL || ginp->type == j_group_input);
	assert(inpvar != NULL && inpvar->type == j_var);

	if (ginp == NULL) {
		/* group into shortcut, basically no grouping */
		ginp = GDKzalloc(sizeof(tree));
		ginp->type = j_group_input;
	}

	ginp->tval1 = inpvar;
	
	return ginp;
}

/* creates a join input variable with optional preserve flag set
 * if invar is not NULL, var is considered the alias variable */
tree *
make_join_input(char preserve, tree *var, tree *invar)
{
	tree *res;

	assert(var != NULL && var->type == j_var);
	assert(invar == NULL || invar->type == j_var);

	res = GDKzalloc(sizeof(tree));
	res->type = j_join_input;
	res->nval = preserve;
	res->tval2 = var;
	if (invar == NULL) {
		res->tval1 = make_varname(GDKstrdup(var->sval), NULL);
	} else {
		res->tval1 = invar;
	}

	return res;
}

tree *
append_join_input(tree *ojinp, tree *njinp)
{
	tree *w = ojinp;

	assert(ojinp != NULL && ojinp->type == j_join_input);
	assert(njinp != NULL && njinp->type == j_join_input);

	while (w->next != NULL)
		w = w->next;

	w->next = njinp;

	return ojinp;
}

/* create a comparison from the given type */
tree *
make_comp(enum comptype t)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_comp;
	res->cval = t;

	return res;
}

/* create an operation of the given type */
tree *
make_op(enum comptype t)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_op;
	res->cval = t;

	return res;
}

/* create an operation over two vars/literals, apply some simple rules
 * to reduce work lateron (e.g. static calculations)
 * return is either a j_num, j_dbl or j_operation with tval1 being j_val
 * or j_operation and tval2 being j_num, j_dbl, j_val or j_operation */
tree *
make_operation(tree *var1, tree *op, tree *var2)
{
	tree *res = GDKzalloc(sizeof(tree));

	assert(var1 != NULL);
	assert(op != NULL && op->type == j_op);
	assert(var2 != NULL);

	if (var1->type == j_bool || var1->type == j_str ||
			var2->type == j_bool || var2->type == j_str)
	{
		/* we can't do arithmetic with these */
		res->type = j_error;
		res->sval = GDKstrdup("transform: cannot perform arithmetic on "
				"string or boolean values");
		freetree(var1);
		freetree(op);
		freetree(var2);
		return res;
	}

	if (var1->type != j_var && var1->type != j_operation) {
		/* left is value (literal) */
		if (var2->type == j_var || var2->type == j_operation) {
			/* right is var, or another operation, swap (want the var
			 * left eventually (if any)) */
			tree *t = var1;
			var1 = var2;
			var2 = t;
		} else {
			/* right is literal, pre-compute the value
			 * only cases left are number/double combinations */
			if (var1->type == j_num)
				var1->dval = (double)var1->nval;
			if (var2->type == j_num)
				var2->dval = (double)var2->nval;
			switch (op->cval) {
				case j_plus:
					if (var1->type == j_dbl || var2->type == j_dbl) {
						res->type = j_dbl;
						res->dval = var1->dval + var2->dval;
					} else {
						res->type = j_num;
						res->nval = var1->nval + var2->nval;
					}
					break;
				case j_min:
					if (var1->type == j_dbl || var2->type == j_dbl) {
						res->type = j_dbl;
						res->dval = var1->dval - var2->dval;
					} else {
						res->type = j_num;
						res->nval = var1->nval - var2->nval;
					}
					break;
				case j_multiply:
					if (var1->type == j_dbl || var2->type == j_dbl) {
						res->type = j_dbl;
						res->dval = var1->dval * var2->dval;
					} else {
						res->type = j_num;
						res->nval = var1->nval * var2->nval;
					}
					break;
				case j_divide:
					if (var1->type == j_dbl || var2->type == j_dbl) {
						res->type = j_dbl;
						res->dval = var1->dval / var2->dval;
					} else {
						res->type = j_num;
						res->nval = var1->nval / var2->nval;
					}
					break;
				default:
					assert(0);
			}
			freetree(var1);
			freetree(op);
			freetree(var2);
			return res;
		}
	}

	res->type = j_operation;
	res->tval1 = var1;
	res->tval2 = op;
	res->tval3 = var2;

	return res;
}

tree *
make_number(long long int n)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_num;
	res->nval = n;

	return res;
}

tree *
make_double(double d)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_dbl;
	res->dval = d;

	return res;
}

tree *
make_string(char *s)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_str;
	res->sval = s;

	return res;
}

tree *
make_bool(char b)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_bool;
	res->nval = b;

	return res;
}

/* creates a function call, with the optional arguments given */
tree *
make_func_call(char *name, tree *args)
{
	tree *res = GDKzalloc(sizeof(tree));

	assert(name != NULL);

	res->type = j_func;
	res->sval = name;
	res->tval1 = args;

	return res;
}

tree *
make_func_arg(tree *arg) {
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_func_arg;
	res->tval1 = arg;

	return res;
}

tree *
append_func_arg(tree *oarg, tree *narg)
{
	tree *w = oarg;

	assert(oarg != NULL && oarg->type == j_func_arg);
	assert(narg != NULL && narg->type == j_func_arg);

	while (w->next != NULL)
		w = w->next;

	w->next = narg;

	return oarg;
}

tree *
set_func_input_from_pipe(tree *func)
{
	assert(func != NULL && func->type == j_func);

	func->nval = 1;

	return func;
}


void
printtree(tree *t, int level, char op)
{
	(void) level;  /* indenting not used (yet) */
#define step 4
	while (t != NULL) {
		switch (t->type) {
			case j_output_var:
				if (op) {
					printf("j_output_var( %s ) ", t->sval);
				} else {
					printf("=> %s ", t->sval);
				}
				break;
			case j_output:
				if (op) {
					printf("j_output() ");
				} else {
					printf("=> <result> ");
				}
				break;
			case j_json:
				if (op) {
					printf("j_json( %s ) ", t->sval);
				} else {
					printf("%s ", t->sval);
				}
				break;
			case j_json_obj:
				if (op) {
					printf("j_json_obj( ");
					printtree(t->tval1, level + step, op);
					printf(") ");
				} else {
					printf("{ ");
					printtree(t->tval1, level + step, op);
					printf("} ");
				}
				break;
			case j_json_arr:
				if (op) {
					printf("j_json_arr( ");
					printtree(t->tval1, level + step, op);
					printf(") ");
				} else {
					printf("[ ");
					printtree(t->tval1, level + step, op);
					printf("] ");
				}
				break;
			case j_pair:
				if (op) {
					printf("j_pair( ");
					if (t->sval != NULL) {
						printf("\"%s\", ", t->sval);
					} else {
						printf("<deduced_name>, ");
					}
					printtree(t->tval1, level + step, op);
					printf(") ");
				} else {
					if (t->sval == NULL) {
						printf("<to be deduced from expansion> ");
					} else {
						printf("\"%s\": ", t->sval);
					}
					printtree(t->tval1, level + step, op);
					if (t->next != NULL)
						printf(", ");
				}
				break;
			case j_filter:
				if (op) {
					printf("j_filter( ");
					printtree(t->tval1, level + step, op);
					printf(", ");
					printtree(t->tval2, level + step, op);
					printf(") ");
				} else {
					printf("as ");
					printtree(t->tval1, level + step, op);
					printf("-> filter: ");
					printtree(t->tval2, level + step, op);
				}
				break;
			case j_transform:
				if (op) {
					printf("j_transform( ");
					printtree(t->tval1, level + step, op);
					printf(", ");
					printtree(t->tval2, level + step, op);
				} else {
					printf("as ");
					printtree(t->tval1, level + step, op);
					printf("-> transform: ");
					printtree(t->tval2, level + step, op);
				}
				t = t->tval3;
				while (t != NULL) {
					printf(", ");
					printtree(t, level + step, op);
					t = t->next;
				}
				if (op)
					printf(") ");
				break;
			case j_expand:
				if (op) {
					printf("j_expand( ");
					printtree(t->tval1, level + step, op);
					printf(", ");
					printtree(t->tval2, level + step, op);
					printf(") ");
				} else {
					printf("as ");
					printtree(t->tval1, level + step, op);
					printf("-> expand: ");
					printtree(t->tval2, level + step, op);
				}
				break;
			case j_unroll:
				if (op) {
					printf("j_unroll( ");
					printtree(t->tval1, level + step, op);
					printf(", ");
					printtree(t->tval2, level + step, op);
					printf(") ");
				} else {
					printf("as ");
					printtree(t->tval1, level + step, op);
					printf("-> expand unroll: ");
					printtree(t->tval2, level + step, op);
				}
				break;
			case j_group:
				if (op) {
					tree *i;
					printf("j_group( ");
					if (t->tval1 != NULL) {
						printtree(t->tval1, level + step, op);
						for (i = t->tval1->next; i != NULL; i = i->next) {
							printf(", ");
							printtree(i, level + step, op);
						}
					}
					printf("), ");
					printtree(t->tval2, level + step, op);
				} else {
					tree *i;
					if (t->tval1 == NULL || t->tval1->next == NULL)
						printf("-> ");
					printf("group by: ( ");
					if (t->tval1 != NULL) {
						printtree(t->tval1, level + step, op);
						for (i = t->tval1->next; i != NULL; i = i->next) {
							printf(", ");
							printtree(i, level + step, op);
						}
					}
					printf(") into ");
					printtree(t->tval2, level + step, op);
				}
				t = t->tval2->next;
				while (t != NULL) {
					printf(", ");
					printtree(t, level + step, op);
					t = t->next;
				}
				if (op)
					printf(") ");
				break;
			case j_join:
				if (op) {
					tree *i;
					printf("j_join( ");
					printtree(t->tval1, level + step, op);
					for (i = t->tval1->next; i != NULL; i = i->next) {
						printf(", ");
						printtree(i, level + step, op);
					}
					printf(", ( ");
					printtree(t->tval2, level + step, op);
					printf("), ");
					printtree(t->tval3, level + step, op);
				} else {
					tree *i;
					printf("as ");
					printtree(t->tval1, level + step, op);
					for (i = t->tval1->next; i != NULL; i = i->next) {
						printf(", ");
						printtree(i, level + step, op);
					}
					printf("-> join: where ( ");
					printtree(t->tval2, level + step, op);
					printf(") into ");
					printtree(t->tval3, level + step, op);
				}
				t = t->tval3->next;
				while (t != NULL) {
					printf(", ");
					printtree(t, level + step, op);
					t = t->next;
				}
				if (op)
					printf(") ");
				break;
			case j_sort:
				if (op) {
					printf("j_sort( ");
					printtree(t->tval1, level + step, op);
					printf(", ( ");
					printtree(t->tval2, level + step, op);
					printf(") ) ");
				} else {
					printf("as ");
					printtree(t->tval1, level + step, op);
					printf("-> sort: [ ");
					printtree(t->tval2, level + step, op);
					printf("] ");
				}
				break;
			case j_top:
				if (op) {
					printf("j_top( %lld ) ", t->nval);
				} else {
					printf("-> top: %lld ", t->nval);
				}
				break;
			case j_comp:
			case j_op:
				switch (t->cval) {
					case j_and:
						printf("&& ");
						break;
					case j_or:
						printf("|| ");
						break;
					case j_not:
						printf("! ");
						break;
					case j_equals:
						printf("== ");
						break;
					case j_nequal:
						printf("!= ");
						break;
					case j_greater:
						printf("> ");
						break;
					case j_gequal:
						printf(">= ");
						break;
					case j_less:
						printf("< ");
						break;
					case j_lequal:
						printf("<= ");
						break;
					case j_plus:
						printf("+ ");
						break;
					case j_min:
						printf("- ");
						break;
					case j_multiply:
						printf("* ");
						break;
					case j_divide:
						printf("/ ");
						break;
					case j_cinvalid:
						printf("<<invalid compare node>>");
						break;
				}
				break;
			case j_group_input:
				if (op) {
					printf("j_group_input( ");
					printtree(t->tval1, level + step, op);
					if (t->sval != NULL) {
						printf(", %s , ", t->sval);
						printtree(t->tval2, level + step, op);
						printf(", ");
						printtree(t->tval2, level + step, op);
					}
					printf(") ");
				} else {
					printtree(t->tval1, level + step, op);
					printf("each %s ", t->tval2->sval);
					if (t->sval != NULL) {
						printf("by %s = ", t->sval);
						printtree(t->tval2, level + step, op);
						printf("as ");
						printtree(t->tval3, level + step, op);
					}
				}
				/* avoid re-recursion after j_group */
				t = NULL;
				break;
			case j_join_input:
				if (op) {
					printf("j_join_input( ");
					printf("%lld , ", t->nval);
					printtree(t->tval1, level + step, op);
					printf(", ");
					printtree(t->tval2, level + step, op);
					printf(") ");
				} else {
					if (t->nval == 1)
						printf("preserve ");
					printtree(t->tval2, level + step, op);
					printf("in ");
					printtree(t->tval1, level + step, op);
				}
				/* avoid re-recursion after j_join */
				t = NULL;
				break;
			case j_pred:
				if (op) {
					printf("j_pred( ");
					printtree(t->tval1, level + step, op);
					printf(", ");
					printtree(t->tval2, level + step, op);
					printf(", ");
					printtree(t->tval3, level + step, op);
					printf(") ");
					if (t->next != NULL)
						printf("&& ");
				} else {
					printf("( ");
					printtree(t->tval1, level + step, op);
					printtree(t->tval2, level + step, op);
					printtree(t->tval3, level + step, op);
					printf(") ");
					if (t->next != NULL)
						printf("and ");
				}
				break;
			case j_operation:
				if (op) {
					printf("j_operation( ");
					printtree(t->tval1, level + step, op);
					printf(", ");
					printtree(t->tval2, level + step, op);
					printf(", ");
					printtree(t->tval3, level + step, op);
					printf(") ");
				} else {
					printf("( ");
					printtree(t->tval1, level + step, op);
					printtree(t->tval2, level + step, op);
					printtree(t->tval3, level + step, op);
					printf(") ");
				}
				break;
			case j_sort_arg:
				if (op) {
					printf("j_sort_arg( ");
					printtree(t->tval1, level + step, op);
					printf(", ");
					if (t->nval == 1) {
						printf("asc ");
					} else {
						printf("desc ");
					}
					printf(") ");
				} else {
					printtree(t->tval1, level + step, op);
					if (t->nval == 1) {
						printf("asc ");
					} else {
						printf("desc ");
					}
				}
				break;
			case j_var:
				if (op) {
					printf("j_var( %s%s ",
							t->sval == NULL ? "*" : t->sval,
							t->tval1 != NULL ? "." : "");
					if (t->tval1 != NULL)
						printtree(t->tval1, level + step, op);
					printf(") ");
				} else {
					printf("%s", t->sval == NULL ? "*" : t->sval);
					printtree(t->tval2, level + step, op);
					printf("%c", t->tval1 != NULL ? '.' : ' ');
					printtree(t->tval1, level + step, op);
				}
				break;
			case j_arr_idx:
				if (op) {
					printf("j_arr_idx( ");
					if (t->nval == -1) {
						printf("* ");
					} else {
						printf("%lld ", t->nval);
					}
					printf(") ");
				} else {
					if (t->nval == -1) {
						printf("[*]");
					} else {
						printf("[%lld]", t->nval);
					}
				}
				break;
			case j_num:
				printf("%lld ", t->nval);
				break;
			case j_dbl:
				printf("%f ", t->dval);
				break;
			case j_str:
				printf("'%s' ", t->sval);
				break;
			case j_bool:
				printf("%s ", t->nval == 0 ? "false" : "true");
				break;
			case j_func: {
				tree *i;
				if (op) {
					printf("j_func( %s, ", t->sval);
				} else {
					if (t->nval == 1)
						printf("-> ");
					printf("%s( ", t->sval);
				}
				if (t->tval1 != NULL) {
					printtree(t->tval1, level + step, op);
					for (i = t->tval1->next; i != NULL; i = i->next) {
						printf(", ");
						printtree(i, level + step, op);
					}
				}
				printf(") ");
			}	break;
			case j_func_arg:
				if (op) {
					printf("j_func_arg( ");
					printtree(t->tval1, level + step, op);
					printf(") ");
				} else {
					printtree(t->tval1, level + step, op);
				}
				/* avoid re-recursion after j_func */
				t = NULL;
				break;
			case j_error:
				if (op) {
					printf("j_error( %s )", t->sval);
				} else {
					printf("!%s\n", t->sval);
				}
				break;
			case j_invalid:
				printf("<<invalid tree node>>");
				break;
		}
		if (t != NULL)
			t = t->next;
	}
}

void
freetree(tree *j)
{
	tree *n;
	while (j != NULL) {
		if (j->sval != NULL)
			GDKfree(j->sval);
		if (j->tval1 != NULL)
			freetree(j->tval1);
		if (j->tval2 != NULL)
			freetree(j->tval2);
		if (j->tval3 != NULL)
			freetree(j->tval3);

		n = j->next;
		GDKfree(j);
		j = n;
	}
}

void
freevars(jvar *v) {
	jvar *n;
	while (v != NULL) {
		GDKfree(v->vname);
		BBPdecref(v->kind, TRUE);
		BBPdecref(v->string, TRUE);
		BBPdecref(v->integer, TRUE);
		BBPdecref(v->doble, TRUE);
		BBPdecref(v->array, TRUE);
		BBPdecref(v->object, TRUE);
		BBPdecref(v->name, TRUE);

		n = v->next;
		GDKfree(v);
		v = n;
	}
}

str
JAQLexecute(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	jc *j = NULL;
	int *ret = (int *)getArgReference(stk, pci, 0);
	str jaql = *(str *)getArgReference(stk, pci, 1);
	str err;

	(void)mb;
	
	if ((err = getContext(cntxt, &j)) != MAL_SUCCEED)
		GDKfree(err);

	if (j == NULL) {
		j = GDKzalloc(sizeof(jc));
		cntxt->state[MAL_SCENARIO_OPTIMIZE] = j;
	}

	j->buf = jaql;
	j->err[0] = '\0';
	yylex_init_extra(j, &j->scanner);

	do {
		yyparse(j);

		if (j->err[0] != '\0')
			break;
		if (j->p == NULL)
			continue;

		switch (j->explain) {
			case 0: /* normal (execution) mode */
			case 1: /* explain: show MAL-plan */ {
				str err;
				Symbol prg = newFunction(putName("user", 4), putName("jaql", 4),
						FUNCTIONsymbol);
				/* we do not return anything */
				setVarType(prg->def, 0, TYPE_void);
				setVarUDFtype(prg->def, 0);
				(void)dumptree(j, cntxt, prg->def, j->p);
				pushEndInstruction(prg->def);
				/* codegen could report an error */
				if (j->err[0] != '\0')
					break;

				chkProgram(cntxt->fdout, cntxt->nspace, prg->def);
				if (j->explain == 1) {
					printFunction(cntxt->fdout, prg->def, 0,
							LIST_MAL_STMT | LIST_MAPI);
				} else {
					err = (str)runMAL(cntxt, prg->def, 1, 0, 0, 0);
					freeMalBlk(prg->def);
					if (err != MAL_SUCCEED) {
						snprintf(j->err, sizeof(j->err), "%s", err);
						GDKfree(err);
						break;
					}
				}
			}	break;
			case 2: /* plan */
			case 3: /* planf */
				printtree(j->p, 0, j->explain == 3);
				printf("\n");
				break;
		}
		freetree(j->p);
		/* reset, j->buf has been reset by the lexer if EOF was found */
		j->p = NULL;
		j->esc_depth = 0;
		j->explain = 0;
	} while (j->buf != NULL && j->err[0] == '\0');

	yylex_destroy(j->scanner);
	j->scanner = NULL;
	/* freevars(j->vars);  should do only on client destroy */

	if (j->err[0] != '\0')
		throw(MAL, "jaql.execute", "%s", j->err);

	*ret = 0;
	return MAL_SUCCEED;
}

str
getContext(Client cntxt, jc **c)
{
	*c = ((jc *) cntxt->state[MAL_SCENARIO_OPTIMIZE]); 
	if (*c == NULL)
		throw(MAL, "jaql.context", "JAQL environment not found");
	return MAL_SUCCEED;
}

str
JAQLgetVar(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	jc *j;
	str msg = getContext(cntxt, &j);
	int *j1 = (int *)getArgReference(stk, pci, 0);
	int *j2 = (int *)getArgReference(stk, pci, 1);
	int *j3 = (int *)getArgReference(stk, pci, 2);
	int *j4 = (int *)getArgReference(stk, pci, 3);
	int *j5 = (int *)getArgReference(stk, pci, 4);
	int *j6 = (int *)getArgReference(stk, pci, 5);
	int *j7 = (int *)getArgReference(stk, pci, 6);
	str var = *(str *)getArgReference(stk, pci, 7);
	jvar *t;

	(void)mb;

	if (msg != MAL_SUCCEED)
		return msg;

	for (t = j->vars; t != NULL; t = t->next) {
		if (strcmp(t->vname, var) == 0) {
			*j1 = t->kind;
			*j2 = t->string;
			*j3 = t->integer;
			*j4 = t->doble;
			*j5 = t->array;
			*j6 = t->object;
			*j7 = t->name;
			break;
		}
	}
	if (t == NULL)
		throw(MAL, "jaql.getVar", "no such variable: %s", var);

	/* incref for MAL interpreter ref */
	BBPincref(t->kind, TRUE);
	BBPincref(t->string, TRUE);
	BBPincref(t->integer, TRUE);
	BBPincref(t->doble, TRUE);
	BBPincref(t->array, TRUE);
	BBPincref(t->object, TRUE);
	BBPincref(t->name, TRUE);
	return MAL_SUCCEED;
}

str
JAQLsetVar(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	jc *j;
	str msg = getContext(cntxt, &j);
	int *ret = (int *)getArgReference(stk, pci, 0);
	str var = *(str *)getArgReference(stk, pci, 1);
	int *j1 = (int *)getArgReference(stk, pci, 2);
	int *j2 = (int *)getArgReference(stk, pci, 3);
	int *j3 = (int *)getArgReference(stk, pci, 4);
	int *j4 = (int *)getArgReference(stk, pci, 5);
	int *j5 = (int *)getArgReference(stk, pci, 6);
	int *j6 = (int *)getArgReference(stk, pci, 7);
	int *j7 = (int *)getArgReference(stk, pci, 8);
	jvar *t;

	(void)mb;

	if (msg != MAL_SUCCEED)
		return msg;

	t = j->vars;
	if (t == NULL) {
		t = j->vars = GDKzalloc(sizeof(jvar));
	} else {
		while (t->next != NULL) {
			if (strcmp(t->vname, var) == 0)
				break;
			t = t->next;
		}
		if (t->next != NULL || strcmp(t->vname, var) == 0) {
			GDKfree(t->vname);
			BBPdecref(t->kind, TRUE);
			BBPdecref(t->string, TRUE);
			BBPdecref(t->integer, TRUE);
			BBPdecref(t->doble, TRUE);
			BBPdecref(t->array, TRUE);
			BBPdecref(t->object, TRUE);
			BBPdecref(t->name, TRUE);
		} else {
			t = t->next = GDKzalloc(sizeof(jvar));
		}
	}
	t->vname = GDKstrdup(var);
	t->kind = *j1;
	t->string = *j2;
	t->integer = *j3;
	t->doble = *j4;
	t->array = *j5;
	t->object = *j6;
	t->name = *j7;
	BBPincref(t->kind, TRUE);
	BBPincref(t->string, TRUE);
	BBPincref(t->integer, TRUE);
	BBPincref(t->doble, TRUE);
	BBPincref(t->array, TRUE);
	BBPincref(t->object, TRUE);
	BBPincref(t->name, TRUE);

	*ret = 0;
	return MAL_SUCCEED;
}

str
JAQLcast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int *)getArgReference(stk, pci, 0);
	int *b = (int *)getArgReference(stk, pci, 1);
	ValPtr t = getArgReference(stk, pci, 2);
	BAT *in;

	(void)mb;
	(void)cntxt;

	in = BBPquickdesc(ABS(*b), FALSE);
	if (*b < 0)
		in = BATmirror(in);

	if (in->ttype != t->vtype)
		throw(MAL, "jaql.cast", "BAT tail is not of required type");

	*ret = *b;
	return MAL_SUCCEED;
}

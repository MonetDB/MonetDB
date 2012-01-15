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

/* create filter action looping over the input array as ident,
 * performing pred condition to include each element in the array. */
tree *
make_jaql_filter(tree *var, tree *pred)
{
	tree *res = GDKzalloc(sizeof(tree));
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
	tree *res = GDKzalloc(sizeof(tree));
	char buf[8096];
	char *p, *q, *r;
	size_t l;
	tree *t;

	/* process tmpl, to turn into a C-like printf string and extract
	 * variable references */
	for (p = tmpl->sval, q = buf; *p != '\0' && q - buf < 8096; p++, q++)
	{
		l = strlen(var->sval);
		if (*p == *var->sval && strncmp(p, var->sval, l) == 0 &&
				!(p[l] >= 'A' && p[l] <= 'Z') &&
				!(p[l] >= 'a' && p[l] <= 'z') &&
				!(p[l] >= '0' && p[l] <= '9') && p[l] != '_')
		{
			p += l;
			if (res->tval3 == NULL) {
				t = res->tval3 = make_varname(var->sval);
			} else {
				t = t->next = make_varname(var->sval);
			}
			*q++ = '%';
			*q++ = 's';

			while (*p == '.') {
				r = p;
				for (; *p != '\0'; p++) {
					if (*p != '_' &&
							!(p[l] >= 'A' && p[l] <= 'Z') &&
							!(p[l] >= 'a' && p[l] <= 'z') &&
							!(p[l] >= '0' && p[l] <= '9'))
					{
						*q = *p;
						*p = '\0';
						append_varname(t, r);
						*p = *q;
					}
				}
			}
			/* we don't support x[n] yet */
		}
		*q = *p;
	}
	*q = '\0';

	GDKfree(tmpl->sval);
	tmpl->sval = GDKstrdup(buf);

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
	/* make execution easier by always giving expand an argument to
	 * expand, which defaults to the var we're looping over as (usually
	 * $, but modified with "each xxx") */
	assert(var->type == j_var);
	if (expr == NULL) {
		expr = GDKzalloc(sizeof(tree));
		expr->type = j_var;
		expr->sval = GDKstrdup(var->sval);
	} else if (strcmp(var->sval, expr->sval) != 0) {
		char buf[128];
		snprintf(buf, sizeof(buf), "expand: unknown variable: %s", expr->sval);
		res->type = j_error;
		res->sval = GDKstrdup(buf);
		res->tval1 = NULL;
		freetree(expr);
		freetree(var);
		return res;
	}
	res->tval2 = expr;

	return res;
}

/* create a sort operation defined by comparator in expr */
tree *
make_jaql_sort(tree *var, tree *expr)
{
	tree *res, *w;

	assert(var != NULL && var->type == j_var);
	assert(expr != NULL && expr->type == j_sort_arg);

	res = GDKzalloc(sizeof(tree));

	for (w = expr; w != NULL; w = w->next) {
		if (strcmp(var->sval, w->tval1->sval) != 0) {
			char buf[128];
			snprintf(buf, sizeof(buf), "sort: unknown variable: %s",
					w->tval1->sval);
			res->type = j_error;
			res->sval = GDKstrdup(buf);
			freetree(expr);
			freetree(var);
			return res;
		}
	}

	res->type = j_sort;
	res->tval1 = var;
	res->tval2 = expr;

	return res;
}

/* create predicate, chaining onto the previous predicate ppred,
 * applying a comparison (AND/OR/NOT currently) with the next predicate
 * pred */
tree *
make_cpred(tree *ppred, tree *comp, tree *pred)
{
	tree *res;

	assert(comp == NULL || comp->type == j_comp);
	assert(pred != NULL && (pred->type == j_pred || pred->type == j_cmpnd));

	/* shortcut to optimize non-not constructions */
	if (comp == NULL && ppred == NULL)
		return pred;

	/* optimise the case where comp is _NOT, and pred is a variable to
	 * rewrite its comp to _NEQUALS */
	if (comp->nval == _NOT && pred->type == j_pred &&
			pred->tval2->nval == _EQUALS)
	{
		pred->tval2->nval = _NEQUAL;
		return pred;
	}

	res = GDKzalloc(sizeof(tree));
	res->type = j_cmpnd;
	res->tval1 = ppred;
	res->tval2 = comp;
	res->tval3 = pred;

	return res;
}

/* create predicate of simple form that compares an identifier to a
 * value */
tree *
make_pred(tree *var, tree *comp, tree *value)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_pred;
	res->tval1 = var;
	res->tval2 = comp;
	res->tval3 = value;

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

/* create a variable name from ident */
tree *
make_varname(char *ident)
{
	tree *res = GDKzalloc(sizeof(tree));
	res->type = j_var;
	res->sval = ident;

	return res;
}

/* append an object indirection to the variable in var with the name
 * from ident */
tree *
append_varname(tree *var, char *ident)
{
	tree *t = var;

	/* find last in chain to append to */
	while (t->tval1 != NULL)
		t = t->tval1;
	t = t->tval1 = GDKzalloc(sizeof(tree));
	t->type = j_var;
	t->sval = ident;

	return var;
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
			case j_cmpnd:
				if (op) {
					printf("j_cmpnd( ");
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
			case j_comp:
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
					default:
						printf("<unknown comp> ");
				}
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
				} else {
					printtree(t->tval1, level + step, op);
					printtree(t->tval2, level + step, op);
					printtree(t->tval3, level + step, op);
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
							t->sval, t->tval1 != NULL ? "." : "");
					if (t->tval1 != NULL)
						printtree(t->tval1, level + step, op);
					printf(") ");
				} else {
					printf("%s%c", t->sval, t->tval1 != NULL ? '.' : ' ');
					printtree(t->tval1, level + step, op);
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
			case j_error:
				if (op) {
					printf("j_error( %s )", t->sval);
				} else {
					printf("!%s\n", t->sval);
				}
				break;
			default:
				printf("<unknown type> ");
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
				(void)dumptree(j, prg->def, j->p);
				pushEndInstruction(prg->def);
				/* codegen could report an error */
				if (j->err[0] != '\0')
					break;

				chkProgram(cntxt->nspace, prg->def);
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

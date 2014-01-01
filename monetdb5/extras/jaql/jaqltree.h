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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef JAQLTREE_H
#define JAQLTREE_H 1

typedef struct _jvar {
	char *vname;
	int kind;
	int string;
	int integer;
	int doble;
	int array;
	int object;
	int name;
	struct _jvar *next;
} jvar;

typedef struct _jc {
	struct _tree *p;
	int esc_depth;
	char expect_json;
	char *buf;
	size_t start;
	size_t pos;
	char *scanbuf;
	void *scanstreamin;
	void *scanstreamout;
	char *tokstart;
	char err[1024];
	void *scanner;
	char explain:1,
		 debug:2,
		 trace:3,
		 plan:4,
		 planf:5,
		 time:6,
		 mapimode:7,
		 scanstreameof:8;
	jvar *vars;
	int j1, j2, j3, j4, j5, j6, j7, startoid;
	char ro1:1, ro2:2, ro3:3, ro4:4, ro5:5, ro6:6, ro7:7;
	int vtop;
	struct {
		long long int parse;
		long long int gencode;
		long long int optimise;
	} timing;
} jc;

/* the order here does matter for filter */
enum treetype {
	j_invalid,
	j_var,
	j_func,
	j_pred,
	j_operation,
	j_null,
	j_bool,
	j_num,
	j_dbl,
	j_str,
	j_json,
	j_json_obj,
	j_json_arr,
	j_output_var,
	j_output,
	j_filter,
	j_transform,
	j_expand,
	j_unroll,
	j_group,
	j_join,
	j_sort,
	j_top,
	j_op,
	j_comp,
	j_group_input,
	j_join_input,
	j_sort_arg,
	j_arr_idx,
	j_pair,
	j_func_arg,
	j_error
};

enum comptype {
	j_cinvalid,
	j_equals,
	j_nequal,
	j_greater,
	j_gequal,
	j_less,
	j_lequal,
	j_in,
	j_not,
	j_or,
	j_and,
	/* bit of hacked in to reuse same field in struct */
	j_plus,
	j_min,
	j_multiply,
	j_divide
};

typedef struct _tree {
	enum treetype type;
	long long int nval;
	double dval;
	char *sval;
	enum comptype cval;
	struct _tree *tval1;
	struct _tree *tval2;
	struct _tree *tval3;
	struct _tree *next;
} tree;

tree *make_json_output(char *ident);
tree *make_json(char *json);
tree *make_json_object(tree *obj);
tree *make_json_array(tree *arr);
tree *append_jaql_pipe(tree *oaction, tree *naction);
tree *make_jaql_filter(tree *var, tree *pred);
tree *make_jaql_transform(tree *var, tree *tmpl);
tree *make_jaql_expand(tree *var, tree *expr);
tree *make_jaql_group(tree *inputs, tree *tmpl, tree *var);
tree *make_jaql_join(tree *inputs, tree *pred, tree *tmpl);
tree *make_jaql_sort(tree *var, tree *expr);
tree *make_jaql_top(long long int num);
tree *make_unroll(tree *var);
tree *make_array_index(long long int idx, char isstar);
tree *make_pred(tree *var, tree *comp, tree *value);
tree *make_sort_arg(tree *var, long long int asc);
tree *append_sort_arg(tree *osarg, tree *nsarg);
tree *make_varname(char *ident);
tree *append_varname(tree *var, char *ident);
tree *append_vararray(tree *var, long long int idx, char isstar);
tree *make_pair(char *name, tree *val);
tree *append_pair(tree *opair, tree *npair);
tree *append_elem(tree *oelem, tree *nelem);
tree *make_group_input(char *grpkeyvar, tree *grpkey, tree *walkvar);
tree *append_group_input(tree *oginp, tree *nginp);
tree *set_group_input_var(tree *ginp, tree *inpvar);
tree *make_join_input(char preserve, tree *var, tree *invar);
tree *append_join_input(tree *ojinp, tree *njinp);
tree *make_comp(enum comptype t);
tree *make_op(enum comptype t);
tree *make_operation(tree *var1, tree *op, tree *var2);
tree *make_number(long long int n);
tree *make_double(double d);
tree *make_string(char *s);
tree *make_bool(char b);
tree *make_null(void);
tree *make_func_call(char *name, tree *args);
tree *make_func_arg(tree *arg);
tree *append_func_arg(tree *oarg, tree *narg);
tree *set_func_input_from_pipe(tree *func);
void freetree(tree *t);
void freevars(jvar *v);

#endif


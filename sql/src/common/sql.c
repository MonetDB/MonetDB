
/* TODO
   fix error output, use (x)gettext to internationalize this
 */

#include <unistd.h>
#include "sql.h"
#include "symbol.h"
#include "statement.h"
#include <mem.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include <scope.h>

#define NAMELEN 64

#define create_stmt_list() list_create((fdestroy)&stmt_destroy)
#define create_column_list() list_create((fdestroy)NULL)
#define create_atom_list() list_create((fdestroy)&atom_destroy)
/* 
 * some string functions.
 * -- need to be move to seperate file 
 */
/* implace cast to lower case string */
char *toLower(char *v)
{
	char *s = v;
	while (*v) {
		*v = (char) tolower(*v);
		v++;
	}
	return s;
}

/* concat s1,s2 into a new result string */
char *strconcat(const char *s1, const char *s2)
{
	int i, j, l1 = strlen(s1);
	int l2 = strlen(s2) + 1;
	char *new_s = NEW_ARRAY(char, l1 + l2);
	for (i = 0; i < l1; i++) {
		new_s[i] = s1[i];
	}
	for (j = 0; j < l2; j++, i++) {
		new_s[i] = s2[j];
	}
	return new_s;
}

/* 
 * For debugging purposes we need to be able to convert sql-tokens to 
 * a string representation.
 */
const char *token2string(int token)
{
	switch (token) {
	case SQL_CREATE_SCHEMA:
		return "Create Schema";
	case SQL_CREATE_TABLE:
		return "Create Table";
	case SQL_CREATE_VIEW:
		return "Create View";
	case SQL_DROP_SCHEMA:
		return "Drop Schema";
	case SQL_DROP_TABLE:
		return "Drop Table";
	case SQL_DROP_VIEW:
		return "Drop View";
	case SQL_ALTER_TABLE:
		return "Alter Table";
	case SQL_NAME:
		return "Name";
	case SQL_USER:
		return "User";
	case SQL_PATH:
		return "Path";
	case SQL_CHARSET:
		return "Char Set";
	case SQL_TABLE:
		return "Table";
	case SQL_COLUMN:
		return "Column";
	case SQL_COLUMN_OPTIONS:
		return "Column Options";
	case SQL_CONSTRAINT:
		return "Constraint";
	case SQL_CHECK:
		return "Check";
	case SQL_DEFAULT:
		return "default";
	case SQL_NOT_NULL:
		return "Not Null";
	case SQL_NULL:
		return "Null";
	case SQL_UNIQUE:
		return "Unique";
	case SQL_PRIMARY_KEY:
		return "Primary Key";
	case SQL_FOREIGN_KEY:
		return "Foreign Key";
	case SQL_COMMIT:
		return "Commit";
	case SQL_ROLLBACK:
		return "Rollback";
	case SQL_SELECT:
		return "Select";
	case SQL_WHERE:
		return "Where";
	case SQL_FROM:
		return "From";
	case SQL_UNION:
		return "Union";
	case SQL_UPDATE_SET:
		return "Update Set";
	case SQL_INSERT_INTO:
		return "Insert Into";
	case SQL_INSERT:
		return "Insert";
	case SQL_DELETE:
		return "Delete";
	case SQL_VALUES:
		return "Values";
	case SQL_ASSIGN:
		return "Assignment";
	case SQL_ORDERBY:
		return "Order By";
	case SQL_GROUPBY:
		return "Group By";
	case SQL_DESC:
		return "Desc";
	case SQL_AND:
		return "And";
	case SQL_OR:
		return "Or";
	case SQL_EXISTS:
		return "Exists";
	case SQL_NOT_EXISTS:
		return "Not Exists";
	case SQL_UNOP:
		return "Unop";
	case SQL_BINOP:
		return "Binop";
	case SQL_BETWEEN:
		return "Between";
	case SQL_NOT_BETWEEN:
		return "Not Between";
	case SQL_LIKE:
		return "Like";
	case SQL_NOT_LIKE:
		return "Not Like";
	case SQL_IN:
		return "In";
	case SQL_NOT_IN:
		return "Not In";
	case SQL_GRANT:
		return "Grant";
	case SQL_PARAMETER:
		return "Parameter";
	case SQL_AGGR:
		return "Aggregates";
	case SQL_COMPARE:
		return "Compare";
	case SQL_TEMP_LOCAL:
		return "Local Temporary";
	case SQL_TEMP_GLOBAL:
		return "Global Temporary";
	case SQL_INT_VALUE:
		return "Integer";
	case SQL_ATOM:
		return "Atom";
	case SQL_ESCAPE:
		return "Escape";
	case SQL_CAST:
		return "Cast";
	case SQL_CASE:
		return "Case";
	case SQL_WHEN:
		return "When";
	case SQL_COALESCE:
		return "Coalesce";
	case SQL_NULLIF:
		return "Nullif";
	case SQL_JOIN:
		return "Join";
	case SQL_CROSS:
		return "Cross";
	case SQL_COPYFROM:
		return "Copy From";
	case SQL_COPYTO:
		return "Copy To";
	default:
		return "unknown";
	}
}


static stmt *sql_select(context * sql, scope * scp, int distinct,
			dlist * selection, dlist * into,
			dlist * table_exp, symbol * orderby);
static stmt *sql_simple_select(context * sql, scope * scp,
			       dlist * selection);
static stmt *sql_logical_exp(context * sql, scope * scp, symbol * sc,
			     group * grp, stmt * subset);
static stmt *sql_value_exp(context * sql, scope * scp, symbol * se,
			   group * grp, stmt * subset);

static stmt *sets2pivot(context * sql, list * ll);
static stmt *set2pivot(context * sql, list * l);
static tvar *query_exp_optname(context * sql, scope * scp, symbol * q);

static stmt *sql_subquery(context * sql, scope * scp, symbol * sq)
{
	dlist *q = sq->data.lval;
	assert(sq->token == SQL_SELECT);
	return
	    sql_select(sql, scp,
		       q->h->data.ival,
		       q->h->next->data.lval,
		       q->h->next->next->data.lval,
		       q->h->next->next->next->data.lval,
		       q->h->next->next->next->next->data.sym);
}

static stmt *scope_subquery(context * sql, scope * scp, symbol * sq)
{
	scope *nscp = scope_open(scp);
	stmt *s = sql_subquery(sql, nscp, sq);
	scp = scope_close(nscp);
	return s;
}


/* 
 * Column references, can be done using simple names or aliases and
 * using a combination of table name and column name (or alias).
 * The sql_column_ref finds the column based on the specification from
 * symbol column_r.
 */

static stmt *sql_column_ref(context * sql, scope * scp, symbol * column_r)
{
	stmt *cs = NULL;
	dlist *l = column_r->data.lval;

	assert(column_r->token == SQL_COLUMN
	       && column_r->type == type_list);

	if (dlist_length(l) == 1) {
		char *name = l->h->data.sval;

		if (!(cs = scope_bind(scp, NULL, name))) {
			snprintf(sql->errstr, ERRSIZE,
				 _("Column: %s unknown"), name);
			scope_dump(scp);
		}
	} else if (dlist_length(l) == 2) {
		char *tname = l->h->data.sval;
		char *cname = l->h->next->data.sval;

		if (!(cs = scope_bind(scp, tname, cname))) {
			snprintf(sql->errstr, ERRSIZE,
				 _("Column: %s.%s unknown"), tname, cname);
			scope_dump(scp);
		}
	} else if (dlist_length(l) >= 3) {
		snprintf(sql->errstr, ERRSIZE,
			 _("TODO: column names of level >= 3\n"));
	}
	return cs;
}

static char *table_name(dlist * tname)
{
	assert(tname && tname->h);

	if (dlist_length(tname) == 1) {
		return tname->h->data.sval;
	} else if (dlist_length(tname) == 2) {
		return tname->h->next->data.sval;
	}
	return "Unknown";
}

static table *create_table_intern(context * sql, schema * schema,
				  char *name, char *query, stmt * sq)
{
	node *m;
	catalog *cat = sql->cat;
	table *table = cat_create_table(cat, 0, schema, name, 0, query);

       	for ( m = sq->op1.lval->h; m; m = m->next ) {
		stmt *st = m->data;
		char *cname = column_name(st);
		char *ctype = tail_type(st)->sqlname;
		column *col = cat_create_column(cat, 0,
						table, cname, ctype,
						"NULL", 1);
		col->s = st;
		st_attache(st, NULL);
	}
	return table;
}

static tvar *scope_add_table_columns(scope * scp, table * t, char *tname)
{
	tvar *tv = scope_add_table(scp, t, NULL, tname);
	node *n = t->columns->h;
	for (; n; n = n->next) {
		column *c = n->data;
		stmt *sc = stmt_column(c, tv);
		table_add_column(tv, c, sc, tv->tname, c->name);
	}
	return tv;
}

static tvar *table_optname(context * sql, scope * scp, stmt * sq,
			   char *query, symbol * optname)
{
	node *m;
	char *tname = NULL;
	dlist *columnrefs = NULL;
	tvar *tv;
	table *tab;
	schema *schema = sql->cat->cur_schema;

	if (optname && optname->token == SQL_NAME) {
		tname = optname->data.lval->h->data.sval;
		columnrefs = optname->data.lval->h->next->data.lval;
	}
	tab = create_table_intern(sql, schema, tname, query, sq);

	tv = scope_add_table(scp, tab, sq, tname);
	if (columnrefs) {
		dnode *d;

		for (m = sq->op1.lval->h, d = columnrefs->h; d && m; 
				d = d->next, m = m->next) {
			stmt *st = m->data;
			table_add_column(tv, NULL, st, tname, d->data.sval);
		}
	} else if (tname) {
		/* foreach column add column name */
		for ( m = sq->op1.lval->h; m; m = m->next) {
			stmt *st = m->data;
			char *cname = column_name(st);

			table_add_column(tv, NULL, st, tname, cname);
		}
	} else {
		/* foreach column add full basetable,column name */
		for ( m = sq->op1.lval->h; m; m = m->next) {
			stmt *st = m->data;
			char *cname = column_name(st);
			stmt *bc = tail_column(st);

			while (bc->type == st_column &&
			       (bc->h == NULL || bc->h->tname == NULL) &&
			       bc->op1.cval->table->name == NULL
			       && bc->op1.cval->s != NULL) {
				bc = tail_column(bc->op1.cval->s);
			}

			if (bc->h && bc->h->tname) {
				table_add_column(tv, NULL, st,
						 bc->h->tname, cname);
			} else {
				table_add_column(tv, NULL, st,
						 bc->op1.cval->table->
						 name, cname);
			}
		}
	}
	return tv;
}

static tvar *sql_subquery_optname(context * sql, scope * scp,
				  symbol * query)
{
	scope *nscp = scope_open(scp);
	tvar *res = NULL;
	stmt *sq = sql_subquery(sql, nscp, query);

	if (!sq)
		return NULL;

	res = table_optname(sql, scp, sq, query->sql,
			    query->data.lval->t->data.sym);
	scp = scope_close(nscp);
	return res;
}

static tvar *table_ref(context * sql, scope * scp, symbol * tableref)
{
	char *tname = NULL;
	table *t = NULL;

	/* todo handle opt_table_ref 
	   (ie tableref->data.lval->h->next->data.sym */

	if (tableref->token == SQL_NAME) {
		tname = table_name(tableref->data.lval->h->data.lval);
		t = cat_bind_table(sql->cat, sql->cat->cur_schema, tname);
		if (!t) {
			snprintf(sql->errstr, ERRSIZE,
				 _("Unknown table %s"), tname);
			return NULL;
		}
		if (tableref->data.lval->h->next->data.sym) {	/* AS */
			tname =
			    tableref->data.lval->h->next->data.sym->data.
			    lval->h->data.sval;
		}
		return scope_add_table_columns(scp, t, tname);
	} else if (tableref->token == SQL_SELECT) {
		return sql_subquery_optname(sql, scp, tableref);
	} else {
		return query_exp_optname(sql, scp, tableref);
	}
}

static char *schema_name(dlist * name_auth)
{
	assert(name_auth && name_auth->h);

	return name_auth->h->data.sval;
}

static char *schema_auth(dlist * name_auth)
{
	assert(name_auth && name_auth->h && dlist_length(name_auth) == 2);

	return name_auth->h->next->data.sval;
}

static stmt *find_subset(stmt * subset, tvar * t)
{
	if (t){
		node *n;
		for (n = subset->op1.lval->h; n; n = n->next) {
			stmt *s = n->data;
			if (s->t == t) 
				return s;
		}
	}
	return NULL;
}

/* before the pivot table is created we need to check on both head and
 * tail for the subset.
 * */
static stmt *complex_find_subset(stmt * subset, tvar * t)
{
	if (t) {
		node *n;
		for (n = subset->op1.lval->h; n; n = n->next) {
			stmt *s = n->data;
			if (s->t == t) 
				return s;
		}
		for (n = subset->op1.lval->h; n; n = n->next) {
			stmt *s = n->data;
			if (s->h == t) 
				return s;
		}
	}
	return NULL;
}


static stmt *first_subset(stmt * subset)
{
	node *n = subset->op1.lval->h;
	if (n)
		return n->data;
	return NULL;
}

static stmt *check_types(context * sql, sql_type * ct, stmt * s)
{
	sql_type *st = tail_type(s);

	if (st) {
		sql_type *t = st;

		/* check if the implementation types are the same */
		while (t && strcmp(t->name, ct->name) != 0) {
			t = t->cast;
		}
		if (!t) {	/* try to convert if needed */
			sql_func *c = sql_bind_func_result("convert",
						       st->sqlname, NULL,
						       NULL,
						       ct->sqlname);
			if (c)
				return stmt_unop(s, c);
		}
		if (!t || strcmp(t->name, ct->name) != 0) {
			snprintf(sql->errstr, ERRSIZE,
				 _("Types %s and %s are not equal"),
				 st->sqlname, ct->sqlname);
			return NULL;
		} else if (t != st) {

			sql_func *c = sql_bind_func_result("convert",
						       st->sqlname, NULL,
						       NULL,
						       ct->sqlname);

			if (!c) {
				snprintf(sql->errstr, ERRSIZE,
					 _
					 ("Missing convert function from %s to %s"),
					 st->sqlname, ct->sqlname);
				return NULL;
			}
			return stmt_unop(s, c);
		}
	} else {
		snprintf(sql->errstr, ERRSIZE,
			 _("Statement has no type information"));
		return NULL;
	}
	return s;
}


/* The case/when construction in the selection works on the resulting
   table (ie. on the marked columns). We just need to know which oid list
   is involved (ie. find one subset).
   We need to check if for all results the types are the same. 
 */
static stmt *sql_search_case(context * sql, scope * scp,
			     dlist * when_search_list,
			     symbol * opt_else, group * grp, stmt * subset)
{
	list *conds = create_stmt_list();
	list *results = create_stmt_list();
	dnode *dn = when_search_list->h;
	sql_type *restype = NULL;
	stmt *res = NULL;
	node *n, *m;

	if (dn) {
		dlist *when = dn->data.sym->data.lval;
		stmt *cond, *result;

		cond = sql_logical_exp(sql, scp, when->h->data.sym, grp,
				    subset);
		result = sql_value_exp(sql, scp, when->h->next->data.sym, grp,
				  subset);
		if (!cond || !result) {
			list_destroy(conds);
			list_destroy(results);
			return NULL;
		}
		list_prepend(conds, cond);
		list_prepend(results, result);

		restype = tail_type(result);
	}
	if (!restype) {
		snprintf(sql->errstr, ERRSIZE,
			 _("Error: result type missing"));
		list_destroy(conds);
		list_destroy(results);
		return NULL;
	}
	for (dn = dn->next; dn; dn = dn->next) {
		sql_type *tpe = NULL;
		dlist *when = dn->data.sym->data.lval;
		stmt *cond, *result;

		cond = sql_logical_exp(sql, scp, when->h->data.sym, grp,
				    subset);
		result = sql_value_exp(sql, scp, when->h->next->data.sym, grp,
				  subset);
		if (!cond || !result) {
			list_destroy(conds);
			list_destroy(results);
			return NULL;
		}
		list_prepend(conds, cond);
		list_prepend(results, result);

		tpe = tail_type(result);
		if (!tpe) {
			snprintf(sql->errstr, ERRSIZE,
				 _("Error: result type missing"));
			list_destroy(conds);
			list_destroy(results);
			return NULL;
		}
		if (restype != tpe) {
			snprintf(sql->errstr, ERRSIZE,
				 _
				 ("Error: result types %s,%s of case are not compatible"),
				 restype->sqlname, tpe->sqlname);
			list_destroy(conds);
			list_destroy(results);
			return NULL;
		}
	}
	if (subset) {
		res = first_subset(subset);
		if (!res) {
			snprintf(sql->errstr, ERRSIZE,
				 _("Subset not found for case stmt"));
			list_destroy(conds);
			list_destroy(results);
			return NULL;
		}
	} else {
		printf("Case in query not handled jet\n");
	}
	if (opt_else) {
		stmt *result = sql_value_exp(sql, scp, opt_else, grp, subset);
		sql_type *t;

		if (!result) {
			list_destroy(conds);
			list_destroy(results);
			return NULL;
		}
		t = tail_type(result);
		if (restype != t) {
			sql_func *c = sql_bind_func_result("convert",
						       t->sqlname, NULL,
						       NULL,
						       restype->sqlname);
			if (!c) {
				snprintf(sql->errstr, ERRSIZE,
					 _
					 ("Cast between (%s,%s) not possible"),
					 t->sqlname, restype->sqlname);
				return NULL;
			}
			result = stmt_unop(result, c);
		}
		if (result->nrcols <= 0) {
			res = stmt_const(res, result);
		} else {
			res = result;
		}
	} else {
		res = stmt_const(res, NULL);
	}
	for (n = conds->h, m = results->h; n && m;
	     n = n->next, m = m->next) {
		stmt *cond = n->data;
		stmt *result = m->data;

		/* need more semantic tests here */
		if (cond->type == st_sets) {
			node *k = cond->op1.lval->h;
			stmt *cur = NULL;

			if (k) {
				list *l1 = k->data;
				cur = l1->h->data;
				k = k->next;
				for (; k; k = k->next) {
					list *l2 = k->data;
					stmt *st = l2->h->data;
					cur = stmt_union(cur, st);
				}
				cond = cur;
			}
		}
		if (cond->type == st_set) {
			cond = cond->op1.lval->h->data;
		}
		if (result->nrcols <= 0)
			result = stmt_const(cond, result);
		else 
			result = stmt_semijoin(result, cond);
		res = stmt_replace(res, result);
	}
	list_destroy(conds);
	list_destroy(results);
	return res;
}

static stmt *sql_case(context * sql, scope * scp, symbol * se,
		      group * grp, stmt * subset)
{
	dlist *l = se->data.lval;
	if (l->h->type == type_list) {
		dlist *when_search_list = l->h->data.lval;
		symbol *opt_else = l->h->next->data.sym;
		return sql_search_case(sql, scp, when_search_list,
				       opt_else, grp, subset);
	} else {
		/*sql_value_case(); */
		printf("sql_value_case not handeled\n");
		return NULL;
	}
	printf("case %d %s\n", se->token, token2string(se->token));
	return NULL;
}

static stmt *sql_cast(context * sql, scope * scp, symbol * se,
		      group * grp, stmt * subset)
{

	dlist *dl = se->data.lval;
	symbol *s = dl->h->data.sym;
	char *tpe = dl->h->next->data.sval;

	stmt *l = sql_value_exp(sql, scp, s, grp, subset);

	if (l) {
		sql_type *st = tail_type(l);
		sql_type *rt = sql_bind_type(tpe);
		sql_func *c = sql_bind_func_result("convert", st->sqlname,
					       NULL, NULL, rt->sqlname);

		if (!c) {
			snprintf(sql->errstr, ERRSIZE,
				 _("CAST operator: cast(%s,%s) unknown"),
				 st->sqlname, tpe);
			stmt_destroy(l);
			return NULL;
		}
		return stmt_unop(l, c);
	}
	return NULL;
}

static stmt *sql_triop(context * sql, scope * scp, symbol * se,
		       group * grp, stmt * subset)
{
	dnode *l = se->data.lval->h;
	stmt *ls = sql_value_exp(sql, scp,
				 l->next->data.sym,
				 grp, subset);
	stmt *rs1 = sql_value_exp(sql, scp,
				  l->next->next->data.sym,
				  grp, subset);
	stmt *rs2 = sql_value_exp(sql, scp,
				  l->next->next->next->
				  data.sym, grp, subset);
	sql_func *f = NULL;
	if (!ls || !rs1 || !rs2)
		return NULL;
	f = sql_bind_func(l->data.sval,
			  tail_type(ls)->sqlname,
			  tail_type(rs1)->sqlname,
			  tail_type(rs2)->sqlname);
	if (f) {
		return stmt_triop(ls, rs1, rs2, f);
	} else {
		snprintf(sql->errstr, ERRSIZE,
			 _
			 ("operator: %s(%s,%s,%s) unknown"),
			 l->data.sval,
			 tail_type(ls)->sqlname,
			 tail_type(rs1)->sqlname, tail_type(rs2)->sqlname);
	}
	return NULL;
}

static stmt *sql_binop(context * sql, scope * scp, symbol * se,
		       group * grp, stmt * subset)
{
	dnode *l = se->data.lval->h;
	stmt *ls = sql_value_exp(sql, scp, l->next->data.sym,
				 grp, subset);
	stmt *rs = sql_value_exp(sql, scp,
				 l->next->next->data.sym,
				 grp, subset);
	sql_func *f = NULL;
	if (!ls || !rs)
		return NULL;
	f = sql_bind_func(l->data.sval,
			  tail_type(ls)->sqlname,
			  tail_type(rs)->sqlname, NULL);
	if (f) {
		return stmt_binop(ls, rs, f);
	} else {
		sql_func *c = NULL;
		int w = 0;
		sql_type *t1 = tail_type(ls);
		sql_type *t2 = tail_type(rs);
		if (t1->nr > t2->nr) {
			sql_type *s = t1;
			t1 = t2;
			t2 = s;
			w = 1;
		}
		c = sql_bind_func_result("convert",
					 t1->sqlname, NULL,
					 NULL, t2->sqlname);
		f = sql_bind_func(l->data.sval,
				  t2->sqlname, t2->sqlname, NULL);

		if (f && c) {
			if (!w) {
				ls = stmt_unop(ls, c);
			} else {
				rs = stmt_unop(rs, c);
			}
			return stmt_binop(ls, rs, f);
		} else {
			snprintf(sql->errstr, ERRSIZE,
				 _
				 ("Binary operator: %s(%s,%s) unknown"),
				 l->data.sval,
				 tail_type(ls)->sqlname,
				 tail_type(rs)->sqlname);
		}
	}
	return NULL;
}

static stmt *sql_unop(context * sql, scope * scp, symbol * se,
		      group * grp, stmt * subset)
{
	dnode *l = se->data.lval->h;
	sql_func *f = NULL;
	stmt *rs = sql_value_exp(sql, scp, l->next->data.sym,
				 grp, subset);
	if (!rs)
		return NULL;
	f = sql_bind_func(l->data.sval,
			  tail_type(rs)->sqlname, NULL, NULL);
	if (f) {
		return stmt_unop(rs, f);
	} else {
		snprintf(sql->errstr, ERRSIZE,
			 _
			 ("Unary operator: %s(%s) unknown"),
			 l->data.sval, tail_type(rs)->sqlname);
	}
	return NULL;
}


static stmt *sql_aggrop(context * sql, scope * scp, symbol * se,
			group * grp, stmt * subset)
{
	dlist *l = se->data.lval;
	sql_aggr *a = NULL;
	int distinct = l->h->next->data.ival;
	stmt *s = NULL;
	if (!l->h->next->next->data.sym) {	/* count(*) case */
		cvar *cv;
		if (strcmp(l->h->data.sval, "count") != 0) {
			snprintf(sql->errstr, ERRSIZE,
				 _
				 ("Aggregate: Cannot do a %s(*)"),
				 l->h->data.sval);
			return NULL;
		}
		if (grp) {
			a = sql_bind_aggr(l->h->data.sval, NULL);
			return stmt_aggr(grp->grp, a, grp);
		}
		cv = scope_first_column(scp);
		if (cv && subset) {
			stmt *foundsubset = find_subset(subset, cv->table);

			s = stmt_join(foundsubset, cv->s, cmp_equal);
		}
	} else {
		s = sql_value_exp(sql, scp,
				  l->h->next->next->data.sym, grp, subset);
	}
	if (!s)
		return NULL;

	if (s && distinct) {
		s = stmt_unique(s,grp);
	}
	if (!s)
		return NULL;
	a = sql_bind_aggr(l->h->data.sval, tail_type(s)->sqlname);
	if (a) {
		return stmt_aggr(s, a, grp);
	} else {
		snprintf(sql->errstr, ERRSIZE,
			 _("Aggregate: %s(%s) unknown"),
			 l->h->data.sval, tail_type(s)->sqlname);
	}
	return NULL;
}

static stmt *sql_column_value(context * sql, scope * scp, symbol * se,
			      group * grp, stmt * subset)
{
	stmt *res = sql_column_ref(sql, scp, se);
	if (res && res->h && subset) {
		stmt *foundsubset = find_subset(subset, res->h);

		if (!foundsubset) {
			snprintf(sql->errstr, ERRSIZE,
				 _
				 ("Subset not found for value expression"));
			return NULL;
		}
		res = stmt_join(foundsubset, res, cmp_equal);
	}
	return res;
}

static stmt *sql_value_exp(context * sql, scope * scp, symbol * se,
			   group * grp, stmt * subset)
{

	switch (se->token) {
	case SQL_TRIOP:
		return sql_triop(sql, scp, se, grp, subset);
	case SQL_BINOP:
		return sql_binop(sql, scp, se, grp, subset);
	case SQL_UNOP:
		return sql_unop(sql, scp, se, grp, subset);
	case SQL_AGGR:
		return sql_aggrop(sql, scp, se, grp, subset);
	case SQL_COLUMN:
		return sql_column_value(sql, scp, se, grp, subset);
	case SQL_SELECT:
		return scope_subquery(sql, scp, se);
	case SQL_PARAMETER:
		printf("parameter not used\n");
		break;
	case SQL_ATOM:
		return stmt_atom(atom_dup(se->data.aval));
	case SQL_CAST:
		return sql_cast(sql, scp, se, grp, subset);
	case SQL_CASE:
		return sql_case(sql, scp, se, grp, subset);
	case SQL_NULLIF:
	case SQL_COALESCE:
		printf("case %d %s\n", se->token, token2string(se->token));
		return NULL;
		break;
	default:
		printf("unknown %d %s\n", se->token,
		       token2string(se->token));
	}
	return NULL;
}

static list *query_and(catalog * cat, list * ands)
{
	list *l = NULL;
	node *n = ands->h;
	int len = list_length(ands), changed = 0;

	while (n && (len > 0)) {
		/* the first in the list changes every interation */
		stmt *s = n->data;
		node *m = n->next;
		l = create_stmt_list();
		while (m) {
			stmt *olds = s;
			stmt *t = m->data;
			if (s->nrcols == 1) {
				tvar *sv = s->h;

				if (t->nrcols == 1 && sv == t->h) {
					s = stmt_semijoin(s, t);
				}
				if (t->nrcols == 2) {
					if (sv == t->h) {
						s = stmt_semijoin(t, s);
					} else if (sv == t->t) {
						stmt *r = stmt_reverse(t);
						s = stmt_semijoin(r, s);
					}
				}
			} else if (s->nrcols == 2) {
				tvar *sv = s->h;
				if (t->nrcols == 1) {
					if (sv == t->h) {
						s = stmt_semijoin(s, t);
					} else if (s->t == t->h) {
						s = stmt_reverse(s);
						s = stmt_semijoin(s, t);
					}
				} else if (t->nrcols == 2) {
					if (sv == t->h && s->t == t->t) {
						s = stmt_intersect(s, t);
					} else if (sv == t->t
						   && s->t == t->h) {
						s = stmt_reverse(s);
						s = stmt_intersect(s, t);
					}
				}
			}
			/* if s changed, t is used so no need to store */
			if (s == olds) {
				list_append(l, t); st_attache(t,NULL);
			} else {
				changed++;
				len--;
			}
			m = m->next;
		}
		if (s){
			list_append(l, s); st_attache(s, NULL);
		}
		n = l->h;
		len--;
	}
	return l;
}

static stmt *set2pivot(context * sql, list * l)
{
	list *pivots = create_stmt_list();
	node *n;
	l = query_and(sql->cat, l);
	n = l->h;
	if (n) {
		int len = 0;
		int markid = 0;
		stmt *st = n->data;
		if (st->nrcols == 1) {
			list_append(pivots, 
				stmt_mark(stmt_reverse(st), markid++));
		}
		if (st->nrcols == 2) {
			list_append(pivots,
				stmt_mark (stmt_reverse(st), markid));
			list_append(pivots, stmt_mark(st, markid++));
		}
		n = list_remove_node(l, n);
		len = list_length(l) + 1;
		while (list_length(l) > 0 && len > 0) {
			len--;
			n = l->h;
			while (n) {
				stmt *st = n->data;
				list *npivots = create_stmt_list();
				node *p = NULL;
				for ( p = pivots->h; p; p = p->next) {
					stmt *tmp = p->data;
					tvar *tv = tmp->t;
					if (tv == st->h) {
						stmt *m = stmt_mark
						    (stmt_reverse(st), markid);
						stmt *m1 = stmt_mark(st,
								     markid++);
						stmt *j = stmt_join(p->data,
							     stmt_reverse(m),
							      cmp_equal);
						stmt *pnl = stmt_mark(j,
								      markid);
						stmt *pnr = stmt_mark
						    (stmt_reverse(j), markid++);
						list_append (npivots,
						     stmt_join(pnl, m1,
							       cmp_equal));

						for (p = pivots->h;p;p=p->next){
							list_append (npivots,
							     stmt_join
							     (pnr, p->data,
							      cmp_equal));
						}
						list_destroy(pivots);
						pivots = npivots;
						n = list_remove_node(l, n);
						break;
					} else if (tv == st->t) {
						stmt *m1 =
						    stmt_mark
						    (stmt_reverse(st),
						     markid);
						stmt *m = stmt_mark(st,
								    markid++);
						stmt *j =
						    stmt_join(p->data,
							      stmt_reverse
							      (m),
							      cmp_equal);
						stmt *pnl = stmt_mark(j,
								      markid);
						stmt *pnr =
						    stmt_mark
						    (stmt_reverse(j),
						     markid++);
						list_append
						    (npivots,
						     stmt_join(pnl,
							       m1,
							       cmp_equal));

						for (p = pivots->h;p;p=p->next){
							list_append(npivots,
							     stmt_join
							     (pnr, p->data,
							      cmp_equal));
						}
						list_destroy(pivots);
						pivots = npivots;
						n = list_remove_node(l, n);
						break;
					}
				}
				if (n)
					n = n->next;
			}
		}
		if (!len) {
			snprintf(sql->errstr, ERRSIZE,
			 _("Semantically incorrect query, unrelated tables"));
			assert(0);
			return NULL;
		}
	}
	list_destroy(l);
	return stmt_list(pivots);
}

/* reason for the group stuff 
 * if a value is selected twice once on the left hand of the
 * or and once on the right hand of the or it will be in the
 * result twice.
 *
 * current version is broken, unique also remove normal doubles.
 */
static stmt *sets2pivot(context * sql, list * ll)
{
	node *n = ll->h;
	if (n) {
		stmt *pivots = set2pivot(sql, n->data);
		list *cur = pivots->op1.lval;
		n = n->next;
		/*
		   stmt *g = NULL;
		 */
		while (n) {
			stmt *npivots = set2pivot(sql, n->data);
			list *l = npivots->op1.lval;
			list *inserts = create_stmt_list();

			node *m = l->h;

			while (m) {
				node *c = cur->h;
				while (c) {
					stmt *cd = c->data;
					stmt *md = m->data;
					if (cd->t == md->t) {
						list_append (inserts,
						     stmt_insert_column
						     (cd, md));
					}
					c = c->next;
				}
				m = m->next;
			}
			/* TODO: cleanup the old cur(rent) and npivots */
			cur = inserts;
			n = n->next;
		}
		/* no double elimination jet 
		   m = inserts->h;
		   while(m){
		   if (g){
		   g = stmt_derive(m->data, g);
		   } else {
		   g = stmt_group(m->data);
		   }
		   m = m->next;
		   }
		   g = stmt_reverse( stmt_unique( 
		   stmt_reverse( g ), NULL));
		   m = inserts->h;
		   cur = create_stmt_list();
		   while(m){
		   list_append( cur, 
		   stmt_semijoin( m->data, g));
		   m = m->next;
		   }
		 */
		return stmt_list(cur);
	}
	return NULL;
}

static stmt *stmt2pivot(context * sql, scope * scp, stmt * s)
{
	if (s->type != st_set && s->type != st_sets) {
		s = stmt_set(s);
	}
	if (s->type == st_sets) {
		stmt *ns = sets2pivot(sql, s->op1.lval);
		stmt_destroy(s);
		s = ns;
	} else {
		stmt *ns = set2pivot(sql, s->op1.lval);
		stmt_destroy(s);
		s = ns;
	}
	return s;
}


static stmt *find_on_column_name(context * sql, scope * scp, tvar * t,
				 char *name)
{
	node *m;

	for (m = t->t->columns->h; m; m = m->next) {
		column *rc = m->data;
		if (strcmp(name, rc->name) == 0) {
			return stmt_column(rc, t);
		}
	}
	return NULL;
}

static list *join_on_column_name(context * sql, scope * scp, tvar * tv,
				 tvar * r, int all)
{
	list *res = create_stmt_list();
	node *n;
	for (n = tv->t->columns->h; n; n = n->next) {
		column *lc = n->data;
		stmt *rs = find_on_column_name(sql, scp, r, lc->name);
		if (rs) {
			list_append(res, stmt_column(lc, tv));
			list_append(res, rs);
		} else if (all) {
			list_destroy(res);
			return NULL;
		}
	}
	return res;
}


static stmt *sql_join_
    (context * sql,
     scope * scp,
     symbol * tab1, int natural, jt jointype, symbol * tab2, symbol * js) {
	stmt *s = NULL, *subset = NULL;
	tvar *tv1, *tv2;

	tv1 = table_ref(sql, scp, tab1);
	tv2 = table_ref(sql, scp, tab2);

	if (!tv1 || !tv2)
		return NULL;

	if (js && natural) {
		snprintf(sql->errstr, ERRSIZE,
			 "Cannot have a NATURAL JOIN with a join specification (ON or USING);\n");
		return NULL;
	}
	if (!js && !natural) {
		snprintf(sql->errstr, ERRSIZE,
			 "Must have NATURAL JOIN or a JOIN with a specification (ON or USING);\n");
		return NULL;
	}

	if (js && js->token != SQL_USING) {	/* On sql_logical_exp */
		s = sql_logical_exp(sql, scp, js, NULL, NULL);
	} else if (js) {	/* using */
		dnode *n = js->data.lval->h;

		for (; n; n = n->next) {
			char *nm = n->data.sval;
			stmt *j;
			stmt *ls = find_on_column_name(sql, scp, tv1, nm);
			stmt *rs = find_on_column_name(sql, scp, tv2, nm);
			if (!ls || !rs) {
				snprintf(sql->errstr, ERRSIZE,
					 "Tables %s and %s do have a matching column %s\n",
					 tv1->tname, tv2->tname, nm);
				if (s)
					stmt_destroy(s);
				return NULL;
			}
			rs = check_types(sql, tail_type(ls),
					 stmt_reverse(rs));
			if (!rs) {
				if (s)
					stmt_destroy(s);
				return NULL;
			}
			j = stmt_join(ls, rs, cmp_equal);
			if (s)
				s = stmt_intersect(s, j);
			else
				s = j;
		}
	} else {		/* ! js -> natural join */
		list *matching_columns;
		node *m;

		matching_columns =
		    join_on_column_name(sql, scp, tv1, tv2, 0);

		if (!matching_columns
		    || list_length(matching_columns) == 0) {
			snprintf(sql->errstr, ERRSIZE,
				 "No attributes of tables %s and %s match\n",
				 tv1->tname, tv2->tname);
			return NULL;
		}

		for (m = matching_columns->h; m; m = m->next->next) {
			stmt *j;
			stmt *ls = m->data;
			stmt *rs = m->next->data;

			rs = check_types(sql, tail_type(ls),
					 stmt_reverse(rs));
			if (!rs) {
				if (s)
					stmt_destroy(s);
				return NULL;
			}
			j = stmt_join(ls, rs, cmp_equal);
			if (s)
				s = stmt_intersect(s, j);
			else
				s = j;
		}
		list_destroy(matching_columns);
	}

	if (s) {
		s = stmt2pivot(sql, scp, s);
		/* possibly lift references */
		/*
		   if (list_length(scp->lifted) > 0){
		   snprintf(sql->errstr, ERRSIZE, 
		   _("Outer join with outer reference not implemented\n"));
		   return NULL;
		   }
		 */
	}
	subset = s;
	if (subset) {
		list *l1, *l2;
		table *t = NULL;
		node *n;
		stmt *fs1 = find_subset(subset, tv1);
		stmt *fs2 = find_subset(subset, tv2);
		stmt *ld = NULL, *rd = NULL;

		if (!fs1 || !fs2) {
			if (!fs1)
				snprintf(sql->errstr, ERRSIZE,
					 _
					 ("Subset %s not found in join expression"),
					 tv1->tname);
			else if (!fs2)
				snprintf(sql->errstr, ERRSIZE,
					 _
					 ("Subset %s not found in join expression"),
					 tv2->tname);
			else
				snprintf(sql->errstr, ERRSIZE,
					 _
					 ("Subsets %s,%s not found in join expression"),
					 tv1->tname, tv2->tname);
			return NULL;
		}

		t = tv1->t;
		if (jointype == jt_left || jointype == jt_full) {
			column *cs = t->columns->h->data;
			/* we need to add the missing oid's */
			ld = stmt_diff(stmt_column(cs, tv1),
				       stmt_reverse(fs1));
			ld = stmt_mark(stmt_reverse(ld), -1);
		}
		t = tv2->t;
		if (jointype == jt_right || jointype == jt_full) {
			column *cs = t->columns->h->data;
			/* we need to add the missing oid's */
			rd = stmt_diff(stmt_column(cs, tv2),
				       stmt_reverse(fs2));
			rd = stmt_mark(stmt_reverse(rd), -1);
		}
		l1 = create_stmt_list();
		t = tv1->t;
		for (n = t->columns->h; n; n = n->next) {
			column *cs = n->data;

			list_append(l1,
					 stmt_join(fs1,
						   stmt_column
						   (cs, tv1), cmp_equal));
		}
		t = tv2->t;
		for (n = t->columns->h; n; n = n->next) {
			column *cs = n->data;

			list_append(l1,
					 stmt_join(fs2,
						   stmt_column
						   (cs, tv2), cmp_equal));
		}
		l2 = create_stmt_list();
		if (jointype == jt_left || jointype == jt_full) {
			node *m = l1->h;
			t = tv1->t;
			for (n = t->columns->h; n;
			     n = n->next, m = m->next) {
				column *cs = n->data;

				list_append(l2,
						 stmt_union(m->data,
							    stmt_join
							    (ld,
							     stmt_column
							     (cs,
							      tv1),
							     cmp_equal)));
			}
			t = tv2->t;
			for (n = t->columns->h; n;
			     n = n->next, m = m->next) {
				column *cs = n->data;

				list_append(l2,
						 stmt_union(m->data,
							    stmt_const
							    (ld,
							     stmt_atom
							     (atom_general
							      (cs->
							       tpe,
							       NULL)))));
			}
			list_destroy(l1);
			l1 = l2;
		}
		if (jointype == jt_right || jointype == jt_full) {
			node *m = l1->h;
			t = tv1->t;
			for (n = t->columns->h; n;
			     n = n->next, m = m->next) {
				column *cs = n->data;

				list_append(l2,
						 stmt_union(m->data,
							    stmt_const
							    (rd,
							     stmt_atom
							     (atom_general
							      (cs->
							       tpe,
							       NULL)))));
			}
			t = tv2->t;
			for (n = t->columns->h; n;
			     n = n->next, m = m->next) {
				column *cs = n->data;

				list_append(l2,
						 stmt_union(m->data,
							    stmt_join
							    (rd,
							     stmt_column
							     (cs,
							      tv2),
							     cmp_equal)));
			}
			list_destroy(l1);
			l1 = l2;
		}
		s = stmt_list(l1);
	}
	return s;
}

static stmt *sql_join(context * sql, scope * scp, symbol * q)
{

	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	int natural = n->next->data.ival;
	jt jointype = (jt) n->next->next->data.ival;
	symbol *tab_ref2 = n->next->next->next->data.sym;
	symbol *joinspec = n->next->next->next->next->data.sym;

	return sql_join_(sql, scp,
			 tab_ref1, natural, jointype, tab_ref2, joinspec);
}

static stmt *sql_cross(context * sql, scope * scp, symbol * q)
{

	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	symbol *tab_ref2 = n->next->data.sym;

	tvar *tv1 = table_ref(sql, scp, tab_ref1);
	tvar *tv2 = table_ref(sql, scp, tab_ref2);
	stmt *ct;

	if (!tv1 || !tv2)
		return NULL;

	ct = stmt_join(stmt_column
		       (tv1->t->columns->h->data, tv1),
		       stmt_reverse(stmt_column
				    (tv2->t->columns->h->data, tv2)), cmp_all);

	if (ct) {
		list *rl = create_stmt_list();
		node *nv;

		ct = stmt2pivot(sql, scp, ct);

		for (nv = scp->tables->h; nv; nv = nv->next) {
			table *t = NULL;
			node *n;
			tvar *tv = nv->data;
			stmt *foundsubset = find_subset(ct, tv);

			t = tv->t;
			for (n = t->columns->h; n; n = n->next) {
				column *cs = n->data;
				list_append(rl,
						 stmt_join
						 (foundsubset,
						  stmt_column(cs,
							      tv),
						  cmp_equal));
			}
		}
		return stmt_list(rl);
	}
	return NULL;
}

static tvar *query_exp_optname(context * sql, scope * scp, symbol * q)
{
	tvar *res = NULL;

	switch (q->token) {
	case SQL_JOIN:
		{
			scope *nscp = scope_open(scp);
			stmt *tq = sql_join(sql, nscp, q);
			if (!tq) {
				printf("empty join result\n");
				return NULL;
			}
			res =
			    table_optname(sql, scp, tq, q->sql,
					  q->data.lval->t->data.sym);
			scp = scope_close(nscp);
			return res;
		}
	case SQL_CROSS:
		{
			scope *nscp = scope_open(scp);
			stmt *tq = sql_cross(sql, nscp, q);
			if (!tq) {
				printf("empty join result\n");
				return NULL;
			}
			res =
			    table_optname(sql, scp, tq, q->sql,
					  q->data.lval->t->data.sym);
			scp = scope_close(nscp);
			return res;
		}
	case SQL_UNION:
		{
			scope *nscp = scope_open(scp);
			node *m;
			dnode *n = q->data.lval->h;
			tvar *lv = table_ref(sql, nscp, n->data.sym);
			int all = n->next->data.ival;
			tvar *rv =
			    table_ref(sql, nscp, n->next->next->data.sym);
			list *unions, *matching_columns;

			if (!lv || !rv)
				return NULL;

			/* find the matching columns (all should match?)
			 * union these 
			 * if !all do a distinct operation at the end 
			 */
			/* join all result columns ie join(lh,rh) on column_name */

			matching_columns =
			    join_on_column_name(sql, nscp, lv, rv, 1);

			if (!matching_columns)
				return NULL;

			unions = create_stmt_list();
			for (m = matching_columns->h; m; m = m->next->next) {
				stmt *l = m->data;
				stmt *r = m->next->data;
				list_append(unions, stmt_union(l, r));
			}
			res =
			    table_optname(sql, scp, stmt_list(unions),
					  q->sql,
					  q->data.lval->t->data.sym);
			list_destroy(matching_columns);
			scp = scope_close(nscp);
			return res;
		}
	default:
		printf("case %d %s\n", q->token, token2string(q->token));
	}
	return NULL;
}

/* column expresion of the form: table.* */
static stmt *columns_exp(context * sql, scope * scp, symbol * column_e,
			 group * grp, stmt * subset)
{
	char *tname = column_e->data.lval->h->data.sval;
	tvar *tv = scope_bind_table(scp, tname);

	if (tv) {
		stmt *foundsubset = find_subset(subset, tv);
		list *columns = create_stmt_list();
		node *n = tv->t->columns->h;
		if (grp)
			foundsubset = stmt_join(grp->ext,
						foundsubset, cmp_equal);
		while (n) {
			list_append(columns,
				stmt_join(foundsubset,
					stmt_column(n->data, tv), cmp_equal));
			n = n->next;
		}
		return stmt_list(columns);
	}
	return NULL;
}


static stmt *column_exp(context * sql, scope * scp, symbol * column_e,
			group * grp, stmt * subset)
{
	dlist *l = column_e->data.lval;
	stmt *s = sql_value_exp(sql, scp, l->h->data.sym,
				grp, subset);
	if (!s)
		return NULL;

	if (grp && s->type != st_aggr) {
		s = stmt_join(grp->ext, s, cmp_equal);
	}

	/* AS name */
	if (s && l->h->next->data.sval) {
		s = stmt_name(s, l->h->next->data.sval);
		scope_add_alias(scp, s, l->h->next->data.sval);
	}
	return s;
}

static stmt *sql_column_exp(context * sql, scope * scp, symbol * column_e,
			    group * grp, stmt * subset)
{
	stmt *res = NULL;
	if (column_e->token == SQL_TABLE) {
		res = columns_exp(sql, scp, column_e, grp, subset);
	} else if (column_e->token == SQL_COLUMN) {
		res = column_exp(sql, scp, column_e, grp, subset);
	}
	if (!res && sql->errstr[0] == '\0') {
		snprintf(sql->errstr, ERRSIZE,
			 _
			 ("Column expression Symbol(%d)->token = %s no output"),
			 (int) column_e->token,
			 token2string(column_e->token));
	}
	return res;
}

list *list_map_merge(list * l2, int seqnr, list * l1)
{
	return list_merge(l1, l2);
}

list *list_map_append_list(list * l2, int seqnr, list * l1)
{
	return list_append(l1, l2);
}

static stmt *sql_compare(context * sql, stmt * ls,
			 stmt * rs, char *compare_op)
{
	int join = 0;
	comp_type type = cmp_equal;

	if (!ls || !rs)
		return NULL;

	if (ls->nrcols <= 0 && rs->nrcols <= 0) {
		snprintf(sql->errstr, ERRSIZE,
			 _
			 ("Compare(%s) between two atoms is not possible"),
			 compare_op);
		return NULL;
	} else if (ls->nrcols > 0 && rs->nrcols > 0) {
		join = 1;
	}
	if (compare_op[0] == '=') {
		type = cmp_equal;
	} else if (compare_op[0] == '<') {
		type = cmp_lt;
		if (compare_op[1] != '\0') {
			if (compare_op[1] == '>') {
				type = cmp_notequal;
			} else if (compare_op[1] == '=') {
				type = cmp_lte;
			}
		}
	} else if (compare_op[0] == '>') {
		type = cmp_gt;
		if (compare_op[1] != '\0') {
			if (compare_op[1] == '=') {
				type = cmp_gte;
			}
		}
	}
	if (join) {
		if (ls->h && rs->h && ls->h == rs->h) {
			/* 
			 * same table, ie. no join 
			 * do a [compare_op].select(true) 
			 */
			sql_func *cmp = sql_bind_func(compare_op,
						  tail_type(ls)->sqlname,
						  tail_type(rs)->sqlname,
						  NULL);

			if (!cmp) {
				snprintf(sql->errstr, ERRSIZE,
					 _
					 ("Binary compare operation %s %s %s missing"),
					 tail_type(ls)->sqlname,
					 compare_op,
					 tail_type(rs)->sqlname);
				return NULL;
			}
			return
			    stmt_select(stmt_binop(ls, rs, cmp),
					stmt_atom(atom_general
						  (sql_bind_type
						   ("BOOL"),
						   _strdup
						   ("1"))), cmp_equal);
		}
		rs = check_types(sql, tail_type(ls), rs);
		if (!rs)
			return NULL;
		rs = stmt_reverse(rs);
		return stmt_join(ls, rs, type);
	} else {
		if (ls->nrcols == 0) {
			stmt *t = ls;
			ls = rs;
			rs = t;
		}
		rs = check_types(sql, tail_type(ls), rs);
		if (!rs)
			return NULL;
		return stmt_select(ls, rs, type);
	}
}

stmt *sql_and(context * sql, stmt * ls, stmt * rs)
{
	if (!ls || !rs)
		return NULL;
	if (ls->type != st_set && ls->type != st_sets) {
		ls = stmt_set(ls);
	}
	if (rs->type != st_set && rs->type != st_sets) {
		rs = stmt_set(rs);
	}
	if (ls->type == st_set && rs->type == st_set) {
		list *nl = NULL;
		list_merge(ls->op1.lval, rs->op1.lval);
		stmt_destroy(rs);
		nl = query_and(sql->cat, ls->op1.lval);
		list_destroy(ls->op1.lval);
		ls->op1.lval = nl;
	} else if (ls->type == st_sets && rs->type == st_set) {
		list_map(ls->op1.lval, (map_func) & list_map_merge,
			 rs->op1.sval);
		stmt_destroy(rs);
	} else if (ls->type == st_set && rs->type == st_sets) {
		list_map(rs->op1.lval, (map_func) & list_map_merge,
			 ls->op1.sval);
		stmt_destroy(ls);
		ls = rs;
	} else if (ls->type == st_sets && rs->type == st_sets) {
		list_map(ls->op1.lval, (map_func) & list_map_merge,
			 rs->op1.sval);
		stmt_destroy(rs);
	}
	return ls;
}

static stmt *sql_logical_exp(context * sql, scope * scp, symbol * sc,
			     group * grp, stmt * subset)
{
	if (!sc)
		return NULL;
	switch (sc->token) {
	case SQL_OR:
		{
			symbol *lo = sc->data.lval->h->data.sym;
			symbol *ro = sc->data.lval->h->next->data.sym;
			stmt *ls =
			    sql_logical_exp(sql, scp, lo, grp, subset);
			stmt *rs =
			    sql_logical_exp(sql, scp, ro, grp, subset);
			if (!ls || !rs)
				return NULL;
			if (ls->type != st_set && ls->type != st_sets) {
				ls = stmt_set(ls);
			}
			if (rs->type != st_set && rs->type != st_sets) {
				rs = stmt_set(rs);
			}
			if (ls->type == st_set && rs->type == st_set) {
				ls = stmt_sets(ls->op1.lval);
				list_append(ls->op1.lval, rs->op1.lval);
			} else if (ls->type == st_sets
				   && rs->type == st_set) {
				list_append(ls->op1.lval, rs->op1.lval);
			} else if (ls->type == st_set
				   && rs->type == st_sets) {
				list_append(rs->op1.lval, ls->op1.lval);
				ls = rs;
			} else if (ls->type == st_sets
				   && rs->type == st_sets) {
				(void) list_map(ls->op1.lval,
						(map_func) &
						list_map_append_list,
						rs->op1.sval);
			}
			return ls;
		}
		break;
	case SQL_AND:
		{
			symbol *lo = sc->data.lval->h->data.sym;
			symbol *ro = sc->data.lval->h->next->data.sym;
			stmt *ls =
			    sql_logical_exp(sql, scp, lo, grp, subset);
			stmt *rs =
			    sql_logical_exp(sql, scp, ro, grp, subset);
			return sql_and(sql, ls, rs);
		}
		break;
	case SQL_COMPARE:
		{
			symbol *lo = sc->data.lval->h->data.sym;
			symbol *ro =
			    sc->data.lval->h->next->next->data.sym;
			char *compare_op =
			    sc->data.lval->h->next->data.sval;
			stmt *rs, *ls =
			    sql_value_exp(sql, scp, lo, grp, subset);

			if (!ls)
				return NULL;
			if (ro->token != SQL_SELECT) {
				rs = sql_value_exp(sql, scp, ro, grp,
						   subset);
				if (!rs)
					return NULL;
				if (grp && !(rs->key || ls->key)) {
					snprintf(sql->errstr, ERRSIZE,
						 _
						 ("Cannot compare sets with values, probably a aggrate function missing"));
					return NULL;
				}
				return sql_compare(sql, ls, rs,
						   compare_op);
			} else {
				node *o;
				rs = scope_subquery(sql, scp, ro);

				if (!rs)
					return NULL;
				if (rs->type != st_list
				    || list_length(rs->op1.lval) == 0) {
					snprintf(sql->errstr, ERRSIZE,
						 _
						 ("Subquery result wrong"));
					return NULL;
				}
				o = rs->op1.lval->h;
				if (list_length(rs->op1.lval) == 1) {
					stmt *j = sql_compare(sql, ls,
							      o->data,
							      compare_op);
					if (!j)
						return NULL;
					return stmt_semijoin(ls, j);
				} else {
					stmt *sd, *j = sql_compare(sql, ls,
								   o->data,
								   compare_op);
					if (!j)
						return NULL;
					sd = stmt_set(
						stmt_join(j, o->next->data, 
							cmp_equal));
					o = o->next;
					o = o->next;
					for (; o; o = o->next) {
						list_append(sd->op1.lval,
							stmt_join(j, o->data,
								  cmp_equal));
					}
					return sd;
				}
				return NULL;
			}
		}
		break;
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
		{
			stmt *res = NULL;
			symbol *lo = sc->data.lval->h->data.sym;
			int symmetric = sc->data.lval->h->next->data.ival;
			symbol *ro1 =
			    sc->data.lval->h->next->next->data.sym;
			symbol *ro2 =
			    sc->data.lval->h->next->next->next->data.sym;
			stmt *ls =
			    sql_value_exp(sql, scp, lo, grp, subset);
			stmt *rs1 =
			    sql_value_exp(sql, scp, ro1, grp, subset);
			stmt *rs2 =
			    sql_value_exp(sql, scp, ro2, grp, subset);
			if (!ls || !rs1 || !rs2)
				return NULL;
			if (rs1->nr > 0 || rs2->nr > 0) {
				snprintf(sql->errstr, ERRSIZE,
					 _
					 ("Between requires an atom on the right handside"));
				return NULL;
			}
			/* add check_type */
			if (symmetric) {
				stmt *tmp = NULL;
				sql_func *min = sql_bind_func("min",
							  tail_type(rs1)->
							  sqlname,
							  tail_type(rs2)->
							  sqlname, NULL);
				sql_func *max = sql_bind_func("max",
							  tail_type(rs1)->
							  sqlname,
							  tail_type(rs2)->
							  sqlname, NULL);
				if (!min || !max) {
					snprintf(sql->errstr, ERRSIZE,
						 _
						 ("min or max operator on types %s %s missing"),
						 tail_type(rs1)->sqlname,
						 tail_type(rs2)->sqlname);
					return NULL;
				}
				tmp = stmt_binop(rs1, rs2, min);
				rs2 = stmt_binop(rs1, rs2, max);
				rs1 = tmp;
			}
			res = stmt_select2(ls, rs1, rs2, cmp_equal);
			if (sc->token == SQL_NOT_BETWEEN)
				res = stmt_diff(ls, res);
			return res;
		}
	case SQL_LIKE:
	case SQL_NOT_LIKE:
		{
			stmt *res = NULL;
			symbol *lo = sc->data.lval->h->data.sym;
			symbol *ro = sc->data.lval->h->next->data.sym;
			stmt *ls =
			    sql_value_exp(sql, scp, lo, grp, subset);
			atom *a = NULL, *e = NULL;
			if (!ls)
				return NULL;
			if (ro->token == SQL_ATOM) {
				a = ro->data.aval;
			} else {
				a = ro->data.lval->h->data.aval;
				e = ro->data.lval->h->next->data.aval;
			}
			if (e) {
				snprintf(sql->errstr, ERRSIZE,
					 _
					 ("Time to implement LIKE escapes"));
				return NULL;
			}
			if (a->type != string_value) {
				snprintf(sql->errstr, ERRSIZE,
					 _
					 ("Wrong type used with LIKE stmt, should be string %s %s"),
					 atom2string(a),
					 atom_type(a)->sqlname);
				return NULL;
			}
			res = stmt_like(ls, stmt_atom(atom_dup(a)));
			if (sc->token == SQL_NOT_LIKE)
				res = stmt_diff(ls, res);
			return res;
		}
	case SQL_IN:
		{
			dlist *l = sc->data.lval;
			symbol *lo = l->h->data.sym;
			stmt *ls =
			    sql_value_exp(sql, scp, lo, grp, subset);
			if (!ls)
				return NULL;
			if (l->h->next->type == type_list) {
				dnode *n = l->h->next->data.lval->h;
				list *nl = create_atom_list();
				while (n) {
					list_append(nl, 
					    atom_dup(n->data.sym->data.aval));
					n = n->next;
				}
				return stmt_exists(ls, nl);
			} else if (l->h->next->type == type_symbol) {
				symbol *ro = l->h->next->data.sym;
				stmt *sq = scope_subquery(sql, scp, ro);

				if (!sq)
					return NULL;
				if (sq->type != st_list
				    || list_length(sq->op1.lval) == 0) {
					snprintf(sql->errstr, ERRSIZE,
						 _
						 ("Subquery result wrong"));
					return NULL;
				}
				if (list_length(sq->op1.lval) == 1) {
					stmt *rs = sq->op1.lval->h->data;
					return
					    stmt_reverse
					    (stmt_semijoin
					     (stmt_reverse(ls),
					      stmt_reverse(rs)));
				} else {	/* >= 2 */
					node *o = sq->op1.lval->h;
					stmt *j = stmt_join(ls,
						stmt_reverse (o->data),
							cmp_equal);
					stmt *sd = stmt_set(
						stmt_join(j, o->next->data,
							cmp_equal));
					o = o->next;
					o = o->next;
					for (; o; o = o->next) {
						list_append(sd->op1.lval,
							stmt_join (j, o->data,
								cmp_equal));
					}
					return sd;
				}
				return NULL;
			} else {
				snprintf(sql->errstr, ERRSIZE,
					 _("In missing inner query"));
				return NULL;
			}
		}
		break;
	case SQL_NOT_IN:
		{
			dlist *l = sc->data.lval;
			symbol *lo = l->h->data.sym;
			stmt *ls =
			    sql_value_exp(sql, scp, lo, grp, subset);
			if (!ls)
				return NULL;
			if (l->h->next->type == type_list) {
				dnode *n = l->h->next->data.lval->h;
				list *nl = create_atom_list();
				while (n) {
					list_append(nl, atom_dup(n->data.aval));
					n = n->next;
				}
				return stmt_diff(ls, stmt_exists(ls, nl));
			} else if (l->h->next->type == type_symbol) {
				stmt *sr = stmt_reverse(ls);
				tvar *sqn = sql_subquery_optname(sql, scp,
								 l->h->
								 next->
								 data.sym);
				if (sqn) {
					column *c = sqn->t->columns->h->data;
					return stmt_reverse(stmt_diff(sr,
					      	stmt_reverse(c->s)));
				}
				return NULL;
			} else {
				snprintf(sql->errstr, ERRSIZE,
					 _("In missing inner query"));
				return NULL;
			}
		}
		break;
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
		{
			symbol *lo = sc->data.sym;
			scope *nscp = scope_open(scp);
			stmt *ls = sql_subquery(sql, nscp, lo);

			scp = scope_close(nscp);

			if (!ls)
				return NULL;
			if (ls->type != st_list) {
				snprintf(sql->errstr, ERRSIZE,
					 _("Subquery result wrong"));
				return NULL;
			}
			if (list_length(ls->op1.lval) == 1) {
				/* NOT still broken */
				return stmt_reverse(ls->op1.lval->h->data);
			} else if (sc->token == SQL_EXISTS) {
				node *o = ls->op1.lval->h->next;	/* skip first */
				stmt *sd, *j = stmt_reverse(o->data);

				o = o->next;
				if (!o)
					return j;

				sd = stmt_set(stmt_join(j, o->data, cmp_equal));
				o = o->next;
				for (; o; o = o->next) {
					list_append(sd->op1.lval,
						stmt_join (j, o->data,
							cmp_equal));
				}
				return sd;
			} else {	/* not exists */
				sql_type *t = sql_bind_type("INTEGER");
				stmt *a = stmt_atom(atom_int(t, 0));
				node *o = ls->op1.lval->h->next;	/* skip first */
				stmt *sd, *j, *jr;

				j = stmt_const(
					stmt_diff(head_column(o->data),
						stmt_reverse(o->data)), a);

				o = o->next;
				if (!o)
					return j;

				jr = stmt_const(
					stmt_diff(head_column(o->data),
						stmt_reverse(o->data)), a);
				sd = stmt_set(stmt_join (j, stmt_reverse(jr),
					cmp_equal));
				o = o->next;
				for (; o; o = o->next) {
					jr = stmt_const(
						stmt_diff(head_column(o->data),
						stmt_reverse(o->data)), a);

					list_append(sd->op1.lval,
						stmt_join (j, stmt_reverse(jr),
							  cmp_equal));
				}
				return sd;
			}
		}
		break;
	case SQL_NULL:
	case SQL_NOT_NULL:
		{
			symbol *cr = sc->data.sym;
			stmt *res =
			    sql_value_exp(sql, scp, cr, grp, subset);

			if (res) {
				sql_type *tpe = tail_type(res);
				stmt *a =
				    stmt_atom(atom_general(tpe, NULL));

				if (sc->token == SQL_NULL) {
					res =
					    stmt_select(res, a, cmp_equal);
				} else {
					res =
					    stmt_select(res, a,
							cmp_notequal);
				}
			}
			return res;
		}
		break;
	default:
		snprintf(sql->errstr, ERRSIZE,
			 _("Predicate %s %d: time to implement some more"),
			 token2string(sc->token), sc->token);
		return NULL;
	}
	snprintf(sql->errstr, ERRSIZE,
		 _("Predicate: time to implement some more"));
	return NULL;
}
static stmt *having_condition(context * sql, scope * scp, symbol * sc,
			      group * grp, stmt * subset)
{
	if (!sc)
		return NULL;
	switch (sc->token) {
	case SQL_OR:
		{
			symbol *lo = sc->data.lval->h->data.sym;
			symbol *ro = sc->data.lval->h->next->data.sym;
			stmt *ls =
			    having_condition(sql, scp, lo, grp, subset);
			stmt *rs =
			    having_condition(sql, scp, ro, grp, subset);
			if (!ls || !rs)
				return NULL;

			return stmt_union(ls, rs);
		}
		break;
	case SQL_AND:
		{
			symbol *lo = sc->data.lval->h->data.sym;
			symbol *ro = sc->data.lval->h->next->data.sym;
			stmt *ls =
			    having_condition(sql, scp, lo, grp, subset);
			stmt *rs =
			    having_condition(sql, scp, ro, grp, subset);
			if (!ls || !rs)
				return NULL;

			return stmt_semijoin(ls, rs);
		}
		break;
	case SQL_COMPARE:
		{
			stmt *s =
			    sql_logical_exp(sql, scp, sc, grp, subset);
			return s;
		}
		break;
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
		{
			stmt *s =
			    sql_logical_exp(sql, scp, sc, grp, subset);
			return s;
		}
	case SQL_LIKE:
	case SQL_NOT_LIKE:
		{
			stmt *s =
			    sql_logical_exp(sql, scp, sc, grp, subset);
			return s;
		}
	case SQL_IN:
		{
			stmt *s =
			    sql_logical_exp(sql, scp, sc, grp, subset);
			return s;
		}
		break;
	case SQL_NOT_IN:
		{
			stmt *s =
			    sql_logical_exp(sql, scp, sc, grp, subset);
			return s;
		}
		break;
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
		{
			stmt *s =
			    sql_logical_exp(sql, scp, sc, grp, subset);
			return s;
		}
		break;
	default:
		snprintf(sql->errstr, ERRSIZE,
			 _("Predicate %s %d: time to implement some more"),
			 token2string(sc->token), sc->token);
		return NULL;
	}
	snprintf(sql->errstr, ERRSIZE,
		 _("Predicate: time to implement some more"));
	return NULL;
}


group *query_groupby_inner(context * sql, scope * scp, stmt * c, stmt * st,
			   group * cur)
{
	node *n = st->op1.lval->h;
	while (n) {
		stmt *s = n->data;
		if (s->t == c->h) {
			stmt *j = stmt_join(s, c, cmp_equal);
			cur = grp_create(j, cur);
			break;
		}
		n = n->next;
	}
	if (!cur) {
		assert(cur);
		snprintf(sql->errstr, ERRSIZE,
			 "subset not found for groupby column %s\n",
			 column_name(c));
	}
	return cur;
}

static group *query_groupby(context * sql, scope * scp, symbol * groupby,
			    stmt * st)
{
	group *cur = NULL;
	dnode *o = groupby->data.lval->h;
	while (o) {
		symbol *grp = o->data.sym;
		stmt *c = sql_column_ref(sql, scp, grp);
		cur = query_groupby_inner(sql, scp, c, st, cur);
		o = o->next;
	}
	return cur;
}

static group *query_groupby_lifted(context * sql, scope * scp, stmt * st)
{
	group *cur = NULL;
	node *o = scp->lifted->h;
	while (o) {
		cvar *cv = o->data;
		stmt *sc = cv->s;
		cur = query_groupby_inner(sql, scp, sc, st, cur);
		o = o->next;
	}
	return cur;
}

static stmt *query_orderby(context * sql, scope * scp,
			   symbol * orderby, stmt * st,
			   stmt * subset, group * grp)
{
	stmt *cur = NULL;
	dnode *o = orderby->data.lval->h;
	while (o) {
		symbol *order = o->data.sym;
		if (order->token == SQL_COLUMN) {
			symbol *col = order->data.lval->h->data.sym;
			int direction =
			    order->data.lval->h->next->data.ival;
			stmt *sc = sql_column_ref(sql, scp, col);
			if (sc) {
				stmt *j = NULL;
				if (sc->type == st_column) {
					j = find_subset(subset, sc->h);
					if (!j) {
						snprintf(sql->errstr,
							 ERRSIZE,
							 "subset not found for orderby column\n");
						return NULL;
					}
					if (grp)
						j = stmt_join(grp->ext,
							      j,
							      cmp_equal);
					j = stmt_join(j, sc, cmp_equal);
				} else {
					j = sc;
				}
				if (cur)
					cur =
					    stmt_reorder(cur, j,
							 direction);
				else
					cur = stmt_order(j, direction);
			} else {
				return NULL;
			}
		} else {
			snprintf(sql->errstr, ERRSIZE,
				 "order not of type SQL_COLUMN\n");
			return NULL;
		}
		o = o->next;
	}
	return cur;
}

static stmt *sql_simple_select(context * sql, scope * scp,
			       dlist * selection)
{
	int toplevel = (!scp) ? 1 : 0;
	stmt *s = NULL;
	list *rl = create_stmt_list();

	scp = scope_open(scp);
	if (toplevel) {		/* only on top level query */
		node *n = sql->cat->cur_schema->tables->h;

		for (; (n); n = n->next) {
			table *p = n->data;
			scope_add_table_columns(scp, p, p->name);
		}
	}

	if (selection) {
		dnode *n = selection->h;
		while (n) {
			stmt *cs =
			    sql_column_exp(sql, scp, n->data.sym, NULL,
					   NULL);

			if (!cs)
				return NULL;

			/* t1.* */
			if (cs->type == st_list
			    && n->data.sym->token == SQL_TABLE)
				list_merge(rl, cs->op1.lval);
			else if (cs->type == st_list) {	/* subquery */
				printf("subquery in simple_select\n");
			} else
				list_append(rl, cs);
			n = n->next;
		}
	}
	s = stmt_list(rl);

	if (scp)
		scp = scope_close(scp);

	if (!s && sql->errstr[0] == '\0')
		snprintf(sql->errstr, ERRSIZE,
			 _("Subquery result missing"));
	return s;
}


static stmt *sql_error(context * sql)
{
	if (sql->errstr[0] == '\0')
		snprintf(sql->errstr, ERRSIZE,
			 _("Subquery result missing"));
	return NULL;
}

static
stmt *sql_select
    (context * sql,
     scope * scp,
     int distinct,
     dlist * selection, dlist * into, dlist * table_exp, symbol * orderby)
{
	list *rl;
	int toplevel = (!scp) ? 1 : 0;
	stmt *s = NULL;

	symbol *from = table_exp->h->data.sym;

	symbol *where = table_exp->h->next->data.sym;
	symbol *groupby = table_exp->h->next->next->data.sym;
	symbol *having = table_exp->h->next->next->next->data.sym;
	stmt *order = NULL, *subset = NULL;
	group *grp = NULL;

	if (!from && !where)
		return sql_simple_select(sql, scp, selection);

	if (toplevel)
		scp = scope_open(scp);

	if (from) {		/* keep variable list with tables and names */
		dlist *fl = from->data.lval;
		dnode *n = NULL;
		tvar *fnd = (tvar *) 1;

		for (n = fl->h; (n && fnd); n = n->next)
			fnd = table_ref(sql, scp, n->data.sym);

		if (!fnd)
			return NULL;

	} else if (toplevel) {	/* only on top level query */
		node *n = sql->cat->cur_schema->tables->h;

		for (; (n); n = n->next) {
			table *p = n->data;
			scope_add_table_columns(scp, p, p->name);
		}
	}

	if (where) {
		node *n;
		stmt *cur = NULL;

		s = sql_logical_exp(sql, scp, where, NULL, NULL);
		if (s->type != st_set && s->type != st_sets) {
			s = stmt_set(s);
		}
		/* check for tables not used in the where part 
		 */
		for (n = scp->tables->h; n; n = n->next) {
			tvar *v = n->data;
			stmt *tmp = complex_find_subset(s, v);
			if (!tmp) {
				cvar *cv = v->columns->h->data;
				tmp = cv -> s;
				/* just add a select whole column */
				if (!cur) {
					cur = tmp;
				/* add join to an allready used column */
				} else {	
					tmp = stmt_join(cur,
						stmt_reverse(tmp), cmp_all);
				}
				s = sql_and(sql, s, tmp);
			} else if (!cur) {
				cur = tmp;
			}
		}
	} else if (from) {
		node *n;
		stmt *cur = NULL;
		for (n = scp->tables->h; n; n = n->next) {
			tvar *v = n->data;
			stmt *tmp = NULL;
			cvar *cv = v->columns->h->data;
			tmp = cv -> s;
			if (!cur) {
				cur = tmp;
			} else {
				tmp =
				    stmt_join(cur,
					      stmt_reverse(tmp), cmp_all);
				if (s) {
					list_append(s->op1.lval, tmp);
				} else {
					s = stmt_set(tmp);
				}
			}
		}
		if (!cur) {
			snprintf(sql->errstr, ERRSIZE,
				 _("Subquery has no columns"));
			return NULL;
		}
		if (!s)
			s = cur;
	}

	if (s) {
		s = stmt2pivot(sql, scp, s);
		if (s && groupby) {
			grp = query_groupby(sql, scp, groupby, s);
			if (!grp) {
				if (subset)
					stmt_destroy(subset);
				return NULL;
			}
		}

		if (s && list_length(scp->lifted) > 0) {
			grp = query_groupby_lifted(sql, scp, s);
			if (!grp) {
				if (subset)
					stmt_destroy(subset);
				return NULL;
			}
		}
	}

	subset = s;
	if (having) {
		s = having_condition(sql, scp, having, grp, subset);

		if (!s)
			return sql_error(sql);

		if (grp) {
			group *ng = grp_semijoin(grp, s);
			grp_destroy(grp);
			grp = ng;
		} else {
			node *n = NULL;
			list *sl = create_stmt_list();
			for (n = subset->op1.lval->h; n; n = n->next) {
				list_append(sl, stmt_semijoin(n->data, s));
			}
			subset = stmt_list(sl);
		}
	}

	if (!subset)
		if (!subset)
			return sql_error(sql);

	rl = create_stmt_list();
	if (selection) {
		dnode *n = selection->h;

		while (n) {
			stmt *cs = sql_column_exp(sql, scp, n->data.sym,
						  grp, subset);
			if (!cs)
				return sql_error(sql);

			/* t1.* */
			if (cs->type == st_list
			    && n->data.sym->token == SQL_TABLE)
				list_merge(rl, cs->op1.lval);
			else if (cs->type == st_list) {	/* subquery */
				if (list_length(cs->op1.lval) == 1) {	/* single value */
					stmt *ss = subset->op1.lval->h->data;
					list_append(rl,
					stmt_join (ss, 
						cs->op1.lval->h->data, 
						cmp_all));
				} else {	/* referenced variable(s) (can only be 2) */
					stmt *ids = cs->op1.lval->h->next->data;
					stmt *ss = find_subset(subset, ids->t);
					list_append(rl, 
						stmt_outerjoin(
						stmt_outerjoin(ss, 
						stmt_reverse (ids), cmp_equal),
							cs->op1.lval->h->data,
							cmp_equal));
				}
			} else
				list_append(rl, cs);
			n = n->next;
		}
	} else {
		/* select * from tables */
		if (toplevel) {
			node *nv;
			for (nv = scp->tables->h; nv; nv = nv->next) {
				table *t = NULL;
				node *n;
				tvar *tv = nv->data;
				stmt *foundsubset =
				    find_subset(subset, tv);

				t = tv->t;
				for (n = t->columns->h; n; n = n->next) {
					column *cs = n->data;
					list_append(rl,
						stmt_join(foundsubset,
						stmt_column(cs, tv),cmp_equal));
				}
			}
		} else {
			/* 
			   * subquery * can only return one column, better
			   * the oids are needed 
			 */
			tvar *tv = scope_first_table(scp);
			stmt *foundsubset = find_subset(subset, tv);

			if (!foundsubset)
				return sql_error(sql);

			list_append(rl, foundsubset);
		}
	}
	/* the inner query should output a table where the first bat
	 * contains the queried column values. 
	 * If variables from the outer query are correlated 
	 * the oids from the base tables of these variables are returned
	 * in the next columns.
	 */
	if (list_length(scp->lifted) > 0) {
		list *vars = scope_unique_lifted_vars(scp);
		node *o = vars->h;
		while (o) {
			tvar *v = o->data;
			stmt *foundsubset = find_subset(subset, v);

			if (!foundsubset)
				return sql_error(sql);
			list_append(rl,
					 stmt_join(grp->ext, foundsubset,
						   cmp_equal));
			o = o->next;
		}
	}
	s = stmt_list(rl);

	if (s && subset && orderby) {
		order = query_orderby(sql, scp, orderby, s, subset, grp);
		if (!order) {
			if (subset)
				stmt_destroy(subset);
			return sql_error(sql);
		}
	}

	if (toplevel && scp)
		scp = scope_close(scp);

	if (!s)
		return sql_error(sql);

	if (subset)
		stmt_destroy(subset);

	if (grp)
		grp_destroy(grp);

	if (s && order)
		return stmt_ordered(order, s);
	return s;
}


static stmt *create_view(context * sql, schema * schema, stmt * ss,
			 dlist * qname, dlist * column_spec,
			 symbol * query, int check)
{

	catalog *cat = sql->cat;
	char *name = table_name(qname);

	if (cat_bind_table(cat, schema, name)) {
		snprintf(sql->errstr, ERRSIZE,
			 _("Create View name %s allready in use"), name);
		return NULL;
	} else {
		stmt *stct = NULL;
		stmt *sq = sql_subquery(sql, NULL, query);

		if (!sq)
			return NULL;

		if (column_spec) {
			table *table =
			    cat_create_table(cat, 0, schema, name, 0,
					     query->sql);
			dnode *n = column_spec->h;
			node *m = sq->op1.lval->h;

			stct = stmt_create_table(ss, table);

			while (n) {
				char *cname = n->data.sval;
				stmt *st = m->data;
				char *ctype = tail_type(st)->sqlname;
				column *col = cat_create_column(cat, 0,
								table,
								cname,
								ctype,
								"NULL", 1);
				col->s = st;
				st_attache(st, NULL);
				n = n->next;
				m = m->next;
			}
		} else {
			table *table =
			    create_table_intern(sql, schema, name,
						query->sql, sq);
			stct = stmt_create_table(ss, table);
		}
		stmt_destroy(sq);
		return stct;
	}
	return NULL;
}

static stmt *column_constraint_type(context * sql, symbol * s, stmt * ss,
			       stmt * ts, stmt * cs, column *c)
{
	stmt *res = NULL;

	switch (s->token) {
	case SQL_UNIQUE: 
	{
		key *k = cat_table_add_key(c->table, ukey, NULL);
		cat_key_add_column(k, c);
		res = stmt_key_add_column(stmt_key(ts, ukey, NULL), cs);
	} break;
	case SQL_PRIMARY_KEY: 
	{
		key *k = cat_table_add_key(c->table, pkey, NULL);
		cat_key_add_column(k, c);
		res = stmt_key_add_column(stmt_key(ts, pkey, NULL), cs);
	} break;
	case SQL_FOREIGN_KEY:
	{
		dnode *n = s->data.lval->h;
		char *tname = table_name(n->data.lval);
		char *cname = n->data.lval->h->data.sval;
		table *ft = cat_bind_table(sql->cat, c->table->schema, tname);
		column *fc = cat_bind_column(sql->cat, ft, cname);

		if (!ft) {
			snprintf(sql->errstr, ERRSIZE,
				_("table %s unknown\n"), tname);
		} else if (!fc) {
			snprintf(sql->errstr, ERRSIZE,
				_("table %s has no column %s\n"), tname, cname);
		} else {
			key *fk = cat_table_add_key(ft, rkey, NULL);
			key *k = cat_table_add_key(c->table, fkey, fk);
			stmt *fts = stmt_bind_table(ss, ft);
			stmt *fcs = stmt_bind_column(fts, fc);
			cat_key_add_column(k, c);
			cat_key_add_column(fk, fc);
			res = stmt_key_add_column(
					stmt_key(fts, rkey, NULL), fcs);
			res = stmt_key_add_column(
					stmt_key(ts, fkey, res), cs);
		}
	} break;
	case SQL_NOT_NULL:
		c->null = 0;
		res = stmt_not_null(cs);
		break;
	}

	if (!res) {
		snprintf(sql->errstr, ERRSIZE,
			 _("Unknown constraint (%ld)->token = %s\n"),
			 (long) s, token2string(s->token));
	}
	return res;
}

/* 
column_option: default | column_constraint ;
*/

static stmt *column_option(context * sql, symbol * s, stmt * ss, 
			   stmt * ts, stmt * cs, column *c)
{
	stmt *res = NULL;
	assert(cs);
	switch (s->token) {
	case SQL_CONSTRAINT:
		{
			dlist *l = s->data.lval;
			/*char *opt_name = l->h->data.sval;  not used jet */
			symbol *sym = l->h->next->data.sym;
			res = column_constraint_type(sql, sym, ss, ts, cs, c);
		}
		break;
	case SQL_ATOM:
		res = stmt_default(cs, stmt_atom(atom_dup(s->data.aval)));
		break;
	}
	if (!res && sql->errstr[0] == '\0') {
		snprintf(sql->errstr, ERRSIZE,
			 _("Unknown column option (%ld)->token = %s\n"),
			 (long) s, token2string(s->token));
	}
	return NULL;
}

static stmt *column_options(context * sql, dlist * opt_list, 
		stmt *ss, stmt * ts, stmt * cs, column *c)
{
	assert(cs);
	if (opt_list) {
		dnode *n = NULL;
		for (n = opt_list->h; n && cs; n = n->next) {
			cs = column_option(sql, n->data.sym, ss, ts, cs, c);
		}
	}
	return cs;
}

static stmt *create_column(context * sql, symbol * s, stmt * ss, stmt * ts, table * table )
{
	catalog *cat = sql->cat;
	dlist *l = s->data.lval;
	char *cname = l->h->data.sval;
	sql_type *ctype = sql_bind_type(l->h->next->data.sval);
	dlist *opt_list = l->h->next->next->data.lval;
	stmt *res = NULL;
	if (cname && ctype) {
		column *c = cat_create_column(cat, 0, table,
					      cname,
					      ctype->sqlname,
					      "NULL", 1);
		res = stmt_create_column(ts, c);
		c->s = res;
		st_attache(res, NULL);
		res = column_options(sql, opt_list, ss, ts, res, c);
	} 

	if (!res && sql->errstr[0] == '\0') {
		snprintf(sql->errstr, ERRSIZE,
			 _("Create Column: type or name"));
	}
	return res;
}

static stmt *table_foreign_key( context * sql, symbol * s, stmt * ss, stmt * ts, table * t )
{
	catalog *cat = sql->cat;
	stmt *res = NULL;
	dnode *n = s->data.lval->h;
	char *tname = table_name(n->data.lval);
	table *ft = cat_bind_table( cat, t->schema, tname );

	if (!ft){
		snprintf(sql->errstr, ERRSIZE, _("Table %s unknown\n"), tname );
		return NULL;
	} else {
		key *fk = cat_table_add_key(ft, fkey, NULL);
		key *k = cat_table_add_key(t, fkey, fk);
		stmt *fts = stmt_bind_table(ss, ft);
		stmt *ks = stmt_key(fts, rkey, NULL);
		stmt *rks = stmt_key(ts, fkey, ks);
		dnode *nms = n->next->data.lval->h;
		dnode *fnms = n->next->data.lval->h;
		if (n->next->next)
			fnms = n->next->next->data.lval->h;

		for(;nms && fnms; nms = nms->next, fnms = fnms->next){
			char *nm = nms->data.sval;
			char *fnm = fnms->data.sval;
			column *c = cat_bind_column(cat, t, nm );
			column *fc = cat_bind_column(cat, ft, fnm );

			if (!c){
				snprintf(sql->errstr, ERRSIZE,
				_("Table %s has no column %s\n"), t->name, nm);
				if (res) stmt_destroy(res);
				return NULL;
			}
			if (!fc){
				snprintf(sql->errstr, ERRSIZE,
				_("Table %s has no column %s\n"), ft->name,fnm);
				if (res) stmt_destroy(res);
				return NULL;
			}
			cat_key_add_column(fk, fc );
			cat_key_add_column(k, c );
			res = stmt_key_add_column(ks, stmt_bind_column(fts,fc));
			res = stmt_key_add_column(rks, stmt_bind_column(ts,c));
		}
		if (nms || fnms){
			snprintf(sql->errstr, ERRSIZE,
			_("Not all colunms are handeled in the foreign key\n"));
			if (res) stmt_destroy(res);
			res = NULL;
		}
	}
	return res;
}

static stmt *table_constraint_type( context * sql, symbol * s, 
		stmt * ss, stmt *ts, table * t )
{
	catalog *cat = sql->cat;
	stmt *res = NULL;
	switch(s->token){
		case SQL_UNIQUE: 
		case SQL_PRIMARY_KEY:
		{
			key_type kt = (s->token==SQL_PRIMARY_KEY?pkey:ukey);
			key *k = cat_table_add_key(t, kt, NULL);
			stmt *ks = stmt_key(ts, kt, NULL);
			dnode *nms = s->data.lval->h;

			for(;nms; nms = nms->next){
				char *nm = nms->data.sval;
				column *c = cat_bind_column(cat, t, nm );
				stmt *cs = stmt_bind_column(ts, c);

				if (!c){
					snprintf(sql->errstr, ERRSIZE,
					_("Table %s has no column %s\n"), 
					t->name, nm);
					return NULL;
				}
				cat_key_add_column(k, c);
				res = stmt_key_add_column(ks, cs);
			}
		} break;
		case SQL_FOREIGN_KEY:
			res = table_foreign_key( sql, s, ss, ts, t );
		 	break;
	}
	if (!res){
		snprintf(sql->errstr, ERRSIZE,
			 _("Table Constraint Type: wrong token (%ld) = %s\n"),
			 (long) s, token2string(s->token));
	}
	return res;
}

static stmt *table_constraint( context * sql, symbol * s, 
		stmt *ss, stmt * ts, table * table) 
{
	stmt *res = NULL;

	if (s->token == SQL_CONSTRAINT){
		dlist *l = s->data.lval;
		/*char *opt_name = l->h->data.sval;  not used jet */
		symbol *sym = l->h->next->data.sym;
		res = table_constraint_type(sql, sym, ss, ts, table );
	}

	if (!res && sql->errstr[0] == '\0') {
		snprintf(sql->errstr, ERRSIZE,
			 _("Table Constraint: wrong token (%ld) = %s\n"),
			 (long) s, token2string(s->token));
	}
	return res;
}

static stmt *table_element(context * sql, symbol * s, 
			stmt *ss, stmt * ts, table *table)
{
	stmt *res = NULL;

	switch (s->token) {
	case SQL_COLUMN:
		res = create_column(sql, s, ss, ts, table);
		break;
	case SQL_CONSTRAINT:
		res = table_constraint(sql, s, ss, ts, table);
		break;
	case SQL_COLUMN_OPTIONS:
		{
			dnode *n = s->data.lval->h;
			char *cname = n->data.sval;
			dlist *olist = n->next->data.lval;
			column *c = cat_bind_column( sql->cat, table, cname);
			stmt *cs = stmt_bind_column( ts, c );

			assert(cs);
			if (!c){
				snprintf(sql->errstr, ERRSIZE,
			 	_("Column %s not found\n"), cname);
			} else {
				res = column_options(sql, olist, ss, ts, cs, c);
			}
		} break;
	}
	if (!res && sql->errstr[0] == '\0') {
		snprintf(sql->errstr, ERRSIZE,
			 _("Unknown table element (%ld)->token = %s\n"),
			 (long) s, token2string(s->token));
	}
	return res;
}

static stmt *create_table(context * sql, schema * schema, stmt * ss,
			int temp, dlist * qname, dlist * columns)
{
	catalog *cat = sql->cat;
	char *name = table_name(qname);

	if (cat_bind_table(cat, schema, name)) {
		snprintf(sql->errstr, ERRSIZE,
			 _("Create Table name %s allready in use"), name);
		return NULL;
	} else {
		table *table = cat_create_table(cat, 0, schema, name, temp,
						NULL);
		stmt *ts = stmt_create_table(ss, table);
		list *newcolumns = list_append(create_stmt_list(), ts);
		dnode *n = columns->h;

		while (n) {
			list_append(newcolumns,
					 table_element(sql, n->data.sym,
						       ss, ts, table));
			n = n->next;
		}
		return stmt_list(newcolumns);
	}
}

static stmt *drop_table(context * sql, dlist * qname, int drop_action)
{
	stmt *res = NULL;
	char *tname = table_name(qname);
	table *t = cat_bind_table(sql->cat, sql->cat->cur_schema, tname);

	if (!t) {
		snprintf(sql->errstr, ERRSIZE,
			 _("Drop Table, table %s unknown"), tname);
	} else {
		stmt *ss = stmt_bind_schema( sql->cat->cur_schema );
		res = stmt_drop_table(ss, t->name, drop_action);
		cat_drop_table(sql->cat, sql->cat->cur_schema, tname);
	}
	return res;
}

static stmt *alter_table(context * sql, schema *schema, stmt *ss,
		dlist * qname, symbol * te)
{
	catalog *cat = sql->cat;
	char *name = table_name(qname);
	table *table = NULL;

	if ((table =
	     cat_bind_table(cat, sql->cat->cur_schema, name)) == NULL) {
		snprintf(sql->errstr, ERRSIZE,
			 _("Alter Table name %s doesn't exist"), name);
		return NULL;
	} else {
		stmt *ts = stmt_bind_table(ss, table);
		stmt *c = create_column(sql, te, ss, ts, table );
		return c;
	}
}

static stmt *create_schema(context * sql, dlist * auth_name,
			   dlist * schema_elements)
{
	catalog *cat = sql->cat;
	char *name = schema_name(auth_name);
	char *auth = schema_auth(auth_name);

	if (auth == NULL) {
		auth = sql->cat->cur_schema->auth;
	}
	if (cat_bind_schema(cat, name)) {
		snprintf(sql->errstr, ERRSIZE,
			 _("Create Schema name %s allready in use"), name);
		return NULL;
	} else {
		schema *schema = cat_create_schema(cat, 0, name, auth);
		stmt *st = stmt_create_schema(schema);
		list *schema_objects = list_append(create_stmt_list(), st);

		dnode *n = schema_elements->h;

		while (n) {
			st = NULL;
			if (n->data.sym->token == SQL_CREATE_TABLE) {
				dlist *l = n->data.sym->data.lval;
				st = create_table(sql, schema, st,
						  l->h->data.ival,
						  l->h->next->data.lval,
						  l->h->next->next->data.
						  lval);
			} else if (n->data.sym->token == SQL_CREATE_VIEW) {
				dlist *l = n->data.sym->data.lval;
				st = create_view(sql, schema, st,
						 l->h->data.lval,
						 l->h->next->data.lval,
						 l->h->next->next->data.
						 sym,
						 l->h->next->next->next->
						 data.ival);
			}
			list_append(schema_objects, st);
			n = n->next;
		}
		return stmt_list(schema_objects);
	}
}


static stmt *copyfrom(context * sql, dlist * qname, char *file, char *sep)
{
	catalog *cat = sql->cat;
	char *tname = table_name(qname);
	table *t = cat_bind_table(cat, sql->cat->cur_schema, tname);

	if (!t) {
		snprintf(sql->errstr, ERRSIZE,
			 _("Copy into non existing table %s"), tname);
		return NULL;
	}
	return stmt_copyfrom(t, file, sep);

}

static stmt *insert_value(context * sql, scope * scp, column * c,
			  symbol * s)
{
	if (s->token != SQL_NULL) {
		stmt *n = NULL;
		stmt *a = sql_value_exp(sql, scp, s, NULL, NULL);
		if (!(n = check_types(sql, c->tpe, a)))
			return NULL;
		return n;
	} else if (s->token == SQL_NULL) {
		return stmt_atom(atom_general(c->tpe, NULL));
	}
	return NULL;
}

static stmt *insert_into(context * sql, dlist * qname,
			 dlist * columns, symbol * val_or_q)
{

	catalog *cat = sql->cat;
	char *tname = table_name(qname);
	table *t = cat_bind_table(cat, sql->cat->cur_schema, tname);
	list *l, *collist = NULL;
	int i, len = 0;
	stmt **inserts;

	if (!t) {
		snprintf(sql->errstr, ERRSIZE,
			 _("Inserting into non existing table %s"), tname);
		return NULL;
	}
	if (columns) {
		/* XXX: what to do for the columns which are not listed */
		dnode *n = columns->h;
		collist = create_column_list();
		while (n) {
			column *c = cat_bind_column(cat, t, n->data.sval);
			if (c) {
				list_append(collist, c);
			} else {
				snprintf(sql->errstr, ERRSIZE,
					 _
					 ("Inserting into non existing column %s.%s"),
					 tname, n->data.sval);
				return NULL;
			}
			n = n->next;
		}
	} else {
		collist = t->columns;
	}

	len = list_length(collist);

	l = create_stmt_list();
	inserts = NEW_ARRAY(stmt *, len);
	for (i = 0; i < len; i++)
		inserts[i] = NULL;

	if (val_or_q->token == SQL_VALUES) {
		dlist *values = val_or_q->data.lval;
		if (dlist_length(values) != list_length(collist)) {
			snprintf(sql->errstr, ERRSIZE,
				 _
				 ("Inserting into table %s, number of values doesn't match number of columns"),
				 tname);
			return NULL;
		} else {
			dnode *n;
			node *m;

			for (n = values->h, m = collist->h;
			     n && m; n = n->next, m = m->next) {
				column *c = m->data;
				inserts[c->colnr] = insert_value(sql, NULL, 
							c, n->data.sym);
			}

		}
	} else {
		stmt *s = sql_subquery(sql, NULL, val_or_q);

		if (!s)
			return NULL;
		if (list_length(s->op1.lval) != list_length(collist)) {
			snprintf(sql->errstr, ERRSIZE,
				 _
				 ("Inserting into table %s, query result doesn't match number of columns"),
				 tname);
			return NULL;
		} else {
			node *m, *n;

			for (n = s->op1.lval->h, m = collist->h;
			     n && m; n = n->next, m = m->next) {
				column *c = m->data;
				inserts[c->colnr] = n->data;
			}
		}
	}
	for (i = 0; i < len; i++) {
		if (!inserts[i])
			return NULL;
		list_append(l, inserts[i]);
	}
	return stmt_insert(t, l);
}

static stmt *sql_update(context * sql, dlist * qname,
			dlist * assignmentlist, symbol * opt_where)
{
	stmt *s = NULL;
	char *tname = table_name(qname);
	table *t = cat_bind_table(sql->cat, sql->cat->cur_schema, tname);

	if (!t) {
		snprintf(sql->errstr, ERRSIZE,
			 _("Updating non existing table %s"), tname);
	} else {
		dnode *n;
		list *l = create_stmt_list();
		scope *scp;

		scp = scope_open(NULL);
		scope_add_table_columns(scp, t, t->name);

		if (opt_where){
			s = sql_logical_exp(sql, scp, opt_where, NULL, NULL);
			if (s) s = stmt2pivot(sql,scp,s);
			if (!s) return NULL;
		}

		n = assignmentlist->h;
		/* change to a list of (column, values-bats) */
		while (n) {
			symbol *a;
			stmt *v;
			dlist *assignment = n->data.sym->data.lval;
			char *cname = assignment->h->data.sval;
			cvar *cl = scope_bind_column(scp, NULL, cname);
			if (!cl) {
				snprintf(sql->errstr, ERRSIZE,
					 _
					 ("Updating non existing column %s.%s"),
					 tname, assignment->h->data.sval);
				list_destroy(l);
				return NULL;
			}
			a = assignment->h->next->data.sym;
			v = sql_value_exp(sql, scp, a, NULL, s);

			if (!v)
				return NULL;

			v = check_types(sql, cl->c->tpe, v);

			if (v->nrcols <= 0)
				v = stmt_const(s ? s : cl->s, v);

			list_append(l, stmt_update(cl->c, v));
			n = n->next;
		}
		scp = scope_close(scp);
		return stmt_list(l);
	}
	return NULL;
}

static stmt *delete_searched(context * sql, dlist * qname,
			     symbol * opt_where)
{
	char *tname = table_name(qname);
	table *t = cat_bind_table(sql->cat, sql->cat->cur_schema, tname);

	if (!t) {
		snprintf(sql->errstr, ERRSIZE,
			 _("Deleting from non existing table %s"), tname);
	} else {
		stmt *s = NULL;
		scope *scp;

		scp = scope_open(NULL);
		scope_add_table_columns(scp, t, t->name);

		if (opt_where)
			s = sql_logical_exp(sql, scp, opt_where, NULL,
					    NULL);

		s = stmt_delete(t, s);

		scp = scope_close(scp);
		return s;
	}
	return NULL;
}

static stmt *sql_stmt(context * sql, symbol * s)
{
	schema *cur = sql->cat->cur_schema;
	stmt *ret = NULL;
	switch (s->token) {

	case SQL_RELEASE:{
			ret = stmt_release(s->data.sval);
		}
		break;
	case SQL_COMMIT:{
			ret = stmt_commit(s->data.ival, NULL);
		}
		break;
	case SQL_SAVEPOINT:{
			ret = stmt_commit(0, s->data.sval);
		}
		break;
	case SQL_ROLLBACK:{
			dlist *l = s->data.lval;
			ret = stmt_rollback(l->h->data.ival,
					    l->h->next->data.sval);
		}
		break;

	case SQL_CREATE_SCHEMA:
		{
			dlist *l = s->data.lval;
			ret = create_schema(sql, l->h->data.lval,
					    l->h->next->next->next->data.
					    lval);
		}
		break;
	case SQL_DROP_TABLE:
		{
			dlist *l = s->data.lval;
			ret =
			    drop_table(sql, l->h->data.lval,
				       l->h->next->data.ival);
		}
		break;
	case SQL_DROP_VIEW:
		{
			dlist *l = s->data.lval;
			ret = drop_table(sql, l, 0);
		}
		break;
	case SQL_CREATE_TABLE:
		{
			
			dlist *l = s->data.lval;
			ret = create_table(sql, cur, stmt_bind_schema(cur),
					   l->h->data.ival,
					   l->h->next->data.lval,
					   l->h->next->next->data.lval);
		}
		break;
	case SQL_CREATE_VIEW:
		{
			dlist *l = s->data.lval;
			ret = create_view(sql, cur, stmt_bind_schema(cur),
					  l->h->data.lval,
					  l->h->next->data.lval,
					  l->h->next->next->data.sym,
					  l->h->next->next->next->data.
					  ival);
		}
		break;
	case SQL_ALTER_TABLE:
		{
			dlist *l = s->data.lval;
			ret = alter_table(sql, cur, stmt_bind_schema(cur),
					l->h->data.lval,	/* table name */
					l->h->next->data.sym);	/* table element */
		}
		break;
	case SQL_COPYFROM:
		{
			dlist *l = s->data.lval;
			ret = copyfrom(sql, l->h->data.lval,
				       l->h->next->data.sval,
				       l->h->next->next->data.sval);
		}
		break;
	case SQL_INSERT_INTO:
		{
			dlist *l = s->data.lval;
			ret = insert_into(sql,
					  l->h->data.lval,
					  l->h->next->data.lval,
					  l->h->next->next->data.sym);
		}
		break;
	case SQL_UPDATE_SET:
		{
			dlist *l = s->data.lval;
			ret = sql_update(sql,
					 l->h->data.lval,
					 l->h->next->data.lval,
					 l->h->next->next->data.sym);
		}
		break;
	case SQL_DELETE:
		{
			dlist *l = s->data.lval;
			ret =
			    delete_searched(sql, l->h->data.lval,
					    l->h->next->data.sym);
		}
		break;
	case SQL_SELECT:
		ret = sql_subquery(sql, NULL, s);
		/* add output stmt */
		if (ret)
			ret = stmt_output(ret);
		break;
	case SQL_JOIN:
		ret = sql_join(sql, NULL, s);
		/* add output stmt */
		if (ret)
			ret = stmt_output(ret);
		break;
	case SQL_CROSS:
		ret = sql_cross(sql, NULL, s);
		/* add output stmt */
		if (ret)
			ret = stmt_output(ret);
		break;
	default:
		snprintf(sql->errstr, ERRSIZE,
			 _("sql_stmt Symbol(%ld)->token = %s"),
			 (long) s, token2string(s->token));
	}
	return ret;
}


stmt *semantic(context * s, symbol * sym)
{
	stmt *res = NULL;

	if (sym && (res = sql_stmt(s, sym)) == NULL) {
		fprintf(stderr, "Semantic error: %s\n", s->errstr);
		fprintf(stderr, "in %s line %d: %s\n",
			sym->filename, sym->lineno, sym->sql);
	}
	return res;
}

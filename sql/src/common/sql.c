
#include <unistd.h>
#include "sql.h"
#include "symbol.h"
#include "statement.h"
#include <mem.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include <scope.h>

#define create_column_list() list_create((fdestroy)NULL)
#define create_string_list() list_create((fdestroy)&GDKfree)

/* 
 * For debugging purposes we need to be able to convert sql-tokens to 
 * a string representation.
 *
 *
 * SQL ERROR <sqlerrno> : <details>
 * SQL DEBUG  <details>
 * SQL WARNING <details>
 * SQL  <informative message, reserved for ...rows affected>
 *
 * Todo add insert bats used for inserts (ie should change the 
 * 	query code to use the global bats (mvc_bat) in read only mode).
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
	case SQL_CREATE_ROLE:
		return "Create ROLE";
	case SQL_DROP_SCHEMA:
		return "Drop Schema";
	case SQL_DROP_TABLE:
		return "Drop Table";
	case SQL_DROP_VIEW:
		return "Drop View";
	case SQL_DROP_ROLE:
		return "Drop ROLE";
	case SQL_ALTER_TABLE:
		return "Alter Table";
	case SQL_GRANT_ROLES:
		return "Grant ROLE";
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
	case SQL_UPDATE:
		return "Update";
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
	case SQL_OP:
		return "Op";
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
	case SQL_REVOKE:
		return "Revoke";
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


static stmt *sql_select(context * sql, scope * scp, SelectNode *sn, int toplevel );
static stmt *sql_simple_select(context * sql, scope * scp, dlist * selection);
static stmt *sql_logical_exp(context * sql, scope * scp, symbol * sc, group * grp, stmt * subset);
static stmt *sql_value_exp(context * sql, scope * scp, symbol * se, group * grp, stmt * subset);
static stmt *sql_compare_exp( context *sql, scope *scp, group *grp, stmt *subset, symbol *lo, symbol *ro, char *compare_op );

static tvar *query_exp_optname(context * sql, scope * scp, symbol * q);
static tvar *sql_subquery_optname(context * sql, scope * scp, symbol * query);

stmt *sql_error( context * sql, int error_code, char *format, ... )
{
	va_list	ap; 

	va_start (ap,format); 
	if (sql->errstr[0] == '\0') {
		sql->status = error_code;
		vsnprintf(sql->errstr, ERRSIZE, _(format), ap); 
	}
	va_end (ap); 
	return NULL;
}

static schema *cur_schema( context *sql )
{
	return sql->cat->cur_schema;
}

static void sql_select_cleanup( context * sql, stmt *s, stmt *subset, group *grp )
{
	if (grp)
		grp_destroy(grp);
	if (subset)
		stmt_destroy(subset);
	if (s)
		stmt_destroy(s);
}

static schema *qname_schema(context *sql, dlist * qname)
{
	schema *s = cur_schema(sql);
	assert(qname && qname->h);

	if (dlist_length(qname) == 2) {
		char *name = qname->h->data.sval;
		s = cat_bind_schema(sql->cat, name);
	}
	return s;
}

static char *qname_table(dlist * qname)
{
	assert(qname && qname->h);

	if (dlist_length(qname) == 1) {
		return qname->h->data.sval;
	} else if (dlist_length(qname) == 2) {
		return qname->h->next->data.sval;
	}
	return "Unknown";
}

static tvar *scope_add_table_columns(scope * scp, table * t, char *tname)
{
	if (t->type == tt_view){
		tvar *tv = scope_add_table(scp, stmt_dup(t->s), tname);
		node *n = t->columns->h;
		for (; n; n = n->next) {
			column *c = n->data;
			stmt *sc = stmt_dup(c->s);
			table_add_column(tv, sc, tv->tname, c->name);
		}
		return tv;
	} else {
		tvar *tv = scope_add_table(scp, stmt_basetable(t), tname);
		node *n = t->columns->h;
		for (; n; n = n->next) {
			column *c = n->data;
			stmt *sc = stmt_cbat(c, stmt_dup(tv->s), RDONLY, st_bat);
			table_add_column(tv, sc, tv->tname, c->name);
		}
		return tv;
	}
}

static tvar *table_ref(context * sql, scope * scp, symbol * tableref)
{
	char *tname = NULL;
	table *t = NULL;

	/* todo handle opt_table_ref 
	   (ie tableref->data.lval->h->next->data.sym */

	if (tableref->token == SQL_NAME) {
		schema *s = qname_schema(sql, tableref->data.lval->h->data.lval);
		tname = qname_table(tableref->data.lval->h->data.lval);
				
		t = cat_bind_table(sql->cat, s, tname);
		if (!t) {
			(void) sql_error( sql, 02, "Unknown table %s", tname);
			return NULL;
		}
		if (tableref->data.lval->h->next->data.sym) {	/* AS */
			tname = tableref->data.lval->h->next->data.sym->data.  lval->h->data.sval;
		}
		return scope_add_table_columns(scp, t, tname);
	} else if (tableref->token == SQL_SELECT) {
		return sql_subquery_optname(sql, scp, tableref);
	} else {
		return query_exp_optname(sql, scp, tableref);
	}
}

static stmt *sql_subquery(context * sql, scope * scp, symbol * sq)
{
	int toplevel = (!scp) ? 1 : 0;
	stmt *res = NULL;
	SelectNode *sn = (SelectNode*)sq;
	assert(sn->s.token == SQL_SELECT);

	if (!toplevel && sn->limit >= 0){
		(void) sql_error( sql, 01, "Can only limit outer select " );
		return NULL;
	}


	if (sn->from) {		/* keep variable list with tables and names */
		dlist *fl = sn->from->data.lval;
		dnode *n = NULL;
		tvar *fnd = (tvar *) 1;

		if (toplevel)
			scp = scope_open(scp);

		for (n = fl->h; (n && fnd); n = n->next)
			fnd = table_ref(sql, scp, n->data.sym);

		if (!fnd){
			if (toplevel && scp)
				scp = scope_close(scp);
			return NULL;
		}

	} else if (toplevel) {	/* only on top level query */
		return sql_simple_select(sql, scp, sn->selection);
	}

	res = sql_select(sql, scp, sn, toplevel );

	if (toplevel && scp)
		scp = scope_close(scp);

	return res;
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
			return sql_error( sql, 02, "Column: %s unknown", name);
		}
	} else if (dlist_length(l) == 2) {
		char *tname = l->h->data.sval;
		char *cname = l->h->next->data.sval;

		if (!(cs = scope_bind(scp, tname, cname))) {
			return sql_error( sql, 02, "Column: %s.%s unknown", tname, cname);
		}
	} else if (dlist_length(l) >= 3) {
		return sql_error( sql, 02, "TODO: column names of level >= 3" );
	}
	return cs;
}

static table *create_table_intern(context * sql, schema * schema,
				  char *name, char *query, stmt * sq, int type)
{
	node *m;
	catalog *cat = sql->cat;
	table *table = cat_create_table(cat, 0, schema, name, type, query);

       	for ( m = sq->op1.lval->h; m; m = m->next ) {
		stmt *st = m->data;
		char *cname = column_name(st);
		column *col = cat_create_column(cat, 0, table, cname, sql_dup_subtype(tail_type(st)), "NULL", 1);
		col->s = stmt_dup(st);
	}
	return table;
}

static tvar *table_optname(context * sql, scope * scp, stmt * sq,
			   char *query, symbol * optname)
{
	node *m;
	char *tname = NULL;
	dlist *columnrefs = NULL;
	tvar *tv;
	table *tab;
	schema *schema = cur_schema(sql);

	if (optname && optname->token == SQL_NAME) {
		tname = optname->data.lval->h->data.sval;
		columnrefs = optname->data.lval->h->next->data.lval;
	}
	tab = create_table_intern(sql, schema, tname, query, sq, tt_temp);
	tv = scope_add_table(scp, sq, tname);
	if (columnrefs) {
		dnode *d;

		for (m = sq->op1.lval->h, d = columnrefs->h; 
		     d && m; 
		     d = d->next, m = m->next) {
			stmt *st = m->data;
			stmt *sc = stmt_ibat(stmt_dup(st), stmt_dup(sq));

			table_add_column(tv, sc, tname, d->data.sval);
		}
	} else if (tname) {
		/* foreach column add column name */
		for ( m = sq->op1.lval->h; m; m = m->next) {
			stmt *st = m->data;
			char *cname = column_name(st);
			stmt *sc = stmt_ibat(stmt_dup(st), stmt_dup(sq));

			table_add_column(tv, sc, tname, cname);
		}
	} else {
		/* foreach column add full basetable,column name */
		for ( m = sq->op1.lval->h; m; m = m->next) {
			stmt *st = m->data;
			char *cname = column_name(st);
			char *tname = table_name(st);
			stmt *sc = stmt_ibat(stmt_dup(st), stmt_dup(sq));

			table_add_column(tv, sc, tname, cname);
		}
	}
	return tv;
}

static tvar *sql_subquery_optname(context * sql, scope * scp,
				  symbol * query)
{
	SelectNode *sn = (SelectNode*)query;
	scope *nscp = scope_open(scp);
	tvar *res = NULL;
	stmt *sq = sql_subquery(sql, nscp, query);

	if (!sq)
		return NULL;

	res = table_optname(sql, scp, sq, sn->s.sql, sn->name);
	scp = scope_close(nscp);
	return res;
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

static stmt *find_pivot(stmt * subset, stmt * t)
{
	assert(subset->type == st_ptable);

	if (t){
		node *n;
		for (n = subset->op1.lval->h; n; n = n->next) {
			stmt *s = n->data;
			if (s->h == t) 
				return stmt_pivot(stmt_dup(s), stmt_dup(subset));
		}
	}
	return NULL;
}

static stmt *first_subset(stmt * subset)
{
	node *n;

	assert(subset->type == st_ptable);

       	n = subset->op1.lval->h;
	if (n) {
		stmt *s = n->data;
		return stmt_pivot(stmt_dup(s), stmt_dup(subset));
	}
	return NULL;
}


/* before the pivot table is created we need to check on both head and
 * tail for the subset.
 * */
static stmt *complex_find_subset(stmt * subset, stmt * t)
{

	if (t) {
		node *n;
		for (n = subset->op1.lval->h; n; n = n->next) {
			stmt *s = n->data;
			if (s->t == t) 
				return stmt_dup(s);
		}
		for (n = subset->op1.lval->h; n; n = n->next) {
			stmt *s = n->data;
			if (s->h == t) 
				return stmt_dup(s);
		}
	}
	return NULL;
}


static stmt *check_types(context * sql, sql_subtype * ct, stmt * s)
{
	sql_subtype *st = tail_type(s);

	if (st) {
		sql_subtype *t = st;

		/* check if the types are the same */
		if (t && subtype_cmp(t, ct) != 0) {
			t = NULL;
		}
		if (!t) {	/* try to convert if needed */
			sql_func *c = sql_bind_func_result("convert",st,NULL,NULL,ct);
			if (c)
				return stmt_unop(s, c);
		}
		if (!t || subtype_cmp(t, ct) != 0) {
			return sql_error(sql, 02, "Types %s(%d,%d) (%s) and %s(%d,%d) (%s) are not equal", st->type->sqlname, st->digits, st->scale, st->type->name, ct->type->sqlname, ct->digits, ct->scale, ct->type->name);
		}
	} else {
		return sql_error(sql, 02, "Statement has no type information");
	}
	return s;
}


/* The case/when construction in the selection works on the resulting
   table (ie. on the marked columns). We just need to know which oid list
   is involved (ie. find one subset).
   We need to check if for all results the types are the same. 
 */
static stmt *sql_case(context * sql, scope * scp, symbol *opt_cond, dlist * when_search_list, symbol * opt_else, group * grp, stmt * subset)
{
	list *conds = create_stmt_list();
	list *results = create_stmt_list();
	dnode *dn = when_search_list->h;
	sql_subtype *restype = NULL;
	stmt *res = NULL;
	node *n, *m;


	if (dn) {
		dlist *when = dn->data.sym->data.lval;
		stmt *cond, *result;

		if (opt_cond){
			cond = sql_compare_exp( sql, scp, grp, subset, 
					opt_cond, when->h->data.sym, "=");
		} else {
			cond = sql_logical_exp(sql, scp, 
					when->h->data.sym, grp, subset);
		}
		result = sql_value_exp(sql, scp, 
					when->h->next->data.sym, grp, subset);
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
		list_destroy(conds);
		list_destroy(results);
		return sql_error(sql, 02, "Error: result type missing");
	}
	for (dn = dn->next; dn; dn = dn->next) {
		sql_subtype *tpe = NULL;
		dlist *when = dn->data.sym->data.lval;
		stmt *cond, *result;

		if (opt_cond){
			cond = sql_compare_exp( sql, scp, grp, subset, 
					opt_cond, when->h->data.sym, "=");
		} else {
			cond = sql_logical_exp(sql, scp, 
					when->h->data.sym, grp, subset);
		}
		result = sql_value_exp(sql, scp, 
					when->h->next->data.sym, grp, subset);
		if (!cond || !result) {
			list_destroy(conds);
			list_destroy(results);
			return NULL;
		}
		list_prepend(conds, cond);
		list_prepend(results, result);

		tpe = tail_type(result);
		if (!tpe) {
			list_destroy(conds);
			list_destroy(results);
			return sql_error(sql, 02, "Error: result type missing");
		}
		if (subtype_cmp(restype, tpe) != 0) {
			list_destroy(conds);
			list_destroy(results);
			sql_subtype_destroy(tpe);
			return sql_error(sql, 02, "Error: result types %s,%s of case are not compatible", restype->type->sqlname, tpe->type->sqlname);
		}
		sql_subtype_destroy(tpe);
	}
	if (subset) {
		res = first_subset(subset);
		if (!res) {
			list_destroy(conds);
			list_destroy(results);
			return sql_error(sql, 02, "Subset not found for case stmt");
		}
	} else {
		printf("Case in query not handled jet\n");
	}
	if (opt_else) {
		stmt *result = sql_value_exp(sql, scp, opt_else, grp, subset);
		sql_subtype *t;

		if (!result) {
			list_destroy(conds);
			list_destroy(results);
			return NULL;
		}
		t = tail_type(result);
		if (subtype_cmp(restype, t) != 0) {
			sql_func *c = sql_bind_func_result("convert", t, NULL, NULL, restype);
			if (!c) {
				return sql_error(sql, 02, "Cast between (%s,%s) not possible", t->type->sqlname, restype->type->sqlname);
			}
			result = stmt_unop(result, c);
		}
		if (result->nrcols <= 0) {
			res = stmt_const(res, result);
		} else {
			if (res) stmt_destroy(res);
			res = result;
		}
	} else {
		res = stmt_const(res, NULL);
	}
	for (n = conds->h, m = results->h; n && m;
	     n = n->next, m = m->next) {
		stmt *cond = stmt_dup(n->data);
		stmt *result = stmt_dup(m->data);

		/* need more semantic tests here */
		if (cond->type == st_sets) {
			node *k = cond->op1.lval->h;
			stmt *cur = NULL;

			if (k) {
				list *l1 = k->data;
				cur = stmt_dup(l1->h->data);
				k = k->next;
				for (; k; k = k->next) {
					list *l2 = k->data;
					stmt *st = stmt_dup(l2->h->data);
					cur = stmt_union(cur, st);
				}
				if (cond) stmt_destroy(cond);
				cond = cur;
			}
		}
		if (cond->type == st_set) {
			stmt *nc = stmt_dup(cond->op1.lval->h->data);
			stmt_destroy(cond);
			cond = nc;
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

static stmt *sql_case_exp(context * sql, scope * scp, symbol * se, group * grp, stmt * subset)
{
	dlist *l = se->data.lval;
	if (l->h->type == type_list) {
		dlist *when_search_list = l->h->data.lval;
		symbol *opt_else = l->h->next->data.sym;
		return sql_case(sql, scp, NULL,  when_search_list, opt_else, grp, subset);
	} else {
		symbol *scalar_exp = l->h->data.sym;
		dlist *when_value_list = l->h->next->data.lval;
		symbol *opt_else = l->h->next->next->data.sym;
		return sql_case(sql, scp, scalar_exp, when_value_list, opt_else, grp, subset);
	}
}

static stmt *sql_cast(context * sql, scope * scp, symbol * se, group * grp, stmt * subset)
{

	dlist *dl = se->data.lval;
	symbol *s = dl->h->data.sym;
	sql_subtype *tpe = dl->h->next->data.typeval;

	stmt *l = sql_value_exp(sql, scp, s, grp, subset);

	if (l) {
		sql_subtype *st = tail_type(l);
		sql_func *c = sql_bind_func_result("convert", st, NULL, NULL, tpe);

		if (!c) {
			stmt_destroy(l);
			return sql_error(sql, 02, "CAST operator: cast(%s,%s) unknown", st->type->sqlname, tpe->type->sqlname);
		}
		return stmt_unop(l, c);
	}
	return NULL;
}

static stmt *sql_triop(context * sql, scope * scp, symbol * se, group * grp, stmt * subset)
{
	dnode *l = se->data.lval->h;
	stmt *ls = sql_value_exp(sql, scp, l->next->data.sym, grp, subset);
	stmt *rs1 = sql_value_exp(sql, scp, l->next->next->data.sym, grp, subset);
	stmt *rs2 = sql_value_exp(sql, scp, l->next->next->next->data.sym, grp, subset);
	sql_func *f = NULL;
	if (!ls || !rs1 || !rs2){
		if (ls) stmt_destroy(ls);
		if (rs1) stmt_destroy(rs1);
		if (rs2) stmt_destroy(rs2);
		return NULL;
	}
	f = sql_bind_func(l->data.sval, tail_type(ls), tail_type(rs1), tail_type(rs2));
	if (f) {
		return stmt_triop(ls, rs1, rs2, f);
	} else {
		stmt_destroy(ls);
		stmt_destroy(rs1);
		stmt_destroy(rs2);
		return sql_error(sql, 02, "operator: %s(%s,%s,%s) unknown", l->data.sval, tail_type(ls)->type->sqlname, tail_type(rs1)->type->sqlname, tail_type(rs2)->type->sqlname);
	}
	return NULL;
}

static stmt *sql_binop(context * sql, scope * scp, symbol * se, group * grp, stmt * subset)
{
	dnode *l = se->data.lval->h;
	stmt *ls = sql_value_exp(sql, scp, l->next->data.sym, grp, subset);
	stmt *rs = sql_value_exp(sql, scp, l->next->next->data.sym, grp, subset);
	sql_func *f = NULL;
	if (!ls || !rs){
		if (ls) stmt_destroy(ls);
		if (rs) stmt_destroy(rs);
		return NULL;
	}

	/* TODO fix memory leak */
	/* also need to add semantic checks here */
	if (ls->type == st_list){
		ls = ls->op1.lval->h->data;
	}
	if (rs->type == st_list){
		rs = rs->op1.lval->h->data;
	}

	f = sql_bind_func(l->data.sval, tail_type(ls), tail_type(rs), NULL);
	if (f) {
		return stmt_binop(ls, rs, f);
	} else {
		sql_func *c = NULL;
		int w = 0;
		sql_subtype *t1 = tail_type(ls);
		sql_subtype *t2 = tail_type(rs);
		if (t1->type->nr > t2->type->nr) {
			sql_subtype *s = t1;
			t1 = t2;
			t2 = s;
			w = 1;
		}
		c = sql_bind_func_result("convert", t1, NULL, NULL, t2);
		f = sql_bind_func(l->data.sval, t2, t2, NULL);

		if (f && c) {
			if (!w) {
				ls = stmt_unop(ls, c);
			} else {
				rs = stmt_unop(rs, c);
			}
			return stmt_binop(ls, rs, f);
		} else {
			stmt_destroy(ls);
			stmt_destroy(rs);
			return sql_error(sql, 02, "Binary operator: %s(%s,%s) unknown", l->data.sval, tail_type(ls)->type->sqlname, tail_type(rs)->type->sqlname);
		}
	}
	return NULL;
}

static stmt *sql_op(context * sql, scope * scp, symbol * se, group * grp, stmt * subset)
{
	dnode *l = se->data.lval->h;
	sql_func *f = NULL;

	f = sql_bind_func(l->data.sval, NULL, NULL, NULL);
	if (f) {
		return stmt_op(f);
	} else {
		return sql_error(sql, 02, "operator: %s() unknown", l->data.sval);
	}
	return NULL;
}

static stmt *sql_unop(context * sql, scope * scp, symbol * se, group * grp, stmt * subset)
{
	dnode *l = se->data.lval->h;
	sql_func *f = NULL;
	stmt *rs = sql_value_exp(sql, scp, l->next->data.sym, grp, subset);
	if (!rs)
		return NULL;
	f = sql_bind_func(l->data.sval, tail_type(rs), NULL, NULL);
	if (f) {
		return stmt_unop(rs, f);
	} else {
		char *type = tail_type(rs)->type->sqlname;
		stmt_destroy(rs);
		return sql_error(sql, 02, "Unary operator: %s(%s) unknown", l->data.sval, type);
	}
	return NULL;
}


static stmt *sql_aggrop(context * sql, scope * scp, symbol * se, group * grp, stmt * subset)
{
	dlist *l = se->data.lval;
	sql_aggr *a = NULL;
	int distinct = l->h->next->data.ival;
	stmt *s = NULL;
	if (!l->h->next->next->data.sym) {	/* count(*) case */
		cvar *cv;
		if (strcmp(l->h->data.sval, "count") != 0) {
			return sql_error(sql, 02, "Aggregate: Cannot do a %s(*)", l->h->data.sval);
		}
		if (grp) {
			a = sql_bind_aggr(l->h->data.sval, NULL);
			return stmt_aggr(stmt_dup(grp->grp), grp_dup(grp), a);
		}
		cv = scope_first_column(scp);
		if (cv && subset) {
			stmt *foundsubset = find_pivot(subset, cv->s->h);

				assert(foundsubset);
			if (!foundsubset) {
				return sql_error(sql, 02, "Aggregate: Cannot find subset for column %s\n", cv->cname) ;
			}
			s = stmt_join(foundsubset, stmt_dup(cv->s), cmp_equal);
		}
	} else {
		/* the values which are aggregated together, no grp should
		 * be given there to optain the values */
		s = sql_value_exp(sql, scp, l->h->next->next->data.sym, /*grp*/ NULL, subset);
	}

	if (s && distinct) {
		s = stmt_unique(s,grp_dup(grp));
	}
	if (!s)
		return NULL;
	a = sql_bind_aggr(l->h->data.sval, tail_type(s));
	if (a) {
		return stmt_aggr(s, grp_dup(grp), a);
	} else {
		char *type = tail_type(s)->type->sqlname;
		stmt_destroy(s);
		return sql_error(sql, 02, "Aggregate: %s(%s) unknown", l->h->data.sval, type);
	}
	return NULL;
}

static stmt *sql_column_value(context * sql, scope * scp, symbol * se, group * grp, stmt * subset)
{
	stmt *res = sql_column_ref(sql, scp, se);
	if (res && res->h && subset) {
		stmt *foundsubset = find_pivot(subset, res->h);

		if (!foundsubset) {
			return sql_error(sql, 02, "Subset not found for value expression");
		}
		res = stmt_join(foundsubset, res, cmp_equal);
	}
	return res;
}

static stmt *sql_value_exp(context * sql, scope * scp, symbol * se, group * grp, stmt * subset)
{

	switch (se->token) {
	case SQL_TRIOP:
		return sql_triop(sql, scp, se, grp, subset);
	case SQL_BINOP:
		return sql_binop(sql, scp, se, grp, subset);
	case SQL_OP:
		return sql_op(sql, scp, se, grp, subset);
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
	case SQL_ATOM: {
		AtomNode *an = (AtomNode*)se;
		return stmt_atom(atom_dup(an->a));
	}
	case SQL_CAST:
		return sql_cast(sql, scp, se, grp, subset);
	case SQL_CASE:
		return sql_case_exp(sql, scp, se, grp, subset);
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

/* turns a stmt set (or a set of stmt_sets) into a pivot table 
 * ie a table with a oid column for each base table.
 * A row of this pivot table expresses how the rows of the base tables
 * relate.
 *
 * TODO current implementation really converts the stmt set here.
 * As this translation involves optimization questions this should be
 * moved to the optimizer. 
 * This change can be implemented using a new stmt pivot table 
 * (stmt_pivot_table(stmt_list (of basetables), stmt_set)) . Through out
 * sql.c the test for st_list should be checked and changed to st_ptable.
 *
 * The optimizer should just go through the stmt tree and change all
 * pivot statements using the current expansion code.
 *
 * simple transition steps
 * 	1) stmt_pivot_table intoduction (but as (stmt_pivot_table(basetableslist, expanded list) (chould be handled by the statement_exec to only expand the second argument of the pivot table.
 * 	2) start optimizer.
 *
 */
static int cmp_head( stmt *sel, stmt *key )
{
	if (key->h && ((key->h == sel->h) || (key->h == sel->t))){
		return 0;
	}
	return -1;
}

static int cmp_tail( stmt *sel, stmt *key )
{
	if (key->t && ((key->t == sel->h) || (key->t == sel->t))){
		return 0;
	}
	return -1;
}

static list *pivot_table(stmt *set)
{
	list *pivots = list_create((fdestroy)&stmt_destroy);
	node *n;

	if (set->type != st_set && set->type != st_sets){
		if (set->nrcols == 1) {
			list_append(pivots, stmt_dup(head_column(set)) );
		}
		if (set->nrcols == 2) {
			list_append(pivots, stmt_dup(head_column(set)) );
			list_append(pivots, stmt_dup(tail_column(set)) );
		}
	} else {
	    list *l = set->op1.lval;
	    if (set->type != st_set)
		l = l->h->data;
	    for( n = l->h; n; n = n->next ){
		stmt *st = n->data;

		if (st->nrcols == 1) {
		       	if (!list_find(pivots, (void*)st, (fcmp)&cmp_head)) {
				list_append(pivots, stmt_dup(head_column(st)) );
			}
		}
		if (st->nrcols == 2) {
			if (!list_find(pivots, (void*)st, (fcmp)&cmp_head)) {
				list_append(pivots, stmt_dup(head_column(st)) );
			}
			if (!list_find(pivots, (void*)st, (fcmp)&cmp_tail)) {
				list_append(pivots, stmt_dup(tail_column(st)) );
			}
		}
	    }
	}
	return pivots;
}

static stmt *stmt2pivot(context * sql, scope * scp, stmt * s)
{
	return stmt_ptable( pivot_table(s), s);
}

static stmt *find_on_column_name(context * sql, scope * scp, tvar * t, char *name)
{
	node *m;

	for (m = t->columns->h; m; m = m->next) {
		cvar *rc = m->data;
		if (strcmp(name, rc->cname) == 0) {
			return stmt_dup(rc->s);
		}
	}
	return NULL;
}

static list *join_on_column_name(context * sql, scope * scp, tvar * tv,
				 tvar * r, int all)
{
	list *res = create_stmt_list();
	node *n;
	for (n = tv->columns->h; n; n = n->next) {
		cvar *lc = n->data;
		stmt *rs = find_on_column_name(sql, scp, r, lc->cname);
		if (rs) {
			list_append(res, stmt_dup(lc->s));
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
		return sql_error(sql, 02, "Cannot have a NATURAL JOIN with a join specification (ON or USING);");
	}
	if (!js && !natural) {
		return sql_error(sql, 02, "Must have NATURAL JOIN or a JOIN with a specification (ON or USING);");
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
				if (ls) stmt_destroy(ls);
				if (rs) stmt_destroy(rs);
				if (s) stmt_destroy(s);
				return sql_error(sql, 02, "Tables %s and %s do have a matching column %s\n", tv1->tname, tv2->tname, nm);
			}
			rs = check_types(sql, tail_type(ls), stmt_reverse(rs));
			if (!rs) {
				stmt_destroy(ls);
				if (s) stmt_destroy(s);
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

		matching_columns = join_on_column_name(sql, scp, tv1, tv2, 0);

		if (!matching_columns || list_length(matching_columns) == 0) {
			if (matching_columns) list_destroy(matching_columns);
			return sql_error(sql, 02, "No attributes of tables %s and %s match\n", tv1->tname, tv2->tname);
		}

		for (m = matching_columns->h; m; m = m->next->next) {
			stmt *j;
			stmt *ls = stmt_dup(m->data);
			stmt *rs = stmt_dup(m->next->data);

			rs = check_types(sql, tail_type(ls), stmt_reverse(rs));
			if (!rs) {
				if (ls)
					stmt_destroy(ls);
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
		     return sql_error(sql, 02, "Outer join with outer reference not implemented\n");
		   }
		 */
	}
	subset = s;
	if (subset) {
		list *l1;
		tvar *t = NULL;
		node *n;
		stmt *fs1 = find_pivot(subset, tv1->s);
		stmt *fs2 = find_pivot(subset, tv2->s);
		stmt *ld = NULL, *rd = NULL;

		if (!fs1 || !fs2) {
			if (fs1) stmt_destroy(fs1);
			if (fs2) stmt_destroy(fs2);
			if (!fs1) {
		     		return sql_error(sql, 02, "Subset %s not found in join expression", tv1->tname);
			} else if (!fs2) {
		     		return sql_error(sql, 02, "Subset %s not found in join expression", tv2->tname);
			} else {
		     		return sql_error(sql, 02, "Subsets %s,%s not found in join expression", tv1->tname, tv2->tname);
			}

		}

		t = tv1;
		if (jointype == jt_left || jointype == jt_full) {
			cvar *cs = t->columns->h->data;
			/* we need to add the missing oid's */
			ld = stmt_diff(stmt_dup(cs->s), stmt_reverse(stmt_dup(fs1)));
			ld = stmt_mark(stmt_reverse(ld), -1);
		}
		t = tv2;
		if (jointype == jt_right || jointype == jt_full) {
			cvar *cs = t->columns->h->data;
			/* we need to add the missing oid's */
			rd = stmt_diff(stmt_dup(cs->s), stmt_reverse(stmt_dup(fs2)));
			rd = stmt_mark(stmt_reverse(rd), -1);
		}
		l1 = create_stmt_list();
		t = tv1;
		for (n = t->columns->h; n; n = n->next) {
			cvar *cs = n->data;

			list_append(l1, stmt_join(stmt_dup(fs1), stmt_dup(cs->s), cmp_equal));
		}
		t = tv2;
		for (n = t->columns->h; n; n = n->next) {
			cvar *cs = n->data;

			list_append(l1, stmt_join(stmt_dup(fs2), stmt_dup(cs->s), cmp_equal));
		}
		if (fs1) stmt_destroy(fs1);
		if (fs2) stmt_destroy(fs2);
		if (jointype == jt_left || jointype == jt_full) {
			list *l2 = create_stmt_list();
			node *m = l1->h;
			t = tv1;
			for (n = t->columns->h; n;
			     n = n->next, m = m->next) {
				cvar *cs = n->data;

				list_append(l2, stmt_union(stmt_dup(m->data), stmt_join (stmt_dup(ld), stmt_dup(cs->s), cmp_equal)));
			}
			t = tv2;
			for (n = t->columns->h; n;
			     n = n->next, m = m->next) {
				cvar *cs = n->data;

				list_append(l2, stmt_union(stmt_dup(m->data), stmt_const (stmt_dup(ld), stmt_atom (atom_general (tail_type(cs->s), NULL)))));
			}
			list_destroy(l1);
			l1 = l2;
		}
		if (jointype == jt_right || jointype == jt_full) {
			list *l2 = create_stmt_list();
			node *m = l1->h;
			t = tv1;
			for (n = t->columns->h; n;
			     n = n->next, m = m->next) {
				cvar *cs = n->data;

				list_append(l2, stmt_union(stmt_dup(m->data), stmt_const (stmt_dup(rd), stmt_atom (atom_general (tail_type(cs->s), NULL)))));
			}
			t = tv2;
			for (n = t->columns->h; n;
			     n = n->next, m = m->next) {
				cvar *cs = n->data;

				list_append(l2, stmt_union(stmt_dup(m->data), stmt_join (stmt_dup(rd), stmt_dup(cs->s), cmp_equal)));
			}
			list_destroy(l1);
			l1 = l2;
		}
		if (ld) stmt_destroy(ld);
		if (rd) stmt_destroy(rd);
		s = stmt_list(l1);
	}
	if (subset) stmt_destroy(subset);
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

	return sql_join_(sql, scp, tab_ref1, natural, jointype, tab_ref2, joinspec);
}

static stmt *sql_cross(context * sql, scope * scp, symbol * q)
{

	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	symbol *tab_ref2 = n->next->data.sym;

	tvar *tv1 = table_ref(sql, scp, tab_ref1);
	tvar *tv2 = table_ref(sql, scp, tab_ref2);
	cvar *cv1 = tv1->columns->h->data;
	cvar *cv2 = tv2->columns->h->data;
	stmt *ct;

	if (!tv1 || !tv2)
		return NULL;

	ct = stmt_join(stmt_dup(cv1->s), stmt_reverse(stmt_dup(cv2->s)), cmp_all);

	if (ct) {
		list *rl = create_stmt_list();
		node *nv;

		ct = stmt2pivot(sql, scp, ct);

		for (nv = scp->tables->h; nv; nv = nv->next) {
			node *n;
			tvar *tv = nv->data;
			stmt *foundsubset = find_pivot(ct, tv->s);

			for (n = tv->columns->h; n; n = n->next) {
				cvar *cs = n->data;
				list_append(rl, stmt_join (foundsubset, stmt_dup(cs->s), cmp_equal));
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
			res = table_optname(sql, scp, tq, q->sql, q->data.lval->t->data.sym);
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
			res = table_optname(sql, scp, tq, q->sql, q->data.lval->t->data.sym);
			scp = scope_close(nscp);
			return res;
		}
	case SQL_UNION:
		{
			scope *nscp = scope_open(scp);
			node *m;
			dnode *n = q->data.lval->h;
			/*int all = n->next->data.ival;*/
			tvar *lv = table_ref(sql, nscp, n->data.sym);
			tvar *rv = table_ref(sql, nscp, n->next->next->data.sym);
			list *unions, *matching_columns;

			if (!lv || !rv)
				return NULL;

			/* find the matching columns (all should match?)
			 * union these 
			 * if !all do a distinct operation at the end 
			 */
			/* join all result columns ie join(lh,rh) on column_name */

			matching_columns = join_on_column_name(sql, nscp, lv, rv, 1);

			if (!matching_columns)
				return NULL;

			unions = create_stmt_list();
			for (m = matching_columns->h; m; m = m->next->next) {
				stmt *l = stmt_dup(m->data);
				stmt *r = stmt_dup(m->next->data);
				list_append(unions, stmt_union(l, r));
			}
			res = table_optname(sql, scp, stmt_list(unions), q->sql, q->data.lval->t->data.sym);
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
static stmt *columns_exp(context * sql, scope * scp, symbol * column_e, group * grp, stmt * subset)
{
	char *tname = column_e->data.lval->h->data.sval;
	tvar *tv = scope_bind_table(scp, tname);

	if (tv) {
		stmt *foundsubset = find_pivot(subset, tv->s);
		list *columns = create_stmt_list();
		node *n = tv->columns->h;
		if (grp)
			foundsubset = stmt_join(stmt_dup(grp->ext), foundsubset, cmp_equal);
		while (n) {
			cvar *cv = n->data;
			list_append(columns, stmt_join(stmt_dup(foundsubset), stmt_dup(cv->s), cmp_equal));
			n = n->next;
		}
		stmt_destroy(foundsubset);
		return stmt_list(columns);
	}
	return NULL;
}


static stmt *column_exp(context * sql, scope * scp, symbol * column_e, group * grp, stmt * subset) {
	dlist *l = column_e->data.lval;
	stmt *s = sql_value_exp(sql, scp, l->h->data.sym, grp, subset);
	if (!s)
		return NULL;

	if (grp && s->type != st_aggr) {
		s = stmt_join(stmt_dup(grp->ext), s, cmp_equal);
	}

	/* AS name */
	if (s && l->h->next->data.sval) {
		s = stmt_alias(s, l->h->next->data.sval);
		scope_add_alias(scp, stmt_dup(s), l->h->next->data.sval);
	}
	return s;
}

static stmt *sql_column_exp(context * sql, scope * scp, symbol * column_e, group * grp, stmt * subset)
{
	stmt *res = NULL;
	if (column_e->token == SQL_TABLE) {
		res = columns_exp(sql, scp, column_e, grp, subset);
	} else if (column_e->token == SQL_COLUMN) {
		res = column_exp(sql, scp, column_e, grp, subset);
	}
	if (!res) {
		return sql_error(sql, 02, "Column expression Symbol(%d)->token = %s no output", (int) column_e->token, token2string(column_e->token));
	}
	return res;
}

static list *list_map_merge(list * l2, int seqnr, list * l1)
{
	list *res = list_dup(l1, (fdup)&stmt_dup);
	res = list_merge(res, l2, (fdup)&stmt_dup);
	return res;
}

static list *list_map_append_list(list * l2, int seqnr, list * l1)
{
	return list_append(l1, list_dup(l2, (fdup)&stmt_dup));
}

static stmt *sql_compare(context * sql, stmt * ls,
			 stmt * rs, char *compare_op)
{
	int join = 0;
	comp_type type = cmp_equal;

	if (!ls || !rs)
		return NULL;

	if (ls->nrcols <= 0 && rs->nrcols <= 0) {
		stmt_destroy(ls);
		stmt_destroy(rs);
		return sql_error(sql, 02, "Compare(%s) between two atoms is not possible", compare_op);
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
			return stmt_select(ls, rs, type);
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

static stmt *sql_compare_exp( context *sql, scope *scp, group *grp, stmt *subset, symbol *lo, symbol *ro, char *compare_op )
{
	stmt *rs, *ls = sql_value_exp(sql, scp, lo, grp, subset);
	if (!ls)
		return NULL;
	if (ro->token != SQL_SELECT) {
		rs = sql_value_exp(sql, scp, ro, grp, subset);
		if (!rs)
			return NULL;
		if (grp && (!rs->key || !ls->key)) {
			stmt_destroy(rs);
			stmt_destroy(ls);
			return sql_error(sql, 02, "Cannot compare sets with values, probably a aggregate function missing");
		}
		return sql_compare(sql, ls, rs, compare_op);
	} else {
		node *o;
		rs = scope_subquery(sql, scp, ro);

		if (!rs)
			return NULL;
		if (rs->type != st_list || list_length(rs->op1.lval) == 0) {
			return sql_error(sql, 02, "Subquery result wrong");
		}
		o = rs->op1.lval->h;
		if (list_length(rs->op1.lval) == 1) {
			stmt *j = sql_compare(sql, stmt_dup(ls), 
					stmt_dup(o->data), compare_op);
			if (!j)
				return NULL;
			return stmt_semijoin(ls, j);
		} else {
			stmt *sd, *j = sql_compare(sql, ls, 
					stmt_dup(o->data), compare_op);
			if (!j)
				return NULL;
			sd = stmt_set( stmt_join(j, 
				stmt_dup(o->next->data), cmp_equal));
			o = o->next;
			o = o->next;
			for (; o; o = o->next) {
				list_append(sd->op1.lval,
					stmt_join(stmt_dup(j), 
						stmt_dup(o->data), cmp_equal));
			}
			return sd;
		}
		stmt_destroy(rs);
		return NULL;
	}
}

static stmt *sql_and(context * sql, stmt * ls, stmt * rs)
{
	stmt *res = NULL;
	if (!ls || !rs)
		return NULL;
	if (ls->type != st_set && ls->type != st_sets) {
		ls = stmt_set(ls);
	}
	if (rs->type != st_set && rs->type != st_sets) {
		rs = stmt_set(rs);
	}
	if (ls->type == st_set && rs->type == st_set) {
		list_merge( ls->op1.lval, rs->op1.lval,(fdup)&stmt_dup);
		stmt_destroy(rs);
		res = ls;
	} else if (ls->type == st_sets && rs->type == st_set) {
		res = stmt_sets(list_map(ls->op1.lval, 
					(map_func) & list_map_merge,
			 		rs->op1.sval));
		stmt_destroy(rs);
		stmt_destroy(ls);
	} else if (ls->type == st_set && rs->type == st_sets) {
		res = stmt_sets(list_map(rs->op1.lval, 
					(map_func) & list_map_merge,
			 		ls->op1.sval));
		stmt_destroy(rs);
		stmt_destroy(ls);
	} else if (ls->type == st_sets && rs->type == st_sets) {
		res = stmt_sets(list_map(ls->op1.lval, 
					(map_func) & list_map_merge,
			 		rs->op1.sval));
		stmt_destroy(rs);
		stmt_destroy(ls);
	}
	return res;
}

static stmt *sql_or(context * sql, stmt * ls, stmt * rs)
{
	stmt *res = NULL;

	if (!ls || !rs)
		return NULL;
	if (ls->type != st_set && ls->type != st_sets) {
		ls = stmt_set(ls);
	}
	if (rs->type != st_set && rs->type != st_sets) {
		rs = stmt_set(rs);
	}
	if (ls->type == st_set && rs->type == st_set) {
		res = stmt_sets(
			list_append( 
			    list_append(
				list_create((fdestroy)&list_destroy),
				  list_dup(ls->op1.lval, (fdup)&stmt_dup)),
				    list_dup(rs->op1.lval, (fdup)&stmt_dup)));
		stmt_destroy(ls);
		stmt_destroy(rs);
	} else if (ls->type == st_sets
		   && rs->type == st_set) {
		list_append(ls->op1.lval, 
				list_dup(rs->op1.lval, (fdup)&stmt_dup));
		res = ls;
		stmt_destroy(rs);
	} else if (ls->type == st_set
		   && rs->type == st_sets) {
		list_append(rs->op1.lval, 
				list_dup(ls->op1.lval, (fdup)&stmt_dup));
		res = rs;
		stmt_destroy(ls);
	} else if (ls->type == st_sets
		   && rs->type == st_sets) {
		(void) list_map(ls->op1.lval, (map_func) &
			list_map_append_list, rs->op1.sval);
		res = ls;
		stmt_destroy(rs);
	}
	return res;
}

static stmt *sql_logical_exp(context * sql, scope * scp, symbol * sc, group * grp, stmt * subset)
{
	if (!sc)
		return NULL;
	switch (sc->token) {
	case SQL_OR:
		{
			symbol *lo = sc->data.lval->h->data.sym;
			symbol *ro = sc->data.lval->h->next->data.sym;
			stmt *ls = sql_logical_exp(sql, scp, lo, grp, subset);
			stmt *rs = sql_logical_exp(sql, scp, ro, grp, subset);
			return sql_or(sql, ls, rs);
		}
		break;
	case SQL_AND:
		{
			symbol *lo = sc->data.lval->h->data.sym;
			symbol *ro = sc->data.lval->h->next->data.sym;
			stmt *ls = sql_logical_exp(sql, scp, lo, grp, subset);
			stmt *rs = sql_logical_exp(sql, scp, ro, grp, subset);
			return sql_and(sql, ls, rs);
		}
		break;
	case SQL_COMPARE:
		{
			symbol *lo = sc->data.lval->h->data.sym;
			symbol *ro = sc->data.lval->h->next->next->data.sym;
			char *compare_op = sc->data.lval->h->next->data.sval;
			return sql_compare_exp(
				 sql, scp, grp, subset, lo, ro, compare_op);
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
			stmt *ls = sql_value_exp(sql, scp, lo, grp, subset);
			stmt *rs1 = sql_value_exp(sql, scp, ro1, grp, subset);
			stmt *rs2 = sql_value_exp(sql, scp, ro2, grp, subset);
			sql_subtype *ct = NULL;
			if (!ls || !rs1 || !rs2)
				return NULL;
			if (rs1->nr > 0 || rs2->nr > 0) {
				return sql_error(sql, 02, "Between requires an atom on the right handside");
			}
			ct = tail_type(ls);
			rs1 = check_types(sql, ct, rs1);
			rs2 = check_types(sql, ct, rs2);
			if (symmetric) {
				stmt *tmp = NULL;
				sql_func *min = sql_bind_func("min", tail_type(rs1), tail_type(rs2), NULL);
				sql_func *max = sql_bind_func("max", tail_type(rs1), tail_type(rs2), NULL);
				if (!min || !max) {
					return sql_error(sql, 02, "min or max operator on types %s %s missing",
						 tail_type(rs1)->type->sqlname,
						 tail_type(rs2)->type->sqlname);
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
				AtomNode *an = (AtomNode*)ro;
				a = an->a;
			} else {
				AtomNode *an = (AtomNode*)ro->data.lval->h->data.sym;
				AtomNode *en = (AtomNode*)ro->data.lval->h->next->data.sym;
				a = an->a;
				e = en->a;
			}
			if (e) {
				return sql_error(sql, 02, "Time to implement LIKE escapes");
			}
			if (a->type != string_value) {
				return sql_error(sql, 02, "Wrong type used with LIKE stmt, should be string '%s' %s",
					 atom2string(a),
					 atom_type(a)->type->sqlname);
			}
			res = stmt_like(ls, stmt_atom(atom_dup(a)));
			if (sc->token == SQL_NOT_LIKE)
				res = stmt_diff(ls, res);
			return res;
		}
	case SQL_IN:
		/*
         <in predicate> ::=
              <row value constructor>
                [ NOT ] IN <in predicate value>

         <in predicate value> ::=
                <table subquery>
              | <left paren> <in value list> <right paren>

         <in value list> ::=
              <value expression> { <comma> <value expression> }...
		*/
		{
			dlist *l = sc->data.lval;
			symbol *lo = l->h->data.sym;
			stmt *ls =
			    sql_value_exp(sql, scp, lo, grp, subset);
			if (!ls)
				return NULL;
			if (l->h->next->type == type_list) {
				dnode *n = l->h->next->data.lval->h;
				stmt *temp = stmt_temp(stmt_dup(ls));
				sql_subtype *ct = tail_type(ls);

				for (; n; n = n->next) {
					AtomNode *an = n->data.symv;
					temp = stmt_append(temp, 
						check_types(sql, ct,
						stmt_atom( atom_dup(an->a))));
				}
				return stmt_join(ls, 
					stmt_reverse(temp), cmp_equal);
			} else if (l->h->next->type == type_symbol) {
				symbol *ro = l->h->next->data.sym;
				stmt *sq = scope_subquery(sql, scp, ro);

				if (!sq)
					return NULL;
				if (sq->type != st_list
				    || list_length(sq->op1.lval) == 0) {
					return sql_error(sql, 02, "Subquery result wrong");
				}
				if (list_length(sq->op1.lval) == 1) {
					stmt *rs = sq->op1.lval->h->data;
					return
					    stmt_reverse
					    (stmt_semijoin
					     (stmt_reverse(ls),
					      stmt_reverse(rs)));
				} else {	/* >= 2, ie match full rows */
					/* TODO fix this broken impl ! */
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
				return sql_error(sql, 02, "In missing inner query");
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
				stmt *temp = stmt_temp(stmt_dup(ls));
				sql_subtype *ct = tail_type(ls);

				for (; n; n = n->next) {
					AtomNode *an = n->data.symv;
					temp = stmt_append(temp, 
						check_types(sql, ct,
						stmt_atom( atom_dup(an->a))));
				}
				return stmt_diff(ls, stmt_join(ls, 
					stmt_reverse(temp), cmp_equal));
			} else if (l->h->next->type == type_symbol) {
				symbol *ro = l->h->next->data.sym;
				stmt *sq = scope_subquery(sql, scp, ro);

				if (!sq)
					return NULL;

				if (sq->type != st_list
				    || list_length(sq->op1.lval) == 0) {
					return sql_error(sql, 02, "Subquery result wrong");
				}
				if (list_length(sq->op1.lval) == 1) {
					stmt *rs = sq->op1.lval->h->data;
					return
					    stmt_reverse
					    (stmt_diff
					     (stmt_reverse(ls),
					      stmt_reverse(rs)));
				} else {	/* >= 2, ie match full rows */
					/* TODO fix this broken impl ! */
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
				return sql_error(sql, 02, "In missing inner query");
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
				stmt_destroy(ls);
				return sql_error(sql, 02, "Subquery result wrong");
			}
			if (list_length(ls->op1.lval) == 1) {
				/* NOT still broken */
				stmt *res, *s = ls->op1.lval->h->data; 
				res = stmt_reverse(stmt_dup(s));
				stmt_destroy(ls);
				return res;
			} else if (sc->token == SQL_EXISTS) {
				node *o = ls->op1.lval->h->next;	/* skip first */
				stmt *sd, *j = stmt_reverse(stmt_dup(o->data));

				o = o->next;
				if (!o) {
					stmt_destroy(ls);
					return j;
				}

				sd = stmt_set(stmt_join(j, 
						stmt_dup(o->data), cmp_equal));
				o = o->next;
				for (; o; o = o->next) {
					list_append(sd->op1.lval,
						stmt_join (j, stmt_dup(o->data),
							cmp_equal));
				}
				stmt_destroy(ls);
				return sd;
			} else {	/* not exists */
				node *o = ls->op1.lval->h->next;	/* skip first */
				stmt *sd, *j, *jr;

				/* broken as tail_column doesn't return the
				 * requested top most tail (stops at first
				 * pivot).
				j = stmt_diff(
					stmt_dup(tail_column(o->data)),
					stmt_reverse(stmt_dup(o->data)));
				 */
				j = stmt_reverse(stmt_dup(o->data));

				o = o->next;
				if (!o) {
					stmt_destroy(ls);
					return j;
				}
				/* current implementation for larger results 
				 * is broken! */

				/* result gives a table, [skip, O1, O2 .. On]
				 * we should return table
				 * 	outer join O1,O2, .. On minus
				 *		result of sub query
				 *
				 * this requires group construction etc
				 * Its probably cheaper to do a sub-proc
				 * per selected value. Ie translate to
				 * [] + select
				 */

				jr = stmt_diff(
					stmt_dup(tail_column(o->data)),
					stmt_reverse(stmt_dup(o->data)));
				sd = stmt_set(stmt_join (j, stmt_reverse(jr),
					cmp_all));
				o = o->next;
				for (; o; o = o->next) {
					jr = stmt_diff(
					  stmt_dup(tail_column(o->data)),
					  stmt_reverse(stmt_dup(o->data)));

					list_append(sd->op1.lval,
						stmt_join (j, stmt_reverse(jr),
							  cmp_all));
				}
				stmt_destroy(ls);
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
				sql_subtype *tpe = tail_type(res);
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
		return sql_error(sql, 02, 
			 "Predicate %s %d: time to implement some more",
			 token2string(sc->token), sc->token);
	}
	return sql_error(sql, 02, "Predicate: time to implement some more");
}

static stmt *having_condition(context * sql, scope * scp, symbol * sc, group * grp, stmt * subset)
{
	if (!sc)
		return NULL;
	switch (sc->token) {
	case SQL_OR:
		{
			symbol *lo = sc->data.lval->h->data.sym;
			symbol *ro = sc->data.lval->h->next->data.sym;
			stmt *ls = having_condition(sql, scp, lo, grp, subset);
			stmt *rs = having_condition(sql, scp, ro, grp, subset);
			if (!ls || !rs)
				return NULL;

			return stmt_union(ls, rs);
		}
		break;
	case SQL_AND:
		{
			symbol *lo = sc->data.lval->h->data.sym;
			symbol *ro = sc->data.lval->h->next->data.sym;
			stmt *ls = having_condition(sql, scp, lo, grp, subset);
			stmt *rs = having_condition(sql, scp, ro, grp, subset);
			if (!ls || !rs)
				return NULL;

			return stmt_semijoin(ls, rs);
		}
		break;
	case SQL_COMPARE:
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
	case SQL_LIKE:
	case SQL_NOT_LIKE:
	case SQL_IN:
	case SQL_NOT_IN:
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
			return sql_logical_exp(sql, scp, sc, grp, subset);
	default:
		return sql_error(sql, 02, 
			 "Predicate %s %d: time to implement some more",
			 token2string(sc->token), sc->token);
	}
	return sql_error(sql, 02, "Predicate: time to implement some more");
}


static group *query_groupby_inner(context * sql, scope * scp, stmt * c, stmt * subset, group * cur)
{
	stmt *s = find_pivot(subset, c->h);
	if (s) {
		stmt *j = stmt_join(s, c, cmp_equal);
		cur = grp_create(j, cur);
	}

	/*
	node *n = subset->op1.lval->h;
	while (n) {
		stmt *s = n->data;
		if (s->t == c->h) {
			stmt *j = stmt_join(stmt_dup(s), c, cmp_equal);
			cur = grp_create(j, cur);
			break;
		}
		n = n->next;
	}
	*/
	if (!cur) {
		assert(cur);
		(void) sql_error(sql, 02, 
			 "subset not found for groupby column %s\n",
			 column_name(c));
		return NULL;
	}
	return cur;
}

static group *query_groupby(context * sql, scope * scp, symbol * groupby, stmt * subset)
{
	group *cur = NULL;
	dnode *o = groupby->data.lval->h;
	while (o) {
		symbol *grp = o->data.sym;
		stmt *c = sql_column_ref(sql, scp, grp);
		cur = query_groupby_inner(sql, scp, c, subset, cur);
		o = o->next;
	}
	return cur;
}

static group *query_groupby_lifted(context * sql, scope * scp, stmt * subset)
{
	group *cur = NULL;
	node *o = scp->lifted->h;
	for ( ; o; o = o->next) {
		tvar *tv = o->data;
		stmt *s = find_pivot(subset, tv->s);
		if (s) 
			cur = grp_create(s, cur);
	}
	return cur;
}

static stmt *query_orderby(context * sql, scope * scp, symbol * orderby, stmt * st, stmt * subset, group * grp)
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
				if (sc->type == st_bat) {
					j = find_pivot(subset, sc->h);
					if (!j) {
						return sql_error(sql, 02, "subset not found for orderby column\n");
					}
					if (grp)
						j = stmt_join(stmt_dup(grp->ext), j, cmp_equal);
					j = stmt_join(j, sc, cmp_equal);
				} else {
					j = sc;
				}
				if (cur)
					cur = stmt_reorder(cur, j, direction);
				else
					cur = stmt_order(j, direction);
			} else {
				return NULL;
			}
		} else {
			return sql_error(sql, 02, "order not of type SQL_COLUMN\n");
		}
		o = o->next;
	}
	return cur;
}

static stmt *sql_simple_select(context * sql, scope * scp, dlist * selection)
{
	stmt *s = NULL;
	list *rl = create_stmt_list();

	if (selection) {
		dnode *n = selection->h;
		while (n) {
			stmt *cs = sql_column_exp(sql, scp, n->data.sym, NULL, NULL);

			if (!cs) {
				list_destroy(rl);
				return NULL;
			}

			/* t1.* */
			if (cs->type == st_list && n->data.sym->token == SQL_TABLE) {
				list_merge(rl, cs->op1.lval, (fdup)&stmt_dup);
				stmt_destroy(cs);
			} else if (cs->type == st_list) {	/* subquery */
				printf("subquery in simple_select\n");
				stmt_destroy(cs);
				return NULL;
			} else if (cs->nrcols == 0){
				list_append(rl, cs);
			} else {
				list_append(rl, cs);
			}
			n = n->next;
		}
	}
	s = stmt_list(rl);

	if (!s)
		return sql_error(sql, 02, "Subquery result missing");
	return s;
}

static stmt *sql_select(context * sql, scope * scp, SelectNode *sn, int toplevel )
{
	list *rl = NULL;
	stmt *s = NULL;

	stmt *order = NULL, *subset = NULL;
	group *grp = NULL;

	if (!sn->from && !sn->where)
		return sql_simple_select(sql, scp, sn->selection);

	if (sn->where) {
		node *n;
		stmt *cur = NULL;

		s = sql_logical_exp(sql, scp, sn->where, NULL, NULL);
		if (!s) {
			sql_select_cleanup(sql, s, subset, grp);
			return sql_error( sql, 02, "Subquery result missing");
		}
		if (s->type != st_set && s->type != st_sets) {
			s = stmt_set(s);
		}
		/* check for tables not used in the where part */
		if (s->type == st_set){ /* TODO: handle st_sets!! */
		    for (n = scp->tables->h; n; n = n->next) {
			tvar *v = n->data;
			stmt *tmp = complex_find_subset(s, v->s);
			if (!tmp) {
				cvar *cv = v->columns->h->data;
				tmp = stmt_dup(cv -> s);
				/* just add a select whole column */
				if (!cur) {
					cur = stmt_dup(tmp);
				/* add join to an allready used column */
				} else {	
					tmp = stmt_join(stmt_dup(cur),
						stmt_reverse(tmp), cmp_all);
				}
				s = sql_and(sql, s, tmp);
			} else if (!cur) {
				cur = tmp;
			} else {
				stmt_destroy(tmp);
			}
		    }
		}
		if (cur) 
			stmt_destroy(cur);
		
	} else if (sn->from) {
		node *n;
		stmt *cur = NULL;
		for (n = scp->tables->h; n; n = n->next) {
			tvar *tv = n->data;
			cvar *cv = tv->columns->h->data;
			stmt *tmp = stmt_dup(cv -> s);
			if (!cur) {
				if (isbasetable(tv->s)){
					cur = stmt_diff(tmp, stmt_reverse(
					   stmt_tbat(basetable_table(tv->s), RDONLY, st_dbat )));
				} else {
					cur = tmp;
				}
			} else {
				tmp = stmt_join(stmt_dup(cur), 
						stmt_reverse(tmp), cmp_all);
				if (s) {
					list_append(s->op1.lval, tmp);
				} else {
					s = stmt_set(tmp);
				}
			}
		}
		if (!cur) {
			sql_select_cleanup(sql, s, subset, grp);
			return sql_error(sql, 02, "Subquery has no columns");
		}
		if (!s)
			s = cur;
		else
			stmt_destroy(cur);
	}

	if (s) {
		s = stmt2pivot(sql, scp, s);
		if (s && sn->groupby) {
			grp = query_groupby(sql, scp, sn->groupby, s);
			if (!grp)  {
				sql_select_cleanup(sql, s, subset, grp);
				return sql_error( sql, 02, "Subquery result missing");
			}
		}

		if (s && list_length(scp->lifted) > 0) {
			grp = query_groupby_lifted(sql, scp, s);
			if (!grp) {
				sql_select_cleanup(sql, s, subset, grp);
				return sql_error( sql, 02, "Subquery result missing");
			}
		}
	}

	subset = stmt_dup(s);
	if (sn->having) {
		if (s) stmt_destroy(s);
		s = having_condition(sql, scp, sn->having, grp, subset);

		if (!s){
			sql_select_cleanup(sql, s, subset, grp);
			return sql_error( sql, 02, "Subquery result missing");
		}

		if (grp) {
			grp = grp_semijoin(grp, stmt_dup(s));
		} else {
			node *n = NULL;
			list *sl = create_stmt_list();
			assert (subset->type == st_ptable);
			for (n = subset->op1.lval->h; n; n = n->next) {
				stmt *cs = n->data;

				list_append(sl, stmt_semijoin(stmt_dup(cs), stmt_dup(s)));
			}
			stmt_destroy(subset);
			subset = stmt_list(sl);
		}
	}

	if (!subset){
		sql_select_cleanup(sql, s, subset, grp);
		return sql_error( sql, 02, "Subquery result missing");
	}

	rl = create_stmt_list();
	if (sn->selection) {
		list *sl = create_stmt_list();
		dnode *n;
		node *m;
		int nrcols = 0;

		for (n = sn->selection->h; n; n = n->next ){
			stmt *cs = sql_column_exp(sql, scp, n->data.sym, grp, subset);
			if (!cs){
				list_destroy(sl);
				list_destroy(rl);
				sql_select_cleanup(sql, s, subset, grp);
				return sql_error( sql, 02, "Subquery result missing");
			}

			list_append(sl, cs);
			if (nrcols < cs->nrcols)
				nrcols = cs->nrcols;
		}

		for (m = sl->h, n = sn->selection->h; 
		     m && n; 
		     m = m->next, n = n->next){
			stmt *cs = m->data; 
			/* t1.* */
			if (cs->type == st_list && n->data.sym->token == SQL_TABLE){
				list_merge(rl, cs->op1.lval, (fdup)&stmt_dup);
			} else if (cs->type == st_list) {	/* subquery */
				if (list_length(cs->op1.lval) == 1) {	/* single value */
					stmt *ss = subset->op1.lval->h->data;
					stmt *cs1 = cs->op1.lval->h->data;
					ss = stmt_dup(ss);
					cs1 = stmt_dup(cs1);
					list_append(rl, stmt_join (ss, cs1, cmp_all));
				} else {	/* referenced variable(s) (can only be 2) */
					stmt *ids = cs->op1.lval->h->next->data;
					stmt *ss = find_pivot(subset, ids->t);
					ids = stmt_dup(ids);
					list_append(rl, stmt_outerjoin( stmt_outerjoin(ss, stmt_reverse (ids), cmp_equal), stmt_dup(cs->op1.lval->h->data), cmp_equal));
				}
			} else if (cs->nrcols == 0 && nrcols > 0){
				stmt *ss = first_subset(subset);
				cs = stmt_const(ss,stmt_dup(cs));
				list_append(rl, cs);
			} else {
				list_append(rl, stmt_dup(cs));
			}
		}
		list_destroy(sl);
	} else {
		/* select * from tables */
		if (toplevel) {
			node *nv;
			for (nv = scp->tables->h; nv; nv = nv->next) {
				node *n;
				tvar *tv = nv->data;
				stmt *foundsubset = find_pivot(subset, tv->s);

				for (n = tv->columns->h; n; n = n->next) {
					cvar *cs = n->data;
					stmt *cbat = stmt_dup(cs->s);
					list_append(rl, stmt_join(stmt_dup(foundsubset), cbat, cmp_equal));
				}
				stmt_destroy(foundsubset);
			}
		} else {
			/* 
			 * subquery * can only return one column, better
			 * the oids are needed 
			 */
			tvar *tv = scope_first_table(scp);
			stmt *foundsubset = find_pivot(subset, tv->s);

			if (!foundsubset){
				list_destroy(rl);
				sql_select_cleanup(sql, s, subset, grp);
				return sql_error( sql, 02, "Subquery result missing");
			}

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
		list *vars = scp->lifted;
		node *o = vars->h;
		for (; o; o = o->next) {
			tvar *v = o->data;
			stmt *foundsubset = find_pivot(subset, v->s);
			stmt *t;

			if (!foundsubset){
				list_destroy(rl);
				list_destroy(vars);
				sql_select_cleanup(sql, s, subset, grp);
				return sql_error( sql, 02, "Subquery result missing");
			}
			/* TODO fix this hack */
			list_append(rl,
				t=stmt_join( stmt_dup(grp->ext), 
					   foundsubset, cmp_equal));
			t->nrcols = 1;
		}
	}
	stmt_destroy(s);
	s = stmt_list(rl);

	if (s && subset && sn->distinct){
		node *n = s->op1.lval->h;
		group *grp = NULL;
		rl = create_stmt_list();
		while(n) {
			stmt *t = n->data;
			grp = grp_create(stmt_dup(t), grp);
			n = n->next;
		}
		n = s->op1.lval->h;
		while(n) {
			stmt *t = n->data;
			list_append(rl, stmt_join(stmt_dup(grp->ext), stmt_dup(t), cmp_equal));
			n = n->next;
		}
		stmt_destroy(s);
		s = stmt_list(rl);
	}


	if (s && subset && sn->orderby) {
		order = query_orderby(sql, scp, sn->orderby, s, subset, grp);
		if (!order) {
			sql_select_cleanup(sql, s, subset, grp);
			return sql_error( sql, 02, "Subquery result missing");
		}
	}

	if (!s) {
		sql_select_cleanup(sql, s, subset, grp);
		return sql_error( sql, 02, "Subquery result missing");
	}

	sql_select_cleanup(sql, NULL, subset, grp);

	if (sn->limit >= 0){
		if (order) 
			order = stmt_limit(order, sn->limit);
		else 
			order = stmt_limit(stmt_dup(s->op1.lval->h->data), sn->limit);
	}

	if (s && order)
		return stmt_ordered(order, s);
	return s;
}


static stmt *create_view(context * sql, schema * schema, stmt * ss,
			 dlist * qname, dlist * column_spec,
			 symbol * query, int check)
{

	catalog *cat = sql->cat;
	char *name = qname_table(qname);

	if (cat_bind_table(cat, schema, name)) {
		stmt_destroy(ss);
		return sql_error(sql, 02, "Create View name %s allready in use", name);
	} else {
		stmt *stct = NULL;
		stmt *sq = sql_subquery(sql, NULL, query);

		if (!sq)
			return NULL;

		if (column_spec) {
			table *table = cat_create_table(cat, 0, schema, name, tt_view, query->sql);
			dnode *n = column_spec->h;
			node *m = sq->op1.lval->h;

			table->s = stmt_dup(sq);
			while (n) {
				char *cname = n->data.sval;
				stmt *st = m->data;
				column *col = cat_create_column(cat, 0, table, cname, sql_dup_subtype( tail_type(st)), "NULL", 1);
				col->s = stmt_column(stmt_dup(st), stmt_dup(sq), name, cname);
				n = n->next;
				m = m->next;
			}
			stct = stmt_create_table(ss, table);
		} else {
			node *m;
			catalog *cat = sql->cat;
			table *table = cat_create_table(cat, 0, schema, name, tt_view, query->sql);
			table->s = stmt_dup(sq);
       			for ( m = sq->op1.lval->h; m; m = m->next ) {
				stmt *st = m->data;
				char *cname = column_name(st);
				column *col = cat_create_column(cat, 0, table, cname, sql_dup_subtype(tail_type(st)), "NULL", 1);
				col->s = stmt_column(stmt_dup(st), stmt_dup(sq), name, cname);
			}
			stct = stmt_create_table(ss, table);
		}
		stmt_destroy(sq);
		return stct;
	}
	return NULL;
}

static stmt *drop_view(context * sql, dlist * qname )
{
	stmt *res = NULL;
	char *tname = qname_table(qname);
	table *t = cat_bind_table(sql->cat, cur_schema(sql), tname);

	if (!t) {
		return sql_error(sql, 02, 
			 "Drop View, view %s unknown", tname);
	} else if (t->type == tt_view){
		stmt *ss = stmt_bind_schema( cur_schema(sql) );
		res = stmt_drop_table(ss, t->name, 0);
		cat_drop_table(sql->cat, cur_schema(sql), tname);
	} else {
		return sql_error(sql, 02, 
			 "Drop View, cannot drop table %s", tname);
	}
	return res;
}


static stmt *create_role(context * sql, schema * s, dlist * qname, int grantor)
{
	char *role_name = qname->t->data.sval;

	if (dlist_length(qname) > 2){
		return sql_error(sql, 02, 
			"qualified role can only have a schema and a role\n");
	} 
	return stmt_create_role(role_name, grantor);
}

static stmt *drop_role(context * sql, schema * s, dlist * qname )
{
	char *role_name = qname->t->data.sval;

	if (dlist_length(qname) > 2){
		return sql_error(sql, 02, "qualified role can only have a schema and a role\n");
	} 
	return stmt_drop_role(role_name);
}

static stmt *column_constraint_type(context * sql, char *name, symbol * s, stmt * ss, stmt * ts, stmt * cs, column *c)
{
	stmt *res = NULL;

	switch (s->token) {
	case SQL_UNIQUE: 
	{
		key *k = cat_table_add_key(c->table, ukey, name, NULL);
		cat_key_add_column(k, c, 0);
		res = stmt_create_key(k, NULL);
		stmt_destroy(cs);
		stmt_destroy(ss);
		stmt_destroy(ts);
	} break;
	case SQL_PRIMARY_KEY: 
	{
		key *k = cat_table_add_key(c->table, pkey, name, NULL);
		cat_key_add_column(k, c, 0);
		res = stmt_create_key(k, NULL);
		stmt_destroy(cs);
		stmt_destroy(ss);
		stmt_destroy(ts);
	} break;
	case SQL_FOREIGN_KEY:
	{
		dnode *n = s->data.lval->h;
		char *tname = qname_table(n->data.lval);
		char *cname = n->data.lval->h->data.sval;
		table *ft = cat_bind_table(sql->cat, c->table->schema, tname);
		key *rk = cat_table_bind_sukey(ft, cname);

		if (!ft) {
			return sql_error(sql, 02, "table %s unknown\n", tname);
		} else if (!rk) {
			return sql_error(sql, 02, "Could not find referenced unique key in table %s\n", ft->name );
		} else {
			key *k = cat_table_add_key(c->table, fkey, name, rk);
			/*stmt *fts = stmt_bind_table(ss, ft);*/
			stmt *rks = stmt_bind_key(stmt_dup(ss), rk);
			cat_key_add_column(k, c, 0);
			res = stmt_create_key(k, rks);
			stmt_destroy(cs);
			stmt_destroy(ts);
		}
	} break;
	case SQL_NOT_NULL:
	case SQL_NULL:
		c->null = (s->token == SQL_NOT_NULL)?0:1;
		res = stmt_null(cs, c->null);
		stmt_destroy(ss);
		stmt_destroy(ts);
		break;
	}

	if (!res) {
		return sql_error(sql, 02, 
			 "Unknown constraint (%ld)->token = %s\n",
			 (long) s, token2string(s->token));
	}
	return res;
}

/* 
column_option: default | column_constraint ;
*/
static char *table_constraint_name( table *t )
{
	static char buf[BUFSIZ];
	int len = 1;
	if (t->keys) len += list_length(t->keys);
	snprintf(buf, BUFSIZ, "c%d", len );
	return buf;
}


static stmt *column_option(context * sql, symbol * s, stmt * ss, stmt * ts, stmt * cs, column *c)
{
	stmt *res = NULL;
	assert(cs);
	switch (s->token) {
	case SQL_CONSTRAINT:
		{
			dlist *l = s->data.lval;
			char *opt_name = l->h->data.sval;  
			symbol *sym = l->h->next->data.sym;
			if (!opt_name) 
				opt_name = table_constraint_name(c->table);
			res = column_constraint_type(sql, opt_name, sym, ss, ts, cs, c);
		}
		break;
	case SQL_ATOM: {
			AtomNode *an = (AtomNode*)s;
			if (!an || !an->a){
				res = stmt_default(cs, stmt_atom(atom_general(sql_dup_subtype(c->tpe),NULL)));
			} else {
				res = stmt_default(cs, stmt_atom(atom_dup(an->a)));
			}
			stmt_destroy(ss);
			stmt_destroy(ts);
		} break;
	}
	if (!res) {
		return sql_error(sql, 02, 
			 "Unknown column option (%ld)->token = %s\n",
			 (long) s, token2string(s->token));
	}
	return res;
}

static stmt *column_options(context * sql, dlist * opt_list, stmt *ss, stmt * ts, stmt * cs, column *c)
{
	assert(cs);
	if (opt_list) {
		dnode *n = NULL;
		for (n = opt_list->h; n && cs; n = n->next) {
			cs = column_option(sql, n->data.sym, stmt_dup(ss), stmt_dup(ts), cs, c);
		}
	}
	return cs;
}

static stmt *create_column(context * sql, symbol * s, stmt * ss, stmt * ts, table * table )
{
	catalog *cat = sql->cat;
	dlist *l = s->data.lval;
	char *cname = l->h->data.sval;
	sql_subtype *ctype = l->h->next->data.typeval;
	dlist *opt_list = l->h->next->next->data.lval;
	stmt *res = NULL;
	if (cname && ctype) {
		column *c = cat_create_column(cat, 0, table, cname, sql_dup_subtype(ctype), "NULL", 1);
		res = stmt_create_column(ts, c);
		c->s = stmt_dup(res);
		res = column_options(sql, opt_list, ss, ts, res, c);
	}

	if (!res) {
		return sql_error(sql, 02, "Create Column: type or name");
	}
	return res;
}

static stmt *table_foreign_key( context * sql, char *name, symbol * s, stmt * ss, stmt * ts, table * t )
{
	catalog *cat = sql->cat;
	stmt *res = NULL;
	dnode *n = s->data.lval->h;
	char *tname = qname_table(n->data.lval);
	table *ft = cat_bind_table( cat, t->schema, tname );

	if (!ft){
		return sql_error(sql, 02, "Table %s unknown\n", tname );
	} else {
		stmt *fts, *rks, *ks;
		key *k, *rk = cat_table_bind_pkey(ft);
		dnode *nms = n->next->data.lval->h;
		node *fnms;

		if (n->next->next->data.lval){ /* find unique referenced key */
			dnode *fnms = n->next->next->data.lval->h;
			list *cols = list_create(NULL);

			for( ;fnms; fnms = fnms->next)
				list_append(cols,fnms->data.sval);

			rk = cat_table_bind_ukey(ft, cols);
			list_destroy(cols);
		}
		if (!rk){
			return sql_error(sql, 02, "Could not find referenced unique key in table %s\n", ft->name );
		}
	       	k = cat_table_add_key(t, fkey, name, rk);
		fts = stmt_bind_table(stmt_dup(ss), ft);
		rks = stmt_bind_key(fts, rk);
		ks = stmt_create_key(k, rks);
		res = ks;

		for(fnms = rk->columns->h; nms && fnms; nms = nms->next, fnms = fnms->next){
			char *nm = nms->data.sval;
			column *c = cat_bind_column(cat, t, nm );

			if (!c){
				if (res) stmt_destroy(res);
				return sql_error(sql, 02, "Table %s has no column %s\n", t->name, nm);
			}
			cat_key_add_column(k, c, 0);
		}
		if (nms || fnms){
			if (res) stmt_destroy(res);
			return sql_error(sql, 02, "Not all colunms are handeled in the foreign key\n");
		}
	}
	return res;
}

static stmt *table_constraint_type( context * sql, char *name, symbol * s, stmt * ss, stmt *ts, table * t )
{
	catalog *cat = sql->cat;
	stmt *res = NULL;
	switch(s->token){
		case SQL_UNIQUE: 
		case SQL_PRIMARY_KEY:
		{
			key_type kt = (s->token==SQL_PRIMARY_KEY?pkey:ukey);
			key *k = cat_table_add_key(t, kt, name, NULL);
			stmt *ks = stmt_create_key(k, NULL);
			dnode *nms = s->data.lval->h;

			for(;nms; nms = nms->next){
				char *nm = nms->data.sval;
				column *c = cat_bind_column(cat, t, nm );

				if (!c){
					return sql_error(sql, 02, "Table %s has no column %s\n", t->name, nm);
				}
				cat_key_add_column(k, c, 0);
			}
			res = ks;
		} break;
		case SQL_FOREIGN_KEY:
			res = table_foreign_key( sql, name, s, ss, ts, t );
		 	break;
	}
	if (!res) {
		return sql_error(sql, 02, 
			 "Table Constraint Type: wrong token (%ld) = %s\n",
			 (long) s, token2string(s->token));
	}
	return res;
}

static stmt *table_constraint( context * sql, symbol * s, stmt *ss, stmt * ts, table * table) 
{
	stmt *res = NULL;

	if (s->token == SQL_CONSTRAINT){
		dlist *l = s->data.lval;
		char *opt_name = l->h->data.sval; 
		symbol *sym = l->h->next->data.sym;
		if (!opt_name) 
			opt_name = table_constraint_name(table);
		res = table_constraint_type(sql, opt_name, sym, ss, ts, table );
	}

	if (!res) {
		return sql_error(sql, 02, 
			 "Table Constraint: wrong token (%ld) = %s\n",
			 (long) s, token2string(s->token));
	}
	return res;
}

static stmt *table_element(context * sql, symbol * s, stmt *ss, stmt * ts, table *table)
{
	stmt *res = NULL;

	switch (s->token) {
	case SQL_COLUMN:
		res = create_column(sql, s, ss, stmt_dup(ts), table);
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
			stmt *cs = stmt_bind_column( stmt_dup(ts), c );

			assert(cs);
			if (!c){
				return sql_error(sql, 02, "Column %s not found\n", cname);
			} else {
				res = column_options(sql, olist, ss, ts, cs, c);
			}
		} break;
	}
	if (!res) {
		return sql_error(sql, 02, 
			 "Unknown table element (%ld)->token = %s\n",
			 (long) s, token2string(s->token));
	}
	return res;
}

static stmt *create_table(context * sql, schema * schema, stmt * ss,
			int temp, dlist * qname, dlist * columns)
{
	catalog *cat = sql->cat;
	char *name = qname_table(qname);

	if (cat_bind_table(cat, schema, name)) {
		stmt_destroy(ss);
		return sql_error(sql, 02, 
			 "Create Table name %s allready in use", name);
	} else {
		table_type tp = (temp)?tt_session:tt_base; 
		table *table = cat_create_table(cat, 0, schema, name, tp, NULL);
		stmt *ts = stmt_create_table(ss, table);
		list *newcolumns = list_append(create_stmt_list(), ts);
		dnode *n = columns->h;

		while (n) {
			list_append(newcolumns, table_element(sql, n->data.sym, ss, ts, table));
			n = n->next;
		}
		return stmt_list(newcolumns);
	}
}

static stmt *drop_table(context * sql, dlist * qname, int drop_action)
{
	stmt *res = NULL;
	char *tname = qname_table(qname);
	table *t = cat_bind_table(sql->cat, cur_schema(sql), tname);

	if (!t) {
		return sql_error(sql, 02, 
			 "Drop Table, table %s unknown", tname);
	} else if (t->type != tt_view){
		stmt *ss = stmt_bind_schema( cur_schema(sql) );
		res = stmt_drop_table(ss, t->name, drop_action);
		cat_drop_table(sql->cat, cur_schema(sql), tname);
	} else {
		return sql_error(sql, 02, 
			 "Drop Table, cannot drop view %s ", tname);
	}
	return res;
}

static stmt *alter_table(context * sql, schema *schema, stmt *ss,
		dlist * qname, symbol * te)
{
	catalog *cat = sql->cat;
	char *name = qname_table(qname);
	table *table = NULL;

	if ((table = cat_bind_table(cat, cur_schema(sql), name)) == NULL) {
		stmt_destroy(ss);
		return sql_error(sql, 02, 
			 "Alter Table name %s doesn't exist", name);
	} else {
		stmt *ts = stmt_bind_table(ss, table);
		stmt *c = create_column(sql, te, ss, ts, table );
		return c;
	}
}

static stmt *grant_roles(context * sql, schema *schema,
		dlist * roles, dlist * grantees, int admin, int grantor)
{
	/* grant roles to the grantees */
	dnode *r, *g;
	list *l = create_stmt_list();
	
	for( r = roles->h; r; r = r->next ){
		char *role = r->data.sval;
		for( g = grantees->h; g; g = g->next ){
			char *grantee = g->data.sval;
			list_append(l, stmt_grant_role(grantee, role) );
		}
	}
	return stmt_list(l);
}

static stmt *revoke_roles(context * sql, schema *schema,
		dlist * roles, dlist * grantees, int admin, int grantor)
{
	/* revoke roles from the grantees */
	dnode *r, *g;
	list *l = create_stmt_list();
	
	for( r = roles->h; r; r = r->next ){
		char *role = r->data.sval;
		for( g = grantees->h; g; g = g->next ){
			char *grantee = g->data.sval;
			list_append(l, stmt_revoke_role(grantee, role) );
		}
	}
	return stmt_list(l);
}


static stmt *create_schema(context * sql, dlist * auth_name, dlist * schema_elements)
{
	catalog *cat = sql->cat;
	char *name = schema_name(auth_name);
	char *auth = schema_auth(auth_name);

	if (auth == NULL) {
		auth = cur_schema(sql)->auth;
	}
	if (cat_bind_schema(cat, name)) {
		return sql_error(sql, 02, 
			"Create Schema name %s allready in use", name);
	} else {
		schema *schema = cat_create_schema(cat, 0, name, auth);
		stmt *st = stmt_create_schema(schema);
		list *schema_objects = list_append(create_stmt_list(), st);

		dnode *n = schema_elements->h;

		while (n) {
			if (n->data.sym->token == SQL_CREATE_TABLE) {
				dlist *l = n->data.sym->data.lval;
				st = create_table(sql, schema, stmt_dup(st),
						  l->h->data.ival,
						  l->h->next->data.lval,
						  l->h->next->next->data.
						  lval);
			} else if (n->data.sym->token == SQL_CREATE_VIEW) {
				dlist *l = n->data.sym->data.lval;
				st = create_view(sql, schema, stmt_dup(st),
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

static stmt *drop_schema(context * sql, dlist * qname, int dropaction)
{
	catalog *cat = sql->cat;
	char *name = schema_name(qname);
	schema *s = cat_bind_schema(cat, name);

	if (!s) {
		return sql_error(sql, 02, 
			 "Drop Schema name %s does not exist", name);
	} else {
		return stmt_drop_schema(s,dropaction);
	}
}


static stmt *copyfrom(context * sql, dlist * qname, dlist *files, dlist *seps, int nr)
{
	catalog *cat = sql->cat;
	char *tname = qname_table(qname);
	table *t = cat_bind_table(cat, cur_schema(sql), tname);
	char *tsep = seps->h->data.sval;
	char *rsep = seps->h->next->data.sval;
	list *filelist = NULL;

	if (!t) {
		return sql_error(sql, 02, 
				"Copy into non existing table %s", tname);
	} else if (files){
		dnode *n = files->h;
		for (filelist = create_string_list(); n; n = n->next) 
			list_append(filelist, _strdup(n->data.sval));
	}
	return stmt_copyfrom(t, filelist, tsep, rsep, nr);

}

static stmt *insert_value(context * sql, scope * scp, column * c, symbol * s)
{
	if (s->token == SQL_NULL) {
		return stmt_atom(atom_general(sql_dup_subtype(c->tpe), NULL));
	} else {
		stmt *n = NULL;
		stmt *a = sql_value_exp(sql, scp, s, NULL, NULL);
		if (!a || !(n = check_types(sql, c->tpe, a)))
			return NULL;
		return n;
	}
	return NULL;
}

static void cleanup_inserts(stmt **inserts, int cnt){
	int i;
	for (i=0; i<cnt; i++){
		if (inserts[i])
			stmt_destroy(inserts[i]);
	}
	_DELETE(inserts);
}

static stmt *sql_insert_check_keys(context * sql, tvar *tv, stmt **inserts, int len)
{
	char buf[BUFSIZ];
	/* int insert = 1;
	 * while insert and has u/pkey and not defered then
	 * 	if u/pkey values exist then
	 * 		insert = 0
	 * while insert and has fkey and not defered then
	 *	find id of corresponding u/pkey  
	 *	if (!found)
	 *		insert = 0
	 * if insert
	 * 	insert values
	 * 	insert fkey/pkey index
	 */
	if (isbasetable(tv->s)){
		table *t = basetable_table(tv->s);
		node *n;
		sql_subtype *it = sql_bind_localtype( "int" );

		if (t->keys) for(n = t->keys->h; n; n = n->next){
			node *m;
			key *k = n->data;
			stmt *s, *h = NULL;
			group *g = NULL;

			if (k->type != pkey && k->type != ukey)
				continue;

			if (list_length(k->columns) > 1){
				sql_aggr *cnt, *sum;
				sql_subtype *bt = sql_bind_localtype( "bit" );
				sql_func *ne = sql_bind_func_result("<>", it, it, NULL, bt);
				sql_subtype_destroy(bt);

			    	for(m = k->columns->h; m; m = m->next){
					kc *c = m->data;
					sql_func *hf = sql_bind_func_result("hash", c->c->tpe, NULL, NULL, it);
					if (h){
						sql_func *xor = sql_bind_func_result("xor", it, it, NULL, it);
						h = stmt_binop(h, 
						 stmt_unop(stmt_dup(inserts[c->c->colnr]->op2.stval), hf), xor);
					} else {
						h = stmt_unop(stmt_dup(inserts[c->c->colnr]->op2.stval), hf);
					}
			    	}
				s = stmt_select( stmt_kbat(k,0), h, cmp_equal);
			    	for(m = k->columns->h; m; m = m->next){
					kc *c = m->data;
					g = grp_create(
					      stmt_append(
					        stmt_semijoin(stmt_dup(inserts[c->c->colnr]->op1.stval),stmt_dup(s)),
						    stmt_dup(inserts[c->c->colnr]->op2.stval)),g);
				}
				stmt_destroy(s);
				cnt = sql_bind_aggr("count", NULL);
				s = stmt_aggr(stmt_dup(g->grp), grp_dup(g), cnt);
				/* find new oids as generated by append */
				s = stmt_diff(s, stmt_dup(inserts[0]->op1.stval));
				/* (count(s) <> sum(s)) */
				sum = sql_bind_aggr("sum", tail_type(s));
				s = stmt_binop( stmt_aggr(stmt_dup(s), NULL, cnt), check_types(sql, it, stmt_aggr(s, NULL, sum)) , ne);
				snprintf(buf, BUFSIZ, 
					"key constraint %s.%s failed", k->t->name, k->name ); 
				return stmt_exception( s, buf );  
			} else { /* single column key */
				kc *c = k->columns->h->data;
				h = stmt_dup(inserts[c->c->colnr]->op2.stval);	
				s = stmt_select( stmt_dup(inserts[c->c->colnr]->op1.stval), h, cmp_equal);
				/* s should be empty */
				snprintf(buf, BUFSIZ, 
					"key constraint %s.%s failed", k->t->name, k->name ); 
				return stmt_exception( stmt_count(s), buf); 
			}
		}
		sql_subtype_destroy(it);
	}
	return NULL;
}

static stmt *sql_insert(context * sql, tvar *tv, stmt **inserts, int len)
{
	int i; 
	list *l = create_stmt_list();

	stmt *ckeys = sql_insert_check_keys(sql, tv, inserts, len);

	if (ckeys) /* first check all the keys */
		list_append(l, ckeys);

	for (i = 0; i < len; i++) {
		if (!inserts[i])
			return NULL;
		list_append(l, inserts[i]);
	}
	return stmt_list(l);
}

static stmt *insert_into(context * sql, dlist * qname,
			 dlist * columns, symbol * val_or_q)
{
	scope *scp;
	tvar *tv = NULL;

	catalog *cat = sql->cat;
	char *tname = qname_table(qname);
	table *t = cat_bind_table(cat, cur_schema(sql), tname);
	list *collist = NULL;
	int i, len = 0;
	stmt **inserts, *res = NULL;

	if (!t) {
		return sql_error(sql, 02, 
			"Inserting into non existing table %s", tname);
	}
	if (columns) {
		dnode *n;

		collist = create_column_list();
		for (n = columns->h; n; n = n->next ) {
			column *c = cat_bind_column(cat, t, n->data.sval);
			if (c) {
				list_append(collist, c);
			} else {
				return sql_error(sql, 02, "Inserting into non existing column %s.%s", tname, n->data.sval);
			}
		}
	} else {
		collist = t->columns;
	}

	len = list_length(t->columns);
	inserts = NEW_ARRAY(stmt *, len);
	for (i = 0; i < len; i++)
		inserts[i] = NULL;

	scp = scope_open(NULL);
	tv = scope_add_table_columns(scp, t, t->name);

	if (val_or_q->token == SQL_VALUES) {
		dlist *values = val_or_q->data.lval;
		if (dlist_length(values) != list_length(collist)) {
			scp = scope_close(scp);
			return sql_error(sql, 02, 
				 "Inserting into table %s, number of values doesn't match number of columns", tname);
		} else {
			dnode *n;
			node *m;

			for (n = values->h, m = collist->h;
			     n && m; n = n->next, m = m->next) {
				column *c = m->data;
				stmt *ins = insert_value(sql, NULL, c, n->data.sym);
				if (!ins){
					cleanup_inserts(inserts, len);
					return NULL;
				}
					
				inserts[c->colnr] = stmt_append( stmt_cbat(c, stmt_dup(tv->s), INS, st_bat), ins );
			}

		}
		for(i=0; i<len; i++){
			if (!inserts[i]){
				node *m;
				for(m = t->columns->h; m; m = m->next){
					column *c = m->data;
					if (c->colnr == i)
						inserts[i] = stmt_append( stmt_cbat(c, stmt_dup(tv->s), INS, st_bat), stmt_atom(atom_general(sql_dup_subtype(c->tpe), _strdup(c->default_value))) );
				}
			}
		}
	} else {
		stmt *s = sql_subquery(sql, NULL, val_or_q);

		if (!s){
			scp = scope_close(scp);
			cleanup_inserts(inserts, len);
			return NULL;
		}
		if (list_length(s->op1.lval) != list_length(collist)) {
			if (s) 
				stmt_destroy(s);
			scp = scope_close(scp);
			cleanup_inserts(inserts, len);
			return sql_error(sql, 02, 
				 "Inserting into table %s, query result doesn't match number of columns", tname);
		} else {
			node *m, *n;

			for (n = s->op1.lval->h, m = collist->h;
			     n && m; n = n->next, m = m->next) {
				column *c = m->data;
				inserts[c->colnr] = stmt_append( stmt_cbat(c, stmt_dup(tv->s), INS, st_bat), stmt_dup(n->data));
			}
		}
		if (s) 
			stmt_dup(s);
	}
	res = sql_insert(sql, tv, inserts, len);
	scp = scope_close(scp);
	_DELETE(inserts);
	return res;
}

static stmt *sql_update(context * sql, dlist * qname,
			dlist * assignmentlist, symbol * opt_where)
{
	stmt *s = NULL;
	char *tname = qname_table(qname);
	table *t = cat_bind_table(sql->cat, cur_schema(sql), tname);

	if (!t) {
		return sql_error(sql, 02, "Updating non existing table %s", tname);
	} else {
		tvar *tv = NULL;
		dnode *n;
		list *l;
		scope *scp;

		scp = scope_open(NULL);
		tv = scope_add_table_columns(scp, t, t->name);

		if (opt_where){
			s = sql_logical_exp(sql, scp, opt_where, NULL, NULL);
			if (s) 
				s = stmt2pivot(sql,scp,s);
			if (!s){
				scope_close(scp);
			       	return NULL;
			}
		} else { /* update all */
			cvar *c = tv->columns->h->data;
			s = stmt2pivot(sql,scp,stmt_dup(c->s));
		}
	       	l = create_stmt_list();

		n = assignmentlist->h;
		while (n) {
			symbol *a = NULL;
			stmt *v = NULL;
			dlist *assignment = n->data.sym->data.lval;
			char *cname = assignment->h->data.sval;
			cvar *cl = scope_bind_column(scp, NULL, cname);
			if (!cl) {
				list_destroy(l);
				return sql_error(sql, 02, 
				  "Updating non existing column %s.%s",
					 tname, assignment->h->data.sval);
			}
			a = assignment->h->next->data.sym;
			v = sql_value_exp(sql, scp, a, NULL, s);

			if (!v)
				return NULL;

			v = check_types(sql, tail_type(cl->s), v);

			if (v->nrcols <= 0){
				v = stmt_const(
				  stmt_reverse(s ? first_subset(s) : stmt_dup(cl->s)), v);
			}

			list_append(l, stmt_replace( stmt_cbat(basecolumn(cl->s), stmt_dup(tv->s), UPD, st_bat), v));
			list_append(l, stmt_insert( stmt_cbat(basecolumn(cl->s), stmt_dup(tv->s), UPD, st_ubat), stmt_dup(v)));
			n = n->next;
		}
		if (s) stmt_destroy(s);
		scp = scope_close(scp);
		return stmt_list(l);
	}
	return NULL;
}

static stmt *sql_delete(context * sql, dlist * qname, symbol * opt_where)
{
	char *tname = qname_table(qname);
	table *t = cat_bind_table(sql->cat, cur_schema(sql), tname);

	if (!t) {
		return sql_error(sql, 02, 
			 "Deleting from non existing table %s", tname);
	} else {
		tvar *tv = NULL;
		node *n;
		stmt *v, *s = NULL;
		scope *scp;
		list *l = create_stmt_list();
		sql_subtype *to;
	       
		to	= sql_bind_subtype("OID", 0, 0);

		scp = scope_open(NULL);
		tv = scope_add_table_columns(scp, t, t->name);

		if (opt_where){
			s = sql_logical_exp(sql, scp, opt_where, NULL, NULL);
			if (s) 
				s = stmt2pivot(sql,scp,s);
			if (!s){
				scope_close(scp);
			       	return NULL;
			}
		} else { /* delete all */
			cvar *c = tv->columns->h->data;
			s = stmt2pivot(sql,scp,stmt_dup(c->s));
		}

		v = stmt_const( stmt_reverse(first_subset(s)), 
					stmt_atom(atom_general(to, NULL)));
		assert (isbasetable(tv->s));
		list_append(l, stmt_append(
				stmt_tbat(basetable_table(tv->s), INS, st_dbat),
				stmt_reverse(v)));
		/* switched of as its not used and insert first needs code
		 * to fill this bat 
		list_append(l, stmt_replace(
				stmt_tbat(basetable_table(tv->s), DEL, st_obat),
				stmt_dup(v)));
				*/
		for(n = t->columns->h; n; n = n->next){
			column *c = n->data;
			stmt *v = stmt_const( stmt_reverse( first_subset(s)), 
				stmt_atom(atom_general(sql_dup_subtype(c->tpe), NULL)));
			list_append(l, stmt_replace( stmt_cbat(c, stmt_dup(tv->s), DEL, st_bat), v));
		}
		scp = scope_close(scp);
		return stmt_list(l);
	}
	return NULL;
}

static stmt *sql_stmt(context * sql, symbol * s)
{
	schema *cur = cur_schema(sql);
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
			ret = create_schema(sql, 
				l->h->data.lval, /* qname (name.authid) */
				l->h->next->next->next->data.lval /* schema_elements */);
		}
		break;
	case SQL_DROP_SCHEMA:
		{
			dlist *l = s->data.lval;
			ret = drop_schema(sql, 
				l->h->data.lval, /* qname */
				l->h->next->next->data.ival); /* drop_action */
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
			ret = drop_view(sql, l);
		}
		break;
	case SQL_CREATE_TABLE:
		{
			
			stmt *ss = stmt_bind_schema(cur);
			dlist *l = s->data.lval;
			ret = create_table(sql, cur, ss,
					   l->h->data.ival,
					   l->h->next->data.lval,
					   l->h->next->next->data.lval);
		}
		break;
	case SQL_CREATE_VIEW:
		{
			stmt *ss = stmt_bind_schema(cur);
			dlist *l = s->data.lval;
			ret = create_view(sql, cur, ss,
					  l->h->data.lval,
					  l->h->next->data.lval,
					  l->h->next->next->data.sym,
					  l->h->next->next->next->data.
					  ival);
		}
		break;
	case SQL_CREATE_ROLE:
		{
			dlist *l = s->data.lval;
			ret = create_role(sql, cur, 
				  l->h->data.lval, /* role name */
				  l->h->next->data.ival); /* role grantor */
		}
		break;
	case SQL_DROP_ROLE:
		{
			dlist *l = s->data.lval;
			ret = drop_role(sql, cur, l); /* role name */
		}
		break;
	case SQL_ALTER_TABLE:
		{
			stmt *ss = stmt_bind_schema(cur);
			dlist *l = s->data.lval;
			ret = alter_table(sql, cur, ss,
					l->h->data.lval,	/* table name */
					l->h->next->data.sym);	/* table element */
		}
		break;
	case SQL_GRANT_ROLES:
		{
			dlist *l = s->data.lval;
			ret = grant_roles(sql, cur, 
			    l->h->data.lval,	/* authids */
			    l->h->next->data.lval,	/* grantees */
			    l->h->next->next->data.ival, /* admin? */
			    l->h->next->next->next->data.ival); /* grantor */
		}
		break;
	case SQL_GRANT:
	case SQL_REVOKE:
		printf("%s\n", s->token == SQL_GRANT?"grant":"revoke");
		break;

	case SQL_COPYFROM:
		{
			dlist *l = s->data.lval;
			ret = copyfrom(sql, l->h->data.lval,
				       l->h->next->data.lval,
				       l->h->next->next->data.lval,
				       l->h->next->next->next->data.ival);
		}
		break;
	case SQL_INSERT:
		{
			dlist *l = s->data.lval;
			ret = insert_into(sql,
					  l->h->data.lval,
					  l->h->next->data.lval,
					  l->h->next->next->data.sym);
		}
		break;
	case SQL_UPDATE:
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
			ret = sql_delete(sql, 
					 l->h->data.lval,
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
		return sql_error(sql, 01, 
			 "sql_stmt Symbol(%ld)->token = %s",
			 (long) s, token2string(s->token));
	}
	return ret;
}


stmt *semantic(context * s, symbol * sym)
{
	stmt *res = NULL;

	if (sym){
		res = sql_stmt(s, sym);
	}
	return res;
}


/* TODO
   1) fix error output 
   	1) (f)printf -> snprintf(sql->errstr, ERRSZE, "etc" )
	2) use (x)gettext to internationalize this
   2) cleanup while(n) -> for( n = ; n; n = n->next)

   3) remove list_map/traverse

   4) code review.

   5) may need levels of errors/warnings so we can skip non important once
      add sqlstate to errors
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

char *toLower( char *v ){
	char *s = v;
	while(*v){
		*v = (char)tolower(*v);
		v++;
	}
	return s;
}

char *removeQuotes( char *v, char c ){
	char *s = NEW_ARRAY(char, strlen(v)), *n = s;
	while(*v && *v != c) v++;
	v++; /* skip \' */
	while(*v && *v != c) *n++ = *v++;
	*n = '\0';
	return s;	
}

char *addQuotes( char *s ){
        int l = strlen(s);
        char *ns = NEW_ARRAY(char, l+3), *n = ns;
        *ns++ = '"';
        strncpy(ns, s, l);
        ns += l;
        *ns++ = '"';
        *ns = '\0';
        return n;
}

char *strconcat( const char *s1, const char *s2 ){
	int i,j,l1 = strlen(s1);
	int l2 = strlen(s2)+1;
	char *new_s = NEW_ARRAY(char,l1+l2);
	for ( i=0; i<l1; i++){
		new_s[i] = s1[i];
	}
	for ( j=0; j<l2; j++, i++){
		new_s[i] = s2[j];
	}
	return new_s;
}

char *token2string(int token){
	switch(token){
	case SQL_CREATE_SCHEMA: return "Create Schema";
	case SQL_CREATE_TABLE: return "Create Table";
	case SQL_CREATE_VIEW: return "Create View";
	case SQL_DROP_SCHEMA: return "Drop Schema";
	case SQL_DROP_TABLE: return "Drop Table";
	case SQL_DROP_VIEW: return "Drop View";
	case SQL_ALTER_TABLE: return "Alter Table";
	case SQL_NAME: return "Name";
	case SQL_USER: return "User";
	case SQL_PATH: return "Path";
	case SQL_CHARSET: return "Char Set";
	case SQL_TABLE: return "Table";
	case SQL_COLUMN: return "Column";
	case SQL_COLUMN_OPTIONS: return "Column Options";
	case SQL_CONSTRAINT: return "Constraint";
	case SQL_CHECK: return "Check";
	case SQL_DEFAULT: return "default";
	case SQL_NOT_NULL: return "Not Null";
	case SQL_NULL: return "Null";
	case SQL_UNIQUE: return "Unique";
	case SQL_PRIMARY_KEY: return "Primary Key";
	case SQL_FOREIGN_KEY: return "Foreign Key";
	case SQL_COMMIT: return "Commit";
	case SQL_ROLLBACK: return "Rollback";
	case SQL_SELECT: return "Select";
	case SQL_WHERE: return "Where";
	case SQL_FROM: return "From";
	case SQL_UNION: return "Union";
	case SQL_UPDATE_SET: return "Update Set";
	case SQL_INSERT_INTO: return "Insert Into";
	case SQL_INSERT: return "Insert";
	case SQL_DELETE: return "Delete";
	case SQL_VALUES: return "Values";
	case SQL_ASSIGN: return "Assignment";
	case SQL_ORDERBY: return "Order By";
	case SQL_GROUPBY: return "Group By";
	case SQL_DESC: return "Desc";
	case SQL_AND: return "And";
	case SQL_OR: return "Or";
	case SQL_EXISTS: return "Exists";
	case SQL_NOT_EXISTS: return "Not Exists";
	case SQL_UNOP: return "Unop";
	case SQL_BINOP: return "Binop";
	case SQL_BETWEEN: return "Between";
	case SQL_NOT_BETWEEN: return "Not Between";
	case SQL_LIKE: return "Like";
	case SQL_NOT_LIKE: return "Not Like";
	case SQL_IN: return "In";
	case SQL_NOT_IN: return "Not In";
	case SQL_GRANT: return "Grant";
	case SQL_PARAMETER: return "Parameter";
	case SQL_AGGR: return "Aggregates";
	case SQL_COMPARE: return "Compare";
	case SQL_TEMP_LOCAL: return "Local Temporary";
	case SQL_TEMP_GLOBAL: return "Global Temporary";
	case SQL_INT_VALUE: return "Integer";
	case SQL_ATOM: return "Atom";
	case SQL_ESCAPE: return "Escape";
	case SQL_CAST: return "Cast";
	case SQL_CASE: return "Case";
	case SQL_WHEN: return "When";
	case SQL_COALESCE: return "Coalesce";
	case SQL_NULLIF: return "Nullif";
	case SQL_JOIN: return "Join";
	case SQL_CROSS: return "Cross";
	default: return "unknown";
	}
}


static statement *sql_select( context *sql, scope *scp, int distinct, 
		dlist *selection, dlist *into, dlist *table_exp, 
		symbol *orderby );
static statement *sql_simple_select( context *sql, scope *scp,  
		dlist *selection );
static statement *search_condition( context *sql, scope *scp, symbol *sc, 
		statement *group, statement *subset );
static statement *scalar_exp( context *sql, scope *scp, symbol *se, 
		statement *group, statement *subset	);

static statement *pearl2pivot( context *sql, list *ll );
static statement *diamond2pivot( context *sql, list *l );
static var *query_exp_optname( context *sql, scope *scp, symbol *q );

statement *subquery( context *sql, scope *scp, symbol *sq ) {
	dlist *q = sq->data.lval;
	assert( sq->token == SQL_SELECT );
	return 
	sql_select( sql, scp,
		  q->h->data.ival, 
		  q->h->next->data.lval,
	          q->h->next->next->data.lval, 
	          q->h->next->next->next->data.lval, 
	          q->h->next->next->next->next->data.sym );
}


static 
statement *column_ref( context *sql, scope *scp, symbol *column_r ){
	dlist *l = column_r->data.lval;
	assert (column_r->token == SQL_COLUMN && column_r->type == type_list);

	if (dlist_length(l) == 1){
		char *name = l->h->data.sval;
		var *tv = NULL;
		column *c = scope_bind_column( scp, name, &tv );

		if (!c){
			statement *sc = scope_bind_statement( scp, name );
			if (sc) return sc;
		}
		if (!c){
			snprintf(sql->errstr, ERRSIZE, 
		  		_("Column: %s unknown"), name );
			return NULL;
		}
		return statement_column(c, tv);
	} else if (dlist_length(l) == 2){
		char *tname = l->h->data.sval;
		char *cname = l->h->next->data.sval;
		var *tv = NULL;
		column *c = scope_bind_table_column( scp, tname, cname, &tv);
		if (tv){
			return statement_column(c, tv); 
		} else {
			snprintf(sql->errstr, ERRSIZE, 
		  	_("Column: %s.%s unknown"), tname, cname );
			return NULL;
		}
	} else if (dlist_length(l) >= 3){
		snprintf(sql->errstr, ERRSIZE, 
		  	_("TODO: column names of level >= 3\n") );
	}
	return NULL;
}

static
char *table_name( dlist *tname ){
	assert(tname && tname->h);

	if (dlist_length(tname) == 1){
		return tname->h->data.sval;
	}
	if (dlist_length(tname) == 2)
		return tname->h->next->data.sval;
	return "Unknown";
}

static
table *create_table_intern( context *sql, schema *schema, 
		char *name, char *query, statement *sq ){

	catalog *cat = sql->cat;
	table *table = cat_create_table( cat, 0, schema, name, 0, query);

	int seqnr = 0;
	node *m = sq->op1.lval->h;

	while(m){
		statement *st = m->data.stval;
		char *cname = column_name(st);
		char *ctype = tail_type(st)->sqlname;
		column *col = cat_create_column( cat, 0,
				table, cname, ctype, "NULL", 1, seqnr);
		col->s = st;
		st_incref(st);
		m = m->next;
		seqnr++;
	}
	return table;
}

static
char *temp_name( context *sql, scope *scp, char *name ){
	static int temp_nr = 1;

	snprintf(name, NAMELEN, "temp_t%d", temp_nr++);
	return name;
}

static
var *table_optname( context *sql, scope *scp, statement *sq,
		    char *query, symbol *optname ){
	char temp[NAMELEN+1];
	char *tname = NULL;
	dlist *columnrefs = NULL;
	var *v;
	table *tab;

	if (optname && optname->token == SQL_NAME ){
		tname = optname->data.lval->h->data.sval;
		columnrefs = optname->data.lval->h->next->data.lval;
	} else {
		tname = temp_name(sql, scp, temp );
	}
	tab = create_table_intern(sql, sql->cat->cur_schema, tname, query, sq );

	v = scope_add_table( scp, tab, tname );
	if (columnrefs){
		node *m = sq->op1.lval->h;
		dnode *d;

		for(d = columnrefs->h; d && m; d = d->next, m = m->next){
			statement *st = m->data.stval;
			scope_add_statement( scp, st, d->data.sval );
			st->h = v; /* Hack to fix ref to temp table */
		}
	}
	return v;
}

static
var *subquery_optname( context *sql, scope *scp, symbol *query ){
	statement *sq = subquery(sql, scp, query );

	if (!sq) return NULL; 

	return table_optname( sql, scp, sq, query->sql, 
			      query->data.lval->t->data.sym );
}

static
var *table_ref( context *sql, scope *scp, symbol *tableref ){
	char *tname = NULL;
	table *t = NULL;

	/* todo handle opt_table_ref 
	   	(ie tableref->data.lval->h-Next->data.sym */

	if (tableref->token == SQL_NAME){
		tname = table_name(tableref->data.lval->h->data.lval);
		t = cat_bind_table(sql->cat, sql->cat->cur_schema, tname);
		if (!t){  
			snprintf(sql->errstr, ERRSIZE, 
					_("Unknown table %s"), tname );
			return NULL;
		}
		if (tableref->data.lval->h->next->data.sym){ /* AS */
			tname = tableref->data.lval->h->next->data.sym->data.lval->h->data.sval;
		} 
		return scope_add_table( scp, t, tname );
	} else if (tableref->token == SQL_SELECT) {
		return subquery_optname( sql, scp, tableref );
	} else { 
		return query_exp_optname( sql, scp, tableref );
	}
}

static
char *schema_name( dlist *name_auth ){
	assert(name_auth && name_auth->h);

	return name_auth->h->data.sval;
}

static
char *schema_auth( dlist *name_auth ){
	assert(name_auth && name_auth->h && dlist_length(name_auth) == 2);

	return name_auth->h->next->data.sval;
}

static
statement *find_subset( statement *subset, var *t ){
	node *n = subset->op1.lval->h;
	if (t) while(n){
		statement *s = n->data.stval;
		if (s->t == t){
			return s;
		}
		n = n->next;
	}
	return NULL;
}

static
statement *first_subset( statement *subset ){
	node *n = subset->op1.lval->h;
	if (n) return n->data.stval;
	return NULL;
}

static
statement *check_types( context *sql, type *ct, statement *s ){
	type *st = tail_type(s);

	if (st){
		type *t = st;

		/* check if the implementation types are the same */
		while( t && strcmp(t->name, ct->name ) != 0 ){
			t = t->cast;
		}
		if (!t){ /* try to convert if needed */
			func *c = cat_bind_func_result(sql->cat, 
			  "convert", st->sqlname, NULL, NULL,
			  ct->sqlname );
			if (c) return statement_unop( s, c );
		}
		if( !t || strcmp(t->name, ct->name ) != 0 ){
			snprintf(sql->errstr, ERRSIZE, 
	 		_("Types %s and %s are not equal" ), 
	 		st->sqlname, ct->sqlname);
			return NULL;
		} else if (t != st){

			func *c = cat_bind_func_result(sql->cat, 
			  "convert", st->sqlname, NULL, NULL,
			  ct->sqlname );

			if (!c){
				snprintf(sql->errstr, ERRSIZE, 
	 			_("Missing convert function from %s to %s" ), 
	 			st->sqlname, ct->sqlname);
			       	return NULL;
			}
			return statement_unop( s, c );
		}
	} else {
		snprintf(sql->errstr, ERRSIZE, 
				_("Statement has no type information" ));
		return NULL;
	}
	return s;
}


/* The case/when construction in the selection works on the resulting
   table (ie. on the marked columns). We just need to know which oid list
   is involved (ie. find one subset).
   We need to check if for all results the types are the same. 
 */
statement *sql_search_case( context *sql, scope *scp, dlist *when_search_list,
		symbol *opt_else, statement *group, statement *subset	){
	list *conds = list_create();
	list *results = list_create();
	dnode *dn = when_search_list->h;
	type *restype = NULL;
	statement *res = NULL;
	node *n,*m;

	if (dn){
		dlist *when = dn->data.sym->data.lval;
		statement *cond, *result;

	       	cond = search_condition( sql, scp, when->h->data.sym, 
				group, subset);
		result = scalar_exp( sql, scp, when->h->next->data.sym, 
				group, subset);
		if (!cond || !result){
			list_destroy(conds);
			list_destroy(results);
		       	return NULL;
		}
		list_prepend_statement(conds, cond );
		list_prepend_statement(results, result );

		restype = tail_type(result);
	}
	if (!restype){
		snprintf(sql->errstr, ERRSIZE, 
			_("Error: result type missing"));
		list_destroy(conds);
		list_destroy(results);
		return NULL;
	}
	for(dn = dn->next; dn; dn = dn->next){
		type *tpe = NULL;
		dlist *when = dn->data.sym->data.lval;
		statement *cond, *result;

	       	cond = search_condition( sql, scp, when->h->data.sym, 
				group, subset);
		result = scalar_exp( sql, scp, when->h->next->data.sym, 
				group, subset);
		if (!cond || !result){
			list_destroy(conds);
			list_destroy(results);
		       	return NULL;
		}
		list_prepend_statement(conds, cond );
		list_prepend_statement(results, result );

		tpe = tail_type(result);
		if (!tpe){
			snprintf(sql->errstr, ERRSIZE, 
				_("Error: result type missing"));
			list_destroy(conds);
			list_destroy(results);
			return NULL;
		}
		if (restype != tpe){
		    snprintf(sql->errstr, ERRSIZE, 
		    _("Error: result types %s,%s of case are not compatible"),
		    restype->sqlname, tpe->sqlname );
		    list_destroy(conds);
		    list_destroy(results);
		    return NULL;
		}
	}
	if (subset){
		res = first_subset(subset);
		if (!res){
			snprintf(sql->errstr, ERRSIZE, 
	  		_("Subset not found") );
		    	list_destroy(conds);
		    	list_destroy(results);
		       	return NULL;
		}
	} else {
		printf("Case in query not handled jet\n");
	}
	if (opt_else){
		statement *result = 
			scalar_exp( sql, scp, opt_else, group, subset);
		type *t = tail_type(result);
		if (restype != t){
			func *c = cat_bind_func_result(sql->cat, "convert",
				t->sqlname, NULL, NULL, restype->sqlname );
			if (!c){
				snprintf(sql->errstr, ERRSIZE, 
		  		_("Cast between (%s,%s) not possible"), 
				t->sqlname, restype->sqlname);
				return NULL;
			}
			result = statement_unop( result, c );
		}
		if (result->nrcols <= 0){
			res = statement_const( res, result );
		} else {
			res = result;
		}
	} else {
		res = statement_const( res, NULL );
	}
	for(n = conds->h, m = results->h; n && m; n = n->next, m = m->next){
		statement *cond = n->data.stval;
		statement *result = m->data.stval;
		
		/* need more semantic tests here */
		if (cond->type == st_pearl){
			node *k = cond->op1.lval->h; 
			statement *cur = NULL;

			if (k){ 
				cur = k->data.lval->h->data.stval;
				k = k->next;
				for( ;k ; k = k->next){
					statement *st = 
						k->data.lval->h->data.stval;
					cur = statement_union( cur, st );
				}
				cond = cur;
			}
		}
		if (result->nrcols <= 0)
			result = statement_const( cond, result );
		else
			result = statement_semijoin( result, cond);
		res = statement_update( res, result);
	}
	list_destroy(conds);
	list_destroy(results);
	return res;
}

statement *sql_case( context *sql, scope *scp, symbol *se, statement *group,
	      statement *subset	){
	dlist *l = se->data.lval;
	if (l->h->type == type_list){
		dlist *when_search_list = l->h->data.lval;
		symbol *opt_else = l->h->next->data.sym;
		return sql_search_case(sql, scp, when_search_list, opt_else,
					group, subset );
	} else {
		/*sql_value_case();*/
		printf("sql_value_case not handeled\n");
		return NULL;
	}
	printf("case %d %s\n", se->token, token2string(se->token));
	return NULL;
}

statement *sql_cast( context *sql, scope *scp, symbol *se, statement *group,
	      statement *subset	){

	dlist *dl = se->data.lval;
	symbol *s = dl->h->data.sym;
	char *tpe = dl->h->next->data.sval;

	statement *l = scalar_exp( sql, scp, s, group, subset );

	if (l){
		type *st = tail_type(l);
		type *rt = cat_bind_type(sql->cat, tpe );
		func *c = cat_bind_func_result(sql->cat, "convert",
				st->sqlname, NULL, NULL, rt->sqlname );

		if (!c){
			snprintf(sql->errstr, ERRSIZE, 
		  	_("CAST operator: cast(%s,%s) unknown"), 
			st->sqlname, tpe );
			statement_destroy(l);
			return NULL;
		}
		return statement_unop( l, c );
	}
	return NULL;
}

static
statement *scalar_exp( context *sql, scope *scp, symbol *se, statement *group,
	      statement *subset	){

	switch(se->token){
	case SQL_TRIOP: { 
		dnode *l = se->data.lval->h;
		statement *ls = scalar_exp( sql, scp, 
				l->next->data.sym, group, subset);
		statement *rs1 = scalar_exp( sql, scp, 
				l->next->next->data.sym, group, subset);
		statement *rs2 = scalar_exp( sql, scp, 
				l->next->next->next->data.sym, group, subset);
		func *f = NULL;
		if (!ls || !rs1 || !rs2 ) return NULL;
		f = cat_bind_func(sql->cat, l->data.sval,
				tail_type(ls)->sqlname, 
				tail_type(rs1)->sqlname, 
				tail_type(rs2)->sqlname);
		if (f){
			return statement_triop( ls, rs1, rs2, f );
		} else {
			snprintf(sql->errstr, ERRSIZE, 
		  	_("operator: %s(%s,%s,%s) unknown"), 
			l->data.sval, tail_type(ls)->sqlname, 
			tail_type(rs1)->sqlname, tail_type(rs2)->sqlname);
		}
	} break;
	case SQL_BINOP: { 
		dnode *l = se->data.lval->h;
		statement *ls = scalar_exp( sql, scp, l->next->data.sym, 
						group, subset);
		statement *rs = scalar_exp( sql, scp, l->next->next->data.sym, 
						group, subset);
		func *f = NULL;
		if (!ls || !rs) return NULL;
		f = cat_bind_func(sql->cat, l->data.sval,
				tail_type(ls)->sqlname, 
				tail_type(rs)->sqlname, NULL);
		if (f){
			return statement_binop( ls, rs ,f );
		} else {
			func *c = NULL;
			int w = 0;
			type *t1 = tail_type(ls);
			type *t2 = tail_type(rs);
			if (t1->nr > t2->nr){
				type *s = t1;
				t1 = t2;
				t2 = s;
				w = 1;
			}
			c = cat_bind_func_result(sql->cat, "convert",
				t1->sqlname, NULL, NULL, t2->sqlname );
			f = cat_bind_func(sql->cat, l->data.sval,
				t2->sqlname, t2->sqlname, NULL );

			if (f && c){
				if (!w){
					ls = statement_unop( ls, c );
				} else {
					rs = statement_unop( rs, c );
				}
				return statement_binop( ls, rs ,f );
			} else { 
				snprintf(sql->errstr, ERRSIZE, 
		  		_("Binary operator: %s(%s,%s) unknown"), 
				l->data.sval, tail_type(ls)->sqlname, 
				tail_type(rs)->sqlname);
			}
		}
	} break;
	case SQL_UNOP: {
		dnode *l = se->data.lval->h;
		func *f = NULL;
		statement *rs = scalar_exp( sql, scp, l->next->data.sym, 
						group, subset);
		if (!rs) return NULL;
		f = cat_bind_func(sql->cat, l->data.sval, 
				tail_type(rs)->sqlname, NULL, NULL);
		if (f){
			return statement_unop( rs, f );
		} else {
			snprintf(sql->errstr, ERRSIZE, 
		  		_("Unary operator: %s(%s) unknown"), 
					l->data.sval, tail_type(rs)->sqlname);
		}
	} break;
	case SQL_COLUMN: {
		statement *res = column_ref( sql, scp, se );
		if (res && res->h && subset){
			statement *foundsubset = find_subset(subset, res->h);

			if (!foundsubset){
				snprintf(sql->errstr, ERRSIZE, 
		  		_("Subset not found") );
			       	return NULL;
			}
			res = statement_join(foundsubset, res, cmp_equal);
		} 
		return res;
	} break;
	case SQL_SELECT: {
		statement *sq = subquery( sql, scp, se );
		statement *res = NULL;

		if (sq){
			int l = list_length(sq->op1.lval);
			if (l <= 0 || l > 1){
				snprintf(sql->errstr, ERRSIZE, 
		  		_("Subqueries should return a single column") );
			       	return NULL;
			} else {
				res = sq->op1.lval->h->data.stval;
			}
		}
		return res;
	} break;
	case SQL_AGGR: {
		dlist *l = se->data.lval;
		aggr *a = NULL;
		int distinct = l->h->next->data.ival;
		statement *s = NULL;
		if (!l->h->next->next->data.sym){ /* count(*) case */
			if (group){
			  a = cat_bind_aggr(sql->cat, l->h->data.sval, NULL);
			  return statement_aggr(group, a, group);
			}
        		s = scope_first_column( scp );
			if (s && subset){
				statement *foundsubset = 
					find_subset(subset, s->h);

				s = statement_join(foundsubset, s, cmp_equal);
			} 
		} else {
		    	s = scalar_exp( sql, scp, l->h->next->next->data.sym, 
					group, subset);
		}
		if (!s) return NULL;

		if (s && distinct) s = statement_unique(s);
		if (!s) return NULL;
		a = cat_bind_aggr(sql->cat, l->h->data.sval, 
				  	tail_type(s)->sqlname );
		if (a){
			if (group){
			  return statement_aggr(s, a, group);
			} else {
			  return statement_aggr(s, a, NULL);
			}
		} else {
			snprintf(sql->errstr, ERRSIZE, 
		  		_("Aggregate: %s(%s) unknown"), 
				l->h->data.sval, tail_type(s)->sqlname );
		}
		return NULL;
	} break;
		 /* atom's */
	case SQL_PARAMETER:
			printf("parameter not used\n");
			break;
	case SQL_ATOM:
			return statement_atom( atom_dup(se->data.aval) );
	case SQL_CAST: 
			return sql_cast( sql, scp, se, group, subset );
	case SQL_CASE: 
			return sql_case( sql, scp, se, group, subset );
	case SQL_NULLIF:
	case SQL_COALESCE:
			printf("case %d %s\n", se->token, token2string(se->token));
			return NULL;
			break;
	default: 	
			printf("unknown %d %s\n", se->token, token2string(se->token));
	}
	return NULL;
}

static statement *sql_join
( 
	context *sql, 
	scope *scp, 
	symbol *tab1, 
	int natural, 
	jt jointype, 
	symbol *tab2, 
	symbol *js
){
  	statement *s = NULL, *subset = NULL;
	var *tv1, *tv2;
	
  	scp = scope_open( scp );

	tv1 = table_ref( sql, scp, tab1);
	tv2 = table_ref( sql, scp, tab2);

	if (!tv1 || !tv2) return NULL;

	if (js && js->token != SQL_USING){ /* On search_condition */
		s = search_condition(sql, scp, js, NULL, NULL);
	} else if (js){ /* using */
	} else { /* ! js */
	}

  	if (s){
		if (s->type != st_diamond && s->type != st_pearl){
			s = statement_diamond(s);
		}
		if (s->type == st_pearl){
			statement *ns = pearl2pivot(sql, s->op1.lval);
			statement_destroy(s);
			s = ns;
		} else {
		  	statement *ns = diamond2pivot(sql, s->op1.lval);
			statement_destroy(s);
			s = ns;
		}
		if (list_length(scp->lifted) > 0){
			/* possibly lift references */
	  		snprintf(sql->errstr, ERRSIZE, 
			_("Outer join with outer reference not implemented\n"));
			return NULL;
		}
  	}
  	subset = s;
	if (subset){
        	list *rl = list_create();
	    	table *t = NULL;
		node *n;
		statement *fs1 = find_subset(subset, tv1);
		statement *fs2 = find_subset(subset, tv2);
		statement *ld = NULL, *lc = NULL;
		statement *rd = NULL, *rc = NULL;

		if(!fs1 || !fs2) return NULL;

		t = tv1->data.tval;
		if (jointype == jt_left || jointype == jt_full){
			column *cs = t->columns->h->data.cval;
			/* we need to add the missing oid's */
			ld = statement_diff( 
				statement_column(cs,tv1), 
				   statement_reverse(subset) );
			lc = statement_count( ld );
		}
		t = tv2->data.tval;
		if (jointype == jt_right || jointype == jt_full){
			column *cs = t->columns->h->data.cval;
			/* we need to add the missing oid's */
			rd = statement_diff( 
				statement_column(cs,tv2), 
				   statement_reverse(subset) );
			rc = statement_count( rd );
		}
		if (jointype == jt_full){
			statement *lnil = statement_const(rd,statement_atom(NULL));
			statement *rnil = statement_const(ld,statement_atom(NULL));
			ld = statement_insert_column( ld,
			       statement_remark( lnil, 
				 statement_count(ld), 0));
			rd = statement_insert_column( rnil,
			       statement_remark( rd, 
				 statement_count(rnil), 0));
		} else {
			if (ld){
				ld = statement_mark(statement_reverse(ld),0);
				rd = statement_const(ld,statement_atom(NULL));
			} else if (rd){
				rd = statement_mark(statement_reverse(rd),0);
				ld = statement_const(rd,statement_atom(NULL));
			}
		}
		t = tv1->data.tval;
	    	for(n = t->columns->h; n; n = n->next){
			column *cs = n->data.cval;

			  list_append_statement(rl, 
			    statement_join(fs1, 
				statement_column(cs, tv1), cmp_equal));
	    	}
		t = tv2->data.tval;
	    	for(n = t->columns->h; n; n = n->next){
			column *cs = n->data.cval;

			  list_append_statement(rl, 
			    statement_join(fs2, 
				statement_column(cs, tv2), cmp_equal));
	    	}
		s = statement_list(rl);
	}
	scope_close(scp);
	return s;
}

static
statement *query_exp( context *sql, scope *scp, symbol *q ){
	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym; 
	int natural = n->next->data.ival;
	jt jointype = (jt)n->next->next->data.ival;
	symbol *tab_ref2 = n->next->next->next->data.sym;
	symbol *joinspec = n->next->next->next->next->data.sym;

	return sql_join( sql, scp,
	    tab_ref1, natural, jointype, tab_ref2, joinspec);
}

static 
var *query_exp_optname( context *sql, scope *scp, symbol *q ){
	/* 
	 * todo handle SQL_UNION (opt_all) (left/right should
	 * be added to parser 
	 */
	switch(q->token){
	case SQL_JOIN: { 
		statement *tq = query_exp( sql, scp, q );
		return table_optname( sql, scp, 
				tq, q->sql, q->data.lval->t->data.sym);
	}
	case SQL_CROSS:
		printf("implement crosstables %d %s\n", q->token, token2string(q->token));
	default:
		printf("case %d %s\n", q->token, token2string(q->token));
	}
	return NULL;
}

static
statement *column_exp( context *sql, scope *scp, symbol *column_e, statement *group, statement *subset ){

	switch(column_e->token){
	case SQL_TABLE: { /* table.* */

		char *tname = column_e->data.lval->h->data.sval;
		var *tv = scope_bind_table( scp, tname ); 

		if (group) group = statement_reverse(statement_unique(group));

		/* needs more work ???*/
		if (tv){
			statement *foundsubset = find_subset(subset,tv);
			list *columns = list_create();
			node *n = tv->data.tval->columns->h; 
			if (group) foundsubset = 
				statement_join( group, foundsubset, cmp_equal );
			while(n){
				list_append_statement(columns, 
				  statement_join(foundsubset,
				    statement_column(n->data.cval, tv),
					cmp_equal) );
				n = n->next;
			}
			return statement_list(columns);
		}
	} break;
	case SQL_COLUMN:
	{
		dlist *l = column_e->data.lval;
		statement *s = scalar_exp(sql, scp, l->h->data.sym, 
							group, subset);
		if (!s) return NULL;

		if (group && s->type != st_aggr){
			group = statement_reverse(statement_unique(group));
			s = statement_join( group, s, cmp_equal );
		}

		/* AS name */
		if (s && l->h->next->data.sval){
			s = statement_name(s, l->h->next->data.sval);
			scope_add_statement( scp, s, l->h->next->data.sval);
		} 
		return s;
	} break;
	/*
	case SQL_SELECT: {
		statement *sq = subquery(sql, scp, column_e );
		return sq;
	} break;
	*/
	default:
		snprintf(sql->errstr, ERRSIZE, 
		  _("Column expression Symbol(%d)->token = %s"),
		  (int)column_e->token, token2string(column_e->token));
	}
	snprintf(sql->errstr, ERRSIZE, 
	  _("Column expression Symbol(%d)->token = %s no output"),
	  (int)column_e->token, token2string(column_e->token));
	return NULL;
}

list *list_map_merge( list *l2, int seqnr, list *l1 ){
	return list_merge(l1, l2 );
}
list *list_map_append_list( list *l2, int seqnr, list *l1 ){
	return list_append_list(l1, l2 );
}

static
list *query_and( catalog *cat, list *ands ){
	list *l = NULL;
	node *n = ands->h;
	int len = list_length(ands), changed = 0;

	while(n && (len > 0)){ 
		/* the first in the list changes every interation */
	    	statement *s = n->data.stval;
		node *m = n->next;
	    	l = list_create();
		while(m){
			statement *olds = s;
			statement *t = m->data.stval;
			if (s->nrcols == 1){
				var *sv = s->h;

				if (t->nrcols == 1 && sv == t->h){
					s = statement_semijoin(s,t);
				}
				if (t->nrcols == 2){ 
				  if( sv == t->h ){
					s = statement_semijoin(t,s);
				  } else if( sv == t->t ){
					statement *r = statement_reverse(t);
					s = statement_semijoin(r,s);
				  }
				}
			} else if (s->nrcols == 2){
				var *sv = s->h;
				if (t->nrcols == 1){ 
				  if( sv == t->h ){
					s = statement_semijoin(s,t);
				  } else if( s->t == t->h){
					s = statement_reverse(s);
					s = statement_semijoin(s,t);
				  } 
				} else if (t->nrcols == 2){ 
			          if (sv == t->h && 
				      s->t == t->t){
					  s = statement_intersect(s,t);
				  } else 
			          if (sv == t->t &&  
				      s->t == t->h){
					  s = statement_reverse(s);
					  s = statement_intersect(s,t);
				  }
				}
			}
			/* if s changed t is used so no need to store */
			if (s == olds){ 
				list_append_statement(l, t);
			} else {
				changed++;
				len--;
			}
			m = m->next;
		}
		if (s) list_append_statement(l, s);
	    	n = l->h;
		len--;
	}
	return l;
}

static
statement *sql_compare( context *sql, statement *ls, statement *rs, char *compare_op ){
	int join = 0;
	int type = 0;

	if (!ls || !rs) return NULL;

	if (ls->nrcols <= 0 && rs->nrcols <= 0){
		snprintf(sql->errstr, ERRSIZE, 
	  	_("Compare(%s) between two atoms is not possible"),
		compare_op);
	       	return NULL;
	} else if (ls->nrcols > 0 && rs->nrcols > 0){
		join = 1;
	}
	if (compare_op[0] == '='){
	   	type = cmp_equal;
	} else if (compare_op[0] == '<'){
	   	type = cmp_lt;
	  	if (compare_op[1] != '\0'){
	    		if (compare_op[1] == '>'){ 
	   			type = cmp_notequal;
	    		} else if (compare_op[1] == '='){
	   			type = cmp_lte;
	    		}
	  	}
	} else if (compare_op[0] == '>'){
	   	type = cmp_gt;
	  	if (compare_op[1] != '\0'){
	       		if (compare_op[1] == '='){ 
	   			type = cmp_gte;
	    		}
	  	}
	}
	if (join){
		if (ls->h && rs->h && ls->h == rs->h ){ 
			/* 
			 * same table, ie. no join 
			 * do a [compare_op].select(true) 
			 */
			func *cmp = cat_bind_func(sql->cat, compare_op,
				tail_type(ls)->sqlname, 
				tail_type(rs)->sqlname, NULL);

			if (!cmp){
				snprintf(sql->errstr, ERRSIZE, 
		  		_("Binary compare operation %s %s %s missing"),
				tail_type(ls)->sqlname, compare_op,
				tail_type(rs)->sqlname);
		       		return NULL;
			}
			return statement_select(
				statement_binop( ls, rs, cmp), 
				  statement_atom(atom_general(
				    cat_bind_type(sql->cat, "BOOL" ), 
				     _strdup("1"))), cmp_equal);
		}
		rs = check_types( sql, tail_type(ls), rs );
		if (!rs) return NULL;
		rs = statement_reverse( rs );
		return statement_join( ls, rs, type );
	} else {
		if (ls->nrcols == 0){
			statement *t = ls;
			ls = rs; 
			rs = t;
		}
		rs = check_types( sql, tail_type(ls), rs );
		if (!rs) return NULL;
		return statement_select( ls, rs, type );
	}
}

static
statement *search_condition( context *sql, scope *scp, symbol *sc, 
		statement *group, statement *subset ){
	if (!sc) return NULL;
	switch(sc->token){
	case SQL_OR: {
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		statement *ls = search_condition( sql, scp, lo, group, subset );
		statement *rs = search_condition( sql, scp, ro, group, subset );
		if (!ls || !rs) return NULL;
		if (ls->type != st_diamond && ls->type != st_pearl){
			ls = statement_diamond( ls ); 
		}
		if (rs->type != st_diamond && rs->type != st_pearl){
			rs = statement_diamond( rs ); 
		}
		if (ls->type == st_diamond && rs->type == st_diamond){
			ls = statement_pearl( ls->op1.lval );
			list_append_list( ls->op1.lval, rs->op1.lval );
		} else if (ls->type == st_pearl && rs->type == st_diamond){
			list_append_list( ls->op1.lval, rs->op1.lval );
		} else if (ls->type == st_diamond && rs->type == st_pearl){
			list_append_list( rs->op1.lval, ls->op1.lval );
			ls = rs;
		} else if (ls->type == st_pearl && rs->type == st_pearl){
			(void)list_map(  ls->op1.lval, 
				(map_func)&list_map_append_list, rs->op1.sval);
		}
		return ls;
	} break;
	case SQL_AND: {
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		statement *ls = search_condition( sql, scp, lo, group, subset );
		statement *rs = search_condition( sql, scp, ro, group, subset );
		if (!ls || !rs) return NULL;
		if (ls->type != st_diamond && ls->type != st_pearl){
			ls = statement_diamond( ls ); 
		}
		if (rs->type != st_diamond && rs->type != st_pearl){
			rs = statement_diamond( rs ); 
		}
		if (ls->type == st_diamond && rs->type == st_diamond){
			list *nl = NULL;
			list_merge( ls->op1.lval, rs->op1.lval );	
			statement_destroy(rs);
			nl = query_and( sql->cat, ls->op1.lval);
			list_destroy( ls->op1.lval );
			ls->op1.lval = nl;
		} else if (ls->type == st_pearl && rs->type == st_diamond){
			list_map(  ls->op1.lval, 
				(map_func)&list_map_merge, rs->op1.sval);
			statement_destroy(rs);
		} else if (ls->type == st_diamond && rs->type == st_pearl){
			list_map(  rs->op1.lval, 
				(map_func)&list_map_merge, ls->op1.sval);
			statement_destroy(ls);
			ls = rs;
		} else if (ls->type == st_pearl && rs->type == st_pearl){
			list_map(  ls->op1.lval, 
				(map_func)&list_map_merge, rs->op1.sval);
			statement_destroy(rs);
		}
		return ls;
	} break;
	case SQL_COMPARE: {
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->next->data.sym;
		char *compare_op = sc->data.lval->h->next->data.sval;
		statement *rs, *ls = scalar_exp(sql, scp, lo, group, subset);

		if (!ls) return NULL;
		if (ro->token != SQL_SELECT){
	       		rs = scalar_exp(sql, scp, ro, group, subset);
			if (!rs) return NULL;
			return sql_compare( sql, ls, rs, compare_op );
		} else {
			node *o;
			rs = subquery(sql, scp, ro );
			if (!rs) return NULL;
			if (rs->type != st_list 
			 || list_length(rs->op1.lval) == 0){
				snprintf(sql->errstr, ERRSIZE, 
		  		_("Subquery result wrong"));
		       		return NULL;
			}
			o = rs->op1.lval->h;
			if (list_length(rs->op1.lval) == 1){
				statement *j = sql_compare( sql, ls, 
						o->data.stval, compare_op );
				if (!j) return NULL;
				return statement_semijoin(ls, j);
			} else {
				statement *sd, *j = sql_compare( sql, ls, 
						o->data.stval, compare_op );
				if (!j) return NULL;
			        sd = statement_diamond(
				       statement_join( j, o->next->data.stval, 
				      	 cmp_equal ) ); 
				o = o->next;
				o = o->next;
				for( ; o; o = o->next ){
					list_append_statement( sd->op1.lval,
					  statement_join( j, o->data.stval, 
				      		cmp_equal ) ); 
				}
				return sd;
			} 
			return NULL;
		}
	} break;
	case SQL_BETWEEN: 
	case SQL_NOT_BETWEEN: {
		statement *res = NULL;
		symbol *lo    = sc->data.lval->h->data.sym;
		int symmetric = sc->data.lval->h->next->data.ival;
		symbol *ro1   = sc->data.lval->h->next->next->data.sym;
		symbol *ro2   = sc->data.lval->h->next->next->next->data.sym;
		statement *ls = scalar_exp(sql, scp, lo,  group, subset);
		statement *rs1 = scalar_exp(sql, scp, ro1, group, subset);
		statement *rs2 = scalar_exp(sql, scp, ro2, group, subset);
		if (!ls || !rs1 || !rs2) return NULL;
		if (rs1->nr > 0 || rs2->nr > 0){
			snprintf(sql->errstr, ERRSIZE, 
		  	_("Between requires an atom on the right handside"));
		       	return NULL;
		}
		/* add check_type */
		if (symmetric){
			statement *tmp = NULL;
			func *min = cat_bind_func(sql->cat, "min",
				tail_type(rs1)->sqlname, 
				tail_type(rs2)->sqlname, NULL);
			func *max = cat_bind_func(sql->cat, "max",
				tail_type(rs1)->sqlname, 
				tail_type(rs2)->sqlname, NULL);
			if (!min || !max){
				snprintf(sql->errstr, ERRSIZE, 
		  		_("min or max operator on types %s %s missing"),
				tail_type(rs1)->sqlname,
				tail_type(rs2)->sqlname);
		       		return NULL;
			}
			tmp = statement_binop( rs1, rs2, min);
			rs2 = statement_binop( rs1, rs2, max); 
			rs1 = tmp;
		}
		res = statement_select2( ls, rs1, rs2, cmp_equal );
		if(sc->token == SQL_NOT_BETWEEN)
			res = statement_diff( ls, res );
		return res;
	}
	case SQL_LIKE: 
	case SQL_NOT_LIKE: {
		statement *res = NULL;
		symbol *lo  = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		statement *ls = scalar_exp(sql, scp, lo, group, subset);
		atom *a = NULL, *e = NULL;
		if (!ls) return NULL;
		if (ro->token == SQL_ATOM){
			a = ro->data.aval;
		} else {
			a = ro->data.lval->h->data.aval;
			e = ro->data.lval->h->next->data.aval;
		}
		if (e){
			snprintf(sql->errstr, ERRSIZE, 
		  	_("Time to implement LIKE escapes"));
		       	return NULL;
		}
		if (a->type != string_value){
			snprintf(sql->errstr, ERRSIZE, 
		  	_("Wrong type used with LIKE statement, should be string %s %s"), atom2string(a), atom_type(a)->sqlname);
		       	return NULL;
		}
		res = statement_like( ls, statement_atom(atom_dup(a)) );
		if(sc->token == SQL_NOT_LIKE)
			res = statement_diff( ls, res );
		return res;
	}
	case SQL_IN: {
		dlist *l = sc->data.lval;
		symbol *lo  = l->h->data.sym;
		statement *ls = scalar_exp(sql, scp, lo, group, subset);
		if (!ls) return NULL;
		if (l->h->next->type == type_list){
			dnode *n = l->h->next->data.lval->h;
			list *nl = list_create();
			while(n){
				list_append_atom(nl, 
					atom_dup(n->data.sym->data.aval));
				n = n->next;
			}
			return statement_exists( ls, nl);
		} else if (l->h->next->type == type_symbol){
			statement *sq =
			       	subquery(sql, scp, l->h->next->data.sym);
			if (!sq) return NULL;
			if (sq->type != st_list 
			 || list_length(sq->op1.lval) == 0){
				snprintf(sql->errstr, ERRSIZE, 
		  		_("Subquery result wrong"));
		       		return NULL;
			}
			if (list_length(sq->op1.lval) == 1){
				  statement *rs = sq->op1.lval->h->data.stval;
				  return statement_reverse(
				      statement_semijoin(
					statement_reverse(ls),
				          statement_reverse(rs)));
			} else { /* >= 2 */
				  node *o = sq->op1.lval->h;
				  statement *j = statement_join(ls, 
					statement_reverse(o->data.stval),
					cmp_equal);
			          statement *sd = statement_diamond(
				       statement_join( j, o->next->data.stval, 
				      	 cmp_equal ) ); 
				  o = o->next;
				  o = o->next;
				  for( ; o; o = o->next ){
					list_append_statement( sd->op1.lval,
					  statement_join( j, o->data.stval, 
				      		cmp_equal ) ); 
				  }
				  return sd;
			}
			return NULL;
		} else {
			snprintf(sql->errstr, ERRSIZE, 
		  	_("In missing inner query"));
			return NULL;
		}
	} break;
	case SQL_NOT_IN: {
		dlist *l = sc->data.lval;
		symbol *lo  = l->h->data.sym;
		statement *ls = scalar_exp(sql, scp, lo, group, subset);
		if (!ls) return NULL;
		if (l->h->next->type == type_list){
			dnode *n = l->h->next->data.lval->h;
			list *nl = list_create();
			while(n){
				list_append_atom(nl, atom_dup(n->data.aval));
				n = n->next;
			}
			return statement_diff(ls, statement_exists( ls, nl));
		} else if (l->h->next->type == type_symbol){
			statement *sr = statement_reverse(ls);
			var  *sqn = subquery_optname(sql, scp,
						l->h->next->data.sym );
			if (sqn){
				return statement_reverse(
				    statement_diff(sr,
				      statement_reverse( 
				        sqn->data.tval->columns->h->data.cval->s
					  )));
			}
			return NULL;
		} else {
			snprintf(sql->errstr, ERRSIZE, 
		  	_("In missing inner query"));
			return NULL;
		}
	} break;
	case SQL_EXISTS: 
	case SQL_NOT_EXISTS: {
		/* NOT still broken */
		symbol *lo = sc->data.sym;
		statement *ls = subquery(sql, scp, lo );

		if (!ls) return NULL;
		if (ls->type != st_list){
			snprintf(sql->errstr, ERRSIZE, 
		  	_("Subquery result wrong"));
		       	return NULL;
		}
		if (list_length(ls->op1.lval) == 1){
			return statement_reverse(ls->op1.lval->h->data.stval); 
		} else {
			node *o = ls->op1.lval->h->next; /* skip first */
			statement *sd, *j = statement_reverse( o->data.stval );

			o = o->next;
			if (!o)
			    return j;

			sd = statement_diamond( 
				 statement_join( j, o->data.stval, cmp_equal )); 
			o = o->next;
			for( ; o; o = o->next ){
				list_append_statement( sd->op1.lval,
				  statement_join( j, o->data.stval, 
			      		cmp_equal ) ); 
			}
			return sd;
		}
	} break;
	default:
		snprintf(sql->errstr, ERRSIZE, 
		_("Predicate %s %d: time to implement some more"), 
			token2string(sc->token), sc->token );
		return NULL;
	}
	snprintf(sql->errstr, ERRSIZE, 
	_("Predicate: time to implement some more"));
	return NULL;
}
static
statement *having_condition( context *sql, scope *scp, symbol *sc, 
		statement *group, statement *subset ){
	if (!sc) return NULL;
	switch(sc->token){
	case SQL_OR: {
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		statement *ls = having_condition( sql, scp, lo, group, subset );
		statement *rs = having_condition( sql, scp, ro, group, subset );
		if (!ls || !rs) return NULL;
		
		return statement_union(ls,rs);
	} break;
	case SQL_AND: {
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		statement *ls = having_condition( sql, scp, lo, group, subset );
		statement *rs = having_condition( sql, scp, ro, group, subset );
		if (!ls || !rs) return NULL;

		return statement_semijoin(ls,rs);
	} break;
	case SQL_COMPARE: {
		statement *s = search_condition( sql, scp, sc, group, subset );
		return s;
	} break;
	case SQL_BETWEEN: 
	case SQL_NOT_BETWEEN: {
		statement *s = search_condition( sql, scp, sc, group, subset );
		return s;
	}
	case SQL_LIKE: 
	case SQL_NOT_LIKE: {
		statement *s = search_condition( sql, scp, sc, group, subset );
		return s;
	}
	case SQL_IN: {
		statement *s = search_condition( sql, scp, sc, group, subset );
		return s;
	} break;
	case SQL_NOT_IN: {
		statement *s = search_condition( sql, scp, sc, group, subset );
		return s;
	} break;
	case SQL_EXISTS: 
	case SQL_NOT_EXISTS: {
		statement *s = search_condition( sql, scp, sc, group, subset );
		return s;
	} break;
	default:
		snprintf(sql->errstr, ERRSIZE, 
		_("Predicate %s %d: time to implement some more"), 
			token2string(sc->token), sc->token );
		return NULL;
	}
	snprintf(sql->errstr, ERRSIZE, 
	_("Predicate: time to implement some more"));
	return NULL;
}


static 
statement *diamond2pivot( context *sql, list *l ){
	list *pivots = list_create();
	node *n;
	l = query_and( sql->cat, l );
       	n = l->h;
	if (n){
		int len = 0;
		int markid = 0;
		statement *st = n->data.stval;
		if (st->nrcols == 1){
			list_append_statement(pivots, statement_mark(
				statement_reverse(st), markid++));
		} 
		if (st->nrcols == 2){
			list_append_statement(pivots, 
				statement_mark( statement_reverse(st), markid));
			list_append_statement(pivots, 
				statement_mark( st,markid++));
		} 
		n = list_remove( l, n );
		len = list_length(l) + 1;
		while(list_length(l) > 0 && len > 0){
		    len--;
		    n = l->h;
		    while(n){
			statement *st = n->data.stval;
			list *npivots = list_create();
			node *p = pivots->h;
			while(p){
				var *tv = p->data.stval->t;
				if (tv == st->h){
				  statement *m = statement_mark(
					 statement_reverse(st), markid);
				  statement *m1 = statement_mark(st, markid++);
				  statement *j = statement_join( p->data.stval, 
				    statement_reverse(m),
				      cmp_equal ); 
				  statement *pnl = statement_mark( j, markid );
				  statement *pnr = statement_mark( 
					   statement_reverse(j), markid++ );
				  list_append_statement( npivots,
				      statement_join( pnl, m1, cmp_equal));

				  p = pivots->h;
				  while(p){
				    list_append_statement( npivots,
				      statement_join( pnr, p->data.stval, 
						    cmp_equal));
					  p = p->next;
				  }
				  list_destroy(pivots);
				  pivots = npivots;
				  n = list_remove( l, n );
				  break;
				} else
				if (tv == st->t){
				  statement *m1 = statement_mark(
					 statement_reverse(st), markid);
				  statement *m = statement_mark( st, markid++ );
				  statement *j = statement_join( p->data.stval, 
				    statement_reverse(m),
				      cmp_equal); 
				  statement *pnl = statement_mark( j, markid );
				  statement *pnr = statement_mark( 
					   statement_reverse(j), markid++ );
				  list_append_statement( npivots,
				      statement_join( pnl, m1, cmp_equal));

				  p = pivots->h;
				  while(p){
				    list_append_statement( npivots,
				      statement_join( pnr, 
					    p->data.stval, cmp_equal));
					  p = p->next;
				  }
				  list_destroy(pivots);
				  pivots = npivots;
				  n = list_remove( l, n );
				  break;
				}
				p = p->next;
			 }
			 if (n) n = n->next;
		    }
		}
		if (!len){
			assert(0);
			snprintf(sql->errstr, ERRSIZE, 
		  	  _("Semantically incorrect query, unrelated tables"));
			return NULL;
		}
	}
	list_destroy(l);
	return statement_list(pivots);
}

/* reason for the group stuff 
 * if a value is selected twice once on the left hand of the
 * or and once on the right hand of the or it will be in the
 * result twice.
 *
 * current version is broken, unique also remove normal doubles.
 */
static 
statement *pearl2pivot( context *sql, list *ll ){
	node *n = ll->h;
	if(n){
		statement *pivots = diamond2pivot(sql, n->data.lval);
		list *cur = pivots->op1.lval;
		n = n->next;
		/*
		statement *g = NULL;
		*/
		while(n){
			statement *npivots = diamond2pivot(sql, n->data.lval);
			list *l = npivots->op1.lval;
			list *inserts = list_create();

			node *m = l->h;

			while(m){
				node *c = cur->h;
				while(c){
				    	if (c->data.stval->t == 
					    m->data.stval->t){
					    list_append_statement( inserts,
					      statement_insert_column(
					       c->data.stval, m->data.stval ));
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
				g = statement_derive(m->data.stval, g);
			} else {
				g = statement_group(m->data.stval);
			}
			m = m->next;
		}
		g = statement_reverse( statement_unique( 
			statement_reverse( g )));
		m = inserts->h;
		cur = list_create();
		while(m){
			list_append_statement( cur, 
			  statement_semijoin( m->data.stval, g));
			m = m->next;
		}
		*/
		return statement_list(cur);
	}
	return NULL;
}

statement *query_groupby_inner( context *sql, scope *scp, statement *c,
				statement *st, statement *cur ){
	node *n = st->op1.lval->h;
	while(n){
		statement *s = n->data.stval;
		if (s->t == c->h ){
			statement *j = statement_join( s, c, cmp_equal);
			if (cur) cur = statement_derive( cur, j );
			else cur = statement_group( j );
			break;
		}
		n = n->next;
	}
	return cur;
}

static
statement *query_groupby( context *sql, scope *scp, symbol *groupby, statement *st ){
	statement *cur = NULL;
	dnode *o = groupby->data.lval->h;
	while(o){
	    symbol *group = o->data.sym;
	    statement *c = column_ref(sql, scp, group );
	    cur = query_groupby_inner( sql, scp, c, st, cur );
	    o = o->next;
	}

	return cur;
}

static
statement *query_groupby_lifted( context *sql, scope *scp, statement *st ){
	statement *cur = NULL;
	node *o = scp->lifted->h;
	while(o){
            lifted *l = (lifted*)o->data.sval;
	    statement *sc = statement_column(l->c, l->v);
	    cur = query_groupby_inner( sql, scp, sc, st, cur );
	    o = o->next;
	}
	return cur;
}

static
statement *query_orderby( context *sql, scope *scp, symbol *orderby, statement *st, statement *subset, statement *group ){
	statement *cur = NULL;
	dnode *o = orderby->data.lval->h;
	if (group) group = statement_reverse(statement_unique(group));
	while(o){
	    symbol *order = o->data.sym;
	    if (order->token == SQL_COLUMN){
		symbol *col = order->data.lval->h->data.sym;
		int direction = order->data.lval->h->next->data.ival;
		statement *sc = column_ref(sql, scp, col );
		if (sc){
		  statement *j = NULL;
		  if (sc->type == st_column){
			j = find_subset( subset, sc->h);
			if (!j){
				snprintf(sql->errstr, ERRSIZE,
				"subset not found for orderby column\n" );
				return NULL;
			}
			if (group)
				j = statement_join( group, j, cmp_equal); 
		        j = statement_join( j, sc, cmp_equal);
		  } else {
			j = sc;
		  }
		  if (cur) cur = statement_reorder( cur, j, direction);
		  else cur = statement_order( j, direction );
	        } else {
			return NULL;
		}
	    } else {
		snprintf(sql->errstr, ERRSIZE,
			"order not of type SQL_COLUMN\n" );
		return NULL;
	    }
	    o = o->next;
	}
	return cur;
}

static
statement *substitute( statement *s, statement *c ){
	switch( s->type){
	case st_unique: return statement_unique( c );
	case st_column: return c;
	case st_atom: return s;
	case st_aggr: return statement_aggr( substitute( s->op1.stval, c), 
				      s->op2.aggrval, s->op3.stval );
	case st_binop: return statement_binop( substitute( s->op1.stval, c), 
				       s->op2.stval, s->op3.funcval );
	default:
		      return s;
	}
	return s;
}

static
statement *sql_simple_select
( 
	context *sql, 
	scope *scp, 
	dlist *selection
){
  int toplevel = (!scp)?1:0;
  statement *s = NULL;
  list *rl = list_create();

  scp = scope_open( scp );
  if (toplevel) { /* only on top level query */
	  node *n = sql->cat->cur_schema->tables->h;

	  for( ; (n); n = n->next ){
		table *p = n->data.tval;
	  	scope_add_table( scp, p, p->name );
	  }
  }

  if (selection){
	dnode *n = selection->h;
	while (n){
		statement *cs = column_exp( sql, scp, n->data.sym, NULL, NULL);
		if (!cs){
		       		return NULL;
		}
		list_append_statement(rl, cs);
		n = n->next;
	}
  }
  s = statement_list(rl);

  if (scp) scp = scope_close( scp );
 
  if (!s && sql->errstr[0] == '\0')
	  snprintf(sql->errstr, ERRSIZE, _("Subquery result missing")); 
  return s;
}

static
statement *sql_select
( 
	context *sql, 
	scope *scp, 
	int distinct, 
	dlist *selection, 
	dlist *into, 
	dlist *table_exp, 
	symbol *orderby
){
  int toplevel = (!scp)?1:0;
  statement *s = NULL;

  symbol *from = table_exp->h->data.sym;

  symbol *where = table_exp->h->next->data.sym;
  symbol *groupby = table_exp->h->next->next->data.sym;
  symbol *having = table_exp->h->next->next->next->data.sym;
  statement *order = NULL, *group = NULL, *subset = NULL;

  if (!from && !where) return sql_simple_select( sql, scp, selection );
		  
  scp = scope_open( scp );
  if (from){ /* keep variable list with tables and names */
  	  dlist *fl = from->data.lval;
	  dnode *n = NULL;

	  for( n = fl->h; (n); n = n->next )
		table_ref( sql, scp, n->data.sym);

  } else if (toplevel) { /* only on top level query */
	  node *n = sql->cat->cur_schema->tables->h;

	  for( ; (n); n = n->next ){
		table *p = n->data.tval;
	  	scope_add_table( scp, p, p->name );
	  }
  }

  if (where){
	s = search_condition(sql, scp, where, NULL, NULL);
  } else if (from) {
	if (scope_count_tables(scp) > 1){
		scope_dump(scp);
		snprintf(sql->errstr, ERRSIZE, 
		  _("Subquery over multiple tables misses where condition"));
		return NULL;
	}
        s = scope_first_column( scp );
	if (!s){
		snprintf(sql->errstr, ERRSIZE, 
		  	_("Subquery has no columns"));
		return NULL;
	}
  } 

  if (s){
	if (s->type != st_diamond && s->type != st_pearl){
		s = statement_diamond(s);
	}
	if (s->type == st_pearl){
		statement *ns = pearl2pivot(sql, s->op1.lval);
		statement_destroy(s);
		s = ns;
	} else {
	  	statement *ns = diamond2pivot(sql, s->op1.lval);
		statement_destroy(s);
		s = ns;
	}

  	if (s && groupby){
	       	group = query_groupby(sql, scp, groupby, s );
		if (!group){
  			if (subset)
  				statement_destroy(subset);
			return NULL;
		}
	}

	if (s && list_length(scp->lifted) > 0){
	       	group = query_groupby_lifted(sql, scp, s );
		if (!group){
  			if (subset)
  				statement_destroy(subset);
			return NULL;
		}
	}
  }

  subset = s;
  if (having){
	s = having_condition(sql, scp, having, group, subset);

	if (!s) return NULL;

	if (group){
	  group = statement_reverse( statement_semijoin(
				  statement_reverse(group), s ));
	} else {
	  node *n = NULL;
	  list *sl = list_create();
	  for(n = subset->op1.lval->h; n; n = n->next){
	    list_append_statement(sl, statement_semijoin( n->data.stval, s) );
	  }
	  subset = statement_list(sl);
	}
  }

  if (subset){
        list *rl = list_create();
  	if (selection){
	  dnode *n = selection->h;
	  while (n){
		statement *cs = column_exp(sql, scp, n->data.sym, group,subset);
		if (!cs){
		       		return NULL;
		}
		list_append_statement(rl, cs);
		n = n->next;
	  }
  	} else {
	    /* select * from single table */
	    if (toplevel){
		var *tv = scope_first_table(scp);
	    	table *t = NULL;
		node *n;

		if (!tv) return NULL;
		t = tv->data.tval;
	    	for(n = t->columns->h; n; n = n->next){
			column *cs = n->data.cval;
			node *m = subset->op1.lval->h;
			if(m){
				statement *ss = m->data.stval;
				list_append_statement(rl, 
			  	statement_join(ss, 
					statement_column(cs, tv), cmp_equal));
			}
	    	}
	    } else { /* 
		      * subquery * can only return one column, better
		      * the oids are needed 
		      */
		var *tv = scope_first_table(scp);
		statement *foundsubset = find_subset(subset, tv);

		if (!foundsubset) return NULL;
		list_append_statement(rl, foundsubset);
	    }
	}
	/* the inner query should output a table where the first bat
	 * contains the queried column values. 
	 * If variables from the outer query are correlated 
	 * the oids from the base tables of these variables are returned
	 * in the next columns.
	 */
	if (list_length(scp->lifted) > 0){
		list *vars = scope_unique_lifted_vars( scp );
		node *o = vars->h;
		group = statement_reverse(statement_unique(group));
		while(o){
	    		var *v = (var*)o->data.sval;
			statement *foundsubset = find_subset(subset, v);
			if (!foundsubset) return NULL;
			list_append_statement(rl, 
			    statement_join( group, foundsubset, cmp_equal ));
	    		o = o->next;
		}
	}
	s = statement_list(rl);
  }

  if (s && subset && orderby){
       	order = query_orderby(sql, scp, orderby, s, subset, group );
	if (!order){
  		if (subset)
  			statement_destroy(subset);
		return NULL;
	}
  }

  if (scp) scp = scope_close( scp );
 
  if (!s && sql->errstr[0] == '\0')
	  snprintf(sql->errstr, ERRSIZE, _("Subquery result missing")); 

  if (subset)
  	statement_destroy(subset);

  if (s && order) 
	  return statement_ordered(order,s);
  return s;
}


static
statement *create_view( context *sql, schema *schema, dlist *qname, 
		dlist *column_spec, symbol *query, int check){

	catalog *cat = sql->cat;
	char *name = table_name(qname);

	if (cat_bind_table(cat, schema, name)){
		snprintf(sql->errstr, ERRSIZE, 
			_("Create View name %s allready in use"), name);
		return NULL; 
	} else {
		statement *stct = NULL;
		statement *sq = subquery( sql, NULL, query );

		if (!sq) return NULL;

		if (column_spec){
			table *table = cat_create_table( 
					cat, 0, schema, name, 0, query->sql);
			int seqnr = 0;
			dnode *n = column_spec->h;
			node *m = sq->op1.lval->h;

			stct = statement_create_table( table );

			while(n){
				char *cname = n->data.sval;
				statement *st = m->data.stval;
				char *ctype = tail_type(st)->sqlname;
				column *col = cat_create_column( cat, 0,  
					table, cname, ctype, "NULL", 1, seqnr);
				col->s = st;
				st_incref(st);
				n = n->next;
				m = m->next;
				seqnr++;
			}
		} else {
			table *table = create_table_intern( 
				sql, schema, name, query->sql, sq );
			stct = statement_create_table( table );
		}
		statement_destroy(sq); 
		return stct;
	}
	return NULL;
}

static
statement *column_option( context *sql, symbol *s, column *c, statement *cc ){

	switch(s->token){
	case SQL_CONSTRAINT:
		{ dlist *l = s->data.lval;
		  /*char *opt_name = l->h->data.sval;*/
		  symbol *ss = l->h->next->data.sym;
		  if (ss->token == SQL_NOT_NULL){
			c->null = 0;
			return statement_not_null( cc );
		  } else {
			printf("constraint not handled ");
			printf("(%d)->token = %s\n", (int)s, token2string(s->token));
		  }
		} break;
	case SQL_PRIMARY_KEY:
		break;
	case SQL_ATOM: 
		{ statement *a = statement_atom(atom_dup(s->data.aval));
		  return statement_default(cc,  a);
		}
		break;
	default:
		printf("column_option (%d)->token = %s\n", (int)s, token2string(s->token));
	}
	return NULL;
}

static
statement *create_column( context *sql, symbol *s, int seqnr, table *table ){
	statement *res = NULL;
	catalog *cat = sql->cat;

	switch(s->token){
	case SQL_COLUMN: { 
		  dlist *l = s->data.lval;
		  char *cname = l->h->data.sval;
		  type *ctype = cat_bind_type( cat, l->h->next->data.sval);
		  dlist *opt_list = l->h->next->next->data.lval;
		  if (cname && ctype){
		  	column *c = cat_create_column( cat, 0, table, 
				cname, ctype->sqlname, "NULL", 1, seqnr);
			res = statement_create_column(c);
		  	if (opt_list){
				dnode *n = opt_list->h;
				while(n){
				   res = column_option(sql,n->data.sym, c, res);
				   n = n->next;
				}
			}
		  } else {
			snprintf(sql->errstr, ERRSIZE, 
				_("Create Column: type or name") );
			return NULL; 
		  }
		} break;
	case SQL_CONSTRAINT:
		 break;
	default:
	  printf("create_column (%d)->token = %s\n", 
			  (int)s, token2string(s->token));
	}
	return res;
}

static
statement * create_table( context *sql, schema *schema, int temp, dlist *qname, dlist *columns){
	catalog *cat = sql->cat;
	char *name = table_name(qname);

	if (cat_bind_table(cat, schema, name)){
		snprintf(sql->errstr, ERRSIZE, 
			_("Create Table name %s allready in use"), name);
		return NULL; 
	} else {
		table *table = cat_create_table( cat, 0, schema, name, temp, 
					NULL);
		statement *st = statement_create_table( table );
		list *newcolumns = list_append_statement(list_create(), st );
		dnode *n = columns->h; 
		int seqnr = 0;

		while(n){
			list_append_statement(newcolumns, 
			   create_column( sql, n->data.sym, seqnr++, table ) );
			n = n->next;
		}
		return statement_list(newcolumns);
	}
}

static
statement * drop_table( context *sql, dlist *qname, int drop_action ){
	char *tname = table_name(qname);
	table *t = cat_bind_table(sql->cat, sql->cat->cur_schema, tname);

	if (!t){
		snprintf(sql->errstr, ERRSIZE, 
			_("Drop Table, table %s unknown"), tname);
	} else {
		cat_destroy_table( sql->cat, sql->cat->cur_schema, tname);
		return statement_drop_table(t, drop_action);
	}
	return NULL;
}

static
statement * alter_table( context *sql, dlist *qname, symbol *table_element){
	catalog *cat = sql->cat;
	char *name = table_name(qname);
	table *table = NULL;

	if ((table = cat_bind_table(cat, sql->cat->cur_schema, name)) == NULL){
		snprintf(sql->errstr, ERRSIZE, 
			_("Alter Table name %s doesn't exist"), name);
		return NULL; 
	} else {
		int seqnr = list_length(table->columns);
		statement *c = create_column( sql, 
				table_element, seqnr++, table);
		return c;
	}
}

static
statement * create_schema( context *sql, dlist *auth_name, 
		dlist *schema_elements){
	catalog *cat = sql->cat;
	char *name = schema_name(auth_name);
	char *auth = schema_auth(auth_name);

	if (auth == NULL){
		auth = sql->cat->cur_schema->auth;
	}
	if (cat_bind_schema(cat,name)){
		snprintf(sql->errstr, ERRSIZE, 
			_("Create Schema name %s allready in use"), name);
		return NULL; 
	} else {
		schema *schema = cat_create_schema( cat, 0, name, auth );
		statement *st = statement_create_schema( schema );
		list *schema_objects = list_append_statement(list_create(), st);

		dnode *n = schema_elements->h; 

		while(n){
			st = NULL;
			if (n->data.sym->token == SQL_CREATE_TABLE){
				dlist *l = n->data.sym->data.lval;
			   	st = create_table( sql, schema, 
					l->h->data.ival, 
					l->h->next->data.lval, 
					l->h->next->next->data.lval );
			} else if (n->data.sym->token == SQL_CREATE_VIEW){
				dlist *l = n->data.sym->data.lval;
			   	st = create_view( sql, schema, 
					l->h->data.lval, l->h->next->data.lval,
					l->h->next->next->data.sym,
					l->h->next->next->next->data.ival ); 
			}
			list_append_statement(schema_objects, st );
			n = n->next;
		}
		return statement_list(schema_objects);
	}
}


statement *insert_value( context *sql, column *c, statement *id, symbol *s ){
	if (s->token == SQL_ATOM){
		statement *n = NULL;
		statement *a = statement_atom( atom_dup(s->data.aval) );
		if (!(n = check_types( sql, c->tpe, a ))) return NULL;
		a = statement_insert( c, id, n );
		return a;
	} else if (s->token == SQL_NULL) {
		return statement_insert( c, id, NULL);
	}
	return NULL;
}

statement *insert_into( context *sql, dlist *qname, dlist *columns, 
			symbol *val_or_q){
	catalog *cat = sql->cat;
	char *tname = table_name(qname);
	table *t = cat_bind_table( cat,  sql->cat->cur_schema, tname );
	list *collist = NULL;

	if (!t){
		snprintf(sql->errstr, ERRSIZE, 
			_("Inserting into non existing table %s"), tname);
		return NULL;
	}
	if (columns){
		/* XXX: what to do for the columns which are not listed */
		dnode *n = columns->h;
		collist = list_create();
		while(n){
			column *c = cat_bind_column(cat, t, n->data.sval );
			if (c){
				list_append_column( collist, c );
			} else {
				snprintf(sql->errstr, ERRSIZE, 
				  _("Inserting into non existing column %s.%s"),
				  tname, n->data.sval);
				return NULL;
			}
			n = n->next;
		}
	} else {
		collist = t->columns;
	}
	if (val_or_q->token == SQL_VALUES){
	    dlist *values = val_or_q->data.lval;
	    if (dlist_length(values) != list_length(collist)){
		snprintf(sql->errstr, ERRSIZE, _("Inserting into table %s, number of values doesn't match number of columns"), tname );
		return NULL;
	    } else {
		dnode *n = values->h;
		node *m = collist->h;
		statement *id = (m)?statement_count( 
				statement_column(m->data.cval, NULL) ):NULL;
		list *l = list_create();
		while(n && m && id){
		  statement *iv = 
			  insert_value( sql, m->data.cval, id, n->data.sym);

		  if (iv){
			list_append_statement( l, iv );
		  } else {
			  return NULL;
		  }
		  n = n->next;
		  m = m->next;
		}
		return statement_insert_list(l);
	    }
	} else {
	    statement *s = subquery(sql, NULL, val_or_q );
	    if (!s) return NULL;
	    if (list_length(s->op1.lval) != list_length(collist)){
		snprintf(sql->errstr, ERRSIZE, _("Inserting into table %s, query result doesn't match number of columns"), tname );
		return NULL;
	    } else {
		list *l = list_create();
		node *m = collist->h;
		node *n = s->op1.lval->h;
		while(n && m){
			list_append_statement( l,
			  statement_insert_column(
		           statement_column(m->data.cval,NULL), n->data.stval));
		  	n = n->next;
		  	m = m->next;
		}
		return statement_list(l);
	    }
	}
	return NULL;
}

statement *update_set( context *sql, dlist *qname, 
		       dlist *assignmentlist, 
		       symbol *opt_where)
{
	statement *s = NULL;
	char *tname = table_name(qname);
	table *t = cat_bind_table( sql->cat,  sql->cat->cur_schema, tname );

	if (!t){
		snprintf(sql->errstr, ERRSIZE, 
			_("Updating non existing table %s"), tname);
	} else {
		dnode *n;
		list *l = list_create();
		scope *scp;
	        
		scp = scope_open(NULL);
		scope_add_table( scp, t, t->name );

		if (opt_where) 
			s = search_condition(sql, scp, opt_where, NULL, NULL);

		n = assignmentlist->h;
		while (n){
			dlist *assignment = n->data.sym->data.lval;
			column *cl = cat_bind_column(
					sql->cat, t, assignment->h->data.sval);
			if (!cl){
				snprintf(sql->errstr, ERRSIZE, 
				  _("Updating non existing column %s.%s"), 
				  	tname, assignment->h->data.sval);
				return NULL;
			} else {
				statement *scl = statement_column(cl, NULL);
				symbol *a = assignment->h->next->data.sym;
				statement *sc = 
					column_exp( sql, scp, a, NULL,NULL);

				sc = check_types( sql, cl->tpe, sc );
				/*
                                statement *sc =
                                    scalar_exp(sql, scp, a, NULL, NULL);

				sc = check_types( sql, cl->tpe, sc );
				if (!sc) return NULL;
                                if (sc->nrcols > 0){
                                        statement *j, *co = sc;
                                        while(sc->type != st_column)
                                                sc = sc->op1.stval;
                                        j = statement_semijoin(sc, s );
                                        sc = substitute( co, j );
                                } else { 
                                        sc = statement_const( s, sc);
                                }
				*/
                                if (sc->nrcols <= 0)
                                        sc = statement_const( scl, sc);

                                list_append_statement( l,
                                        statement_update( scl, sc ));
			}
			n = n->next;
		}
		scp = scope_close(scp);
		return statement_list(l);
	}
	return NULL;
}

statement *delete_searched( context *sql, dlist *qname, symbol *opt_where){
	char *tname = table_name(qname);
	table *t = cat_bind_table( sql->cat,  sql->cat->cur_schema, tname );

	if (!t){
		snprintf(sql->errstr, ERRSIZE, 
			_("Deleting from non existing table %s"), tname);
	} else {
		statement *s = NULL;
		node *n;
		list *l = list_create();
		scope *scp;
	        
		scp = scope_open(NULL);
		scope_add_table( scp, t, t->name );

		if (opt_where) 
			s = search_condition(sql, scp, opt_where, NULL, NULL);

		n = t->columns->h;
		while (n){
			column *cl = n->data.cval;
			list_append_statement( l, statement_delete( cl, s ));  
			n = n->next;
		}
		scp = scope_close(scp);
		return statement_list(l);
	}
	return NULL;
}

static
statement *sql_statement( context *sql, symbol *s ){
	statement *ret = NULL;
	switch(s->token){
		case SQL_CREATE_SCHEMA:
			{ dlist *l = s->data.lval;
			  ret = create_schema( sql, l->h->data.lval, 
				l->h->next->next->next->data.lval );
			}
			break;
		case SQL_CREATE_TABLE:
			{ dlist *l = s->data.lval;
			  ret = create_table( sql, sql->cat->cur_schema, 
				l->h->data.ival, l->h->next->data.lval,
					l->h->next->next->data.lval ); 
			}
			break;
		case SQL_DROP_TABLE:
			{ dlist *l = s->data.lval;
			  ret = drop_table( sql, 
				l->h->data.lval, l->h->next->data.ival );
			}
			break;
		case SQL_CREATE_VIEW:
			{ dlist *l = s->data.lval;
			  ret = create_view( sql, sql->cat->cur_schema, 
				l->h->data.lval, l->h->next->data.lval,
				l->h->next->next->data.sym,
				l->h->next->next->next->data.ival ); 
			}
			break;
		case SQL_DROP_VIEW:
			{ dlist *l = s->data.lval;
			  ret = drop_table( sql, l, 0 );
			}
			break;
		case SQL_ALTER_TABLE:
			{ dlist *l = s->data.lval;
			  ret = alter_table( sql,
				  l->h->data.lval, /* table name*/
				  l->h->next->data.sym ); /* table element*/ 
			}
			break;
		case SQL_INSERT_INTO:
			{ dlist *l = s->data.lval;
			  ret = insert_into( sql,
				l->h->data.lval,
				l->h->next->data.lval,
				l->h->next->next->data.sym );
			}
			break;
		case SQL_UPDATE_SET:
			{ dlist *l = s->data.lval;
			  ret = update_set( sql, 
				l->h->data.lval,
				l->h->next->data.lval,
				l->h->next->next->data.sym );
			}
			break;
		case SQL_DELETE:
			{ dlist *l = s->data.lval;
			  ret = delete_searched( sql,
				l->h->data.lval, l->h->next->data.sym );
			}
			break;
		case SQL_SELECT: 
			ret = subquery( sql, NULL, s );
			/* add output statement */
			if (ret) ret = statement_output( ret );
			break;
		case SQL_JOIN: 
		case SQL_CROSS: 
			ret = query_exp( sql, NULL, s);
			/* add output statement */
			if (ret) ret = statement_output( ret );
			break;
		default:
		     	snprintf(sql->errstr, ERRSIZE, 
				_("sql_statement Symbol(%d)->token = %s"), 
				(int)s, token2string(s->token));
	}
	return ret;
}


statement *semantic( context *s,  symbol *sym ){
        statement *res = NULL;

	if (sym && (res = sql_statement( s, sym)) == NULL){
                fprintf(stderr, "Semanic error: %s\n", s->errstr);
                fprintf(stderr, "in %s line %d: %s\n",
                  	sym->filename, sym->lineno, sym->sql );
        }
        return res;
}


/*
 *	Text handling routines for simple embedded SQL
 */

/* use a template.mil file to hold the string mil data */

/* TODO
   1) fix error output 
   	1) (f)printf -> snprintf(sql->errstr, ERRSZE, "etc" )
	2) use (x)gettext to internationalize this
   2) cleanup while(n) -> for( n = ; n; n = n->next)

   3) remove list_map/traverse

   4) print code and review.

   5) may need levels of errors/warnings so we can skip non important once
 */ 

#include <unistd.h>
#include "sql.h"
#include "symbol.h"
#include "statement.h"
#include <mem.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

typedef struct var {
	table *t;
	char *vname;
} var;

static statement *query( context *sql, int distinct, dlist *selection, 
			 dlist *into, dlist *table_exp , symbol *orderby );

char *toLower( char *v ){
	char *s = v;
	while(*v) *v++ = (char)tolower(*v);
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
	case SQL_UNION_ALL: return "Union All";
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
	case SQL_NOT: return "Not";
	case SQL_EXISTS: return "Exists";
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
	case SQL_AMMSC: return "Ammsc";
	case SQL_COMPARE: return "Compare";
	case SQL_TEMP_LOCAL: return "Local Temporary";
	case SQL_TEMP_GLOBAL: return "Global Temporary";
	case SQL_INT_VALUE: return "Integer";
	case SQL_ATOM: return "Atom";
	case SQL_ESCAPE: return "Escape";
	default: return "unknown";
	}
}

typedef struct data {
	catalog *cat;
	char *d;
} data;

table *bind_table( list *tables, char *name ){
	node *n = tables->h; 
	while(n){
		var *v = (var*)n->data.sval;
		if (strcmp( v->vname, name) == 0){
			return v->t;	
		}
		n = n->next;
	}
	return NULL;
}

column *bind_column( list *columns, char *name ){
	node *n = columns->h; 
	while(n){
		column *c = (column*)n->data.sval;
		if (strcmp( c->name, name) == 0){
			return c;	
		}
		n = n->next;
	}
	return NULL;
}

static 
column *column_ref( catalog *cat, list *tables, symbol *column_r ){
	dlist *l = column_r->data.lval;
	assert (column_r->token == SQL_COLUMN && column_r->type == type_list);

	if (dlist_length(l) == 1){
		char *name = l->h->data.sval;
		column *c = NULL;
		node *n = tables->h; 
		while(n && !c){
			var *v = ((var*)n->data.sval);
			c = bind_column(v->t->columns, name);	
			n = n->next;
		}
		if (!c)
			printf("missing column %s\n", name);
		return c;
	} else if (dlist_length(l) == 2){
		char *tname = l->h->data.sval;
		char *cname = l->h->next->data.sval;
		table *t = bind_table( tables, tname );
		if (t){
			column *c = bind_column( t->columns, cname ); 
			if (!c)
				printf("missing column %s.%s\n", tname, cname);
			return c;
		} else {
			printf("missing table %s\n", tname);
		}
	} else if (dlist_length(l) >= 3){
		printf("not handeled yet column names of level >= 3\n" );
	}
	return NULL;
}

static
char *table_name( dlist *tname ){
	assert(tname && tname->h);

	if (dlist_length(tname) == 1)
		return tname->h->data.sval;
	if (dlist_length(tname) == 2)
		return tname->h->next->data.sval;
	return "Unknown";
}

static
table *table_ref( context *sql, symbol *tableref ){
	char *tname;
	if (tableref->token == SQL_NAME){
		tname = table_name(tableref->data.lval->h->data.lval);
	} else {
		tname = table_name(tableref->data.lval);
	}
	return sql->cat->bind_table(sql->cat, sql->cat->cur_schema, tname);
}

static
char *table_ref_var( context *sql, symbol *tableref ){
	char *var;
	if (tableref->token == SQL_NAME){
		var = tableref->data.lval->h->next->data.sval;
	} else {
		var = table_name(tableref->data.lval);
	}
	return var;
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
statement *scalar_exp( symbol *se, context *sql, list *tables, symbol *groupby ){
	column* c;
	switch(se->token){
	case SQL_BINOP: { 
		dnode *l = se->data.lval->h;
		func *f = sql->cat->bind_func(sql->cat, l->data.sval);
		if (f){
			statement *rs = scalar_exp( l->next->data.sym, 
						sql, tables, groupby);
			statement *ls = scalar_exp( l->next->next->data.sym, 
						sql, tables, groupby);
			return statement_binop( rs, ls ,f );
		} else {
			printf("Missing binop\n");
		}
	} break;
	case SQL_UNOP: {
		dnode *l = se->data.lval->h;
		func *f = sql->cat->bind_func(sql->cat, l->data.sval);
		if (f){
			statement *rs = scalar_exp( l->next->data.sym, 
						sql, tables, groupby);
			return statement_unop( rs, f );
		} else {
			printf("Missing unop\n");
		}
	} break;
	case SQL_COLUMN: {
		        c = column_ref( sql->cat, tables, se );
			if (c){ 
				return statement_column(c);
			} else {
				printf("missing column\n" );
			}
			break;
	}
	case SQL_AMMSC: {
		dlist *l = se->data.lval;
		aggr *a = sql->cat->bind_aggr(sql->cat, l->h->data.sval );
		int distinct = l->h->next->data.ival;
		statement *s;
		if (!l->h->next->next->data.sym){
		    	table *t = ((var*)tables->h->data.sval)->t;
		    	s = statement_column( t->columns->h->data.cval);
		} else {
		    	s = scalar_exp( l->h->next->next->data.sym, 
					sql, tables, groupby);
		}
		if (distinct) s = statement_unique(s);
		if (s && a){
			return statement_aggr(s, a, (groupby)?1:0);
		} else {
			printf("aggr %s missing\n", l->h->data.sval );
		}
		return NULL;
	} break;
		 /* atom's */
	case SQL_PARAMETER:
			printf("parameter not used\n");
			break;
	case SQL_ATOM:
			return statement_atom( atom_dup(se->data.aval) );
	default: 	
			printf("unknown %d %s\n", se->token, token2string(se->token));
	}
	return NULL;
}

static
statement *column_exp( context *sql, list *tables, symbol *column_e, symbol *groupby ){

	switch(column_e->token){
	case SQL_TABLE: {

		symbol *tsym = column_e->data.lval->h->data.sym;
		table *t = sql->cat->bind_table(sql->cat,  
				sql->cat->cur_schema, tsym->data.sval );

		/* needs more work */
		if (t){
			list *columns = list_create();
			node *n = t->columns->h; 
			while(n){
				list_append_statement(columns, 
					statement_column(n->data.cval) );
				n = n->next;
			}
			return statement_list(columns);
		}
	} break;
	case SQL_COLUMN:
	{
		dlist *l = column_e->data.lval;
		statement *s = scalar_exp(l->h->data.sym, sql, tables, groupby);
		if (l->h->next->data.sval){
			s = statement_name(s, l->h->next->data.sval);
		} 
		return s;
	} break;
	default:
		snprintf(sql->errstr, ERRSIZE, 
		  _("Column expression Symbol(%d)->token = %s"),
		  (int)column_e->token, token2string(column_e->token));
		return NULL;
	}
	return NULL;
}

statement *subquery( context *sql, symbol *sq) {
	dlist *q = sq->data.lval;
	assert( sq->token == SQL_SELECT );
	return 
	query( sql, q->h->data.ival, 
		  q->h->next->data.lval,
	          q->h->next->next->data.lval, 
	          q->h->next->next->next->data.lval, 
	          q->h->next->next->next->next->data.sym );
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
				table *st = s->h;

				if (t->nrcols == 1 && st->id == t->h->id){
					s = statement_semijoin(s,t);
				}
				if (t->nrcols == 2){ 
				  if( st->id == t->h->id ){
					s = statement_semijoin(t,s);
				  } else if( st->id == t->t->id ){
					statement *r = statement_reverse(t);
					s = statement_semijoin(r,s);
				  }
				}
			} else if (s->nrcols == 2){
				table *st = s->h;
				if (t->nrcols == 1){ 
				  if( st->id == t->h->id ){
					s = statement_semijoin(s,t);
				  } else if( s->t->id == t->h->id){
					s = statement_reverse(s);
					s = statement_semijoin(s,t);
				  } 
				} else if (t->nrcols == 2){ 
			          if (st->id == t->h->id && 
				      s->t->id == t->t->id){
					  s = statement_intersect(s,t);
				  } else 
			          if (st->id == t->t->id &&  
				      s->t->id == t->h->id){
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
statement *search_condition( symbol *sc, context *sql, list *tables ){
	switch(sc->token){
	case SQL_OR: {
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		statement *ls = search_condition( lo, sql, tables );
		statement *rs = search_condition( ro, sql, tables );
		if (!ls && !rs) return NULL;
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
		statement *ls = search_condition( lo, sql, tables );
		statement *rs = search_condition( ro, sql, tables );
		if (!ls && !rs) return NULL;
		if (ls->type != st_diamond && ls->type != st_pearl){
			ls = statement_diamond( ls ); 
		}
		if (rs->type != st_diamond && rs->type != st_pearl){
			rs = statement_diamond( rs ); 
		}
		if (ls->type == st_diamond && rs->type == st_diamond){
			list_merge( ls->op1.lval, rs->op1.lval );	
			ls->op1.lval = query_and( sql->cat, ls->op1.lval);
		} else if (ls->type == st_pearl && rs->type == st_diamond){
			list_map(  ls->op1.lval, 
				(map_func)&list_map_merge, rs->op1.sval);
		} else if (ls->type == st_diamond && rs->type == st_pearl){
			list_map(  rs->op1.lval, 
				(map_func)&list_map_merge, ls->op1.sval);
			ls = rs;
		} else if (ls->type == st_pearl && rs->type == st_pearl){
			list_map(  ls->op1.lval, 
				(map_func)&list_map_merge, rs->op1.sval);
		}
		return ls;
	} break;
	case SQL_NOT: {
		symbol *o = sc->data.lval->h->data.sym;
		statement *s = search_condition( o, sql, tables );
		return s;
	} break;
	case SQL_COMPARE: {
		int join = 1;
		int type = 0;
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->next->data.sym;
		char *compare_op = sc->data.lval->h->next->data.sval;
		statement *rs, *ls = scalar_exp(lo, sql, tables, NULL);
		if (ro->token != SQL_SELECT){
	       		rs = scalar_exp(ro, sql, tables, NULL);
		} else {
			rs = subquery(sql, ro);
		}
		if (!ls || !rs){
			snprintf(sql->errstr, ERRSIZE, 
		  	_("Compare(%s) missing left or right handside"),
			compare_op);
		       	return NULL;
		}
		if (ls->h == NULL && rs->h == NULL){
			snprintf(sql->errstr, ERRSIZE, 
		  	_("Compare(%s) between two atoms is not possible"),
			compare_op);
		       	return NULL;
		} else if ((ls->h != NULL && rs->h == NULL) ||
		           (ls->h == NULL && rs->h != NULL)) {
			join = 0;
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
			rs = statement_reverse( rs );
			return statement_join( ls, rs, type );
		} else {
			return statement_select( ls, rs, type );
		}
	} break;
	case SQL_BETWEEN: {
		symbol *lo  = sc->data.lval->h->data.sym;
		symbol *ro1 = sc->data.lval->h->next->data.sym;
		symbol *ro2 = sc->data.lval->h->next->next->data.sym;
		statement *ls = scalar_exp(lo, sql, tables, NULL);
		statement *rs1 = scalar_exp(ro1, sql, tables, NULL);
		statement *rs2 = scalar_exp(ro2, sql, tables, NULL);
		if (rs1->type != st_atom || rs2->type != st_atom){
			snprintf(sql->errstr, ERRSIZE, 
		  	_("Between requires an atom on the right handside"));
		       	return NULL;
		}
		return statement_select2( ls, rs1, rs2 );
	}
	case SQL_NOT_BETWEEN: {
		symbol *lo  = sc->data.lval->h->data.sym;
		symbol *ro1 = sc->data.lval->h->next->data.sym;
		symbol *ro2 = sc->data.lval->h->next->next->data.sym;
		statement *ls = scalar_exp(lo, sql, tables, NULL);
		statement *rs1 = scalar_exp(ro1, sql, tables, NULL);
		statement *rs2 = scalar_exp(ro2, sql, tables, NULL);
		if (rs1->type != st_atom || rs2->type != st_atom){
			snprintf(sql->errstr, ERRSIZE, 
		  	_("Between requires an atom on the right handside"));
		       	return NULL;
		}
		return statement_select2( ls, rs1, rs2 );
	}
	case SQL_LIKE: {
		symbol *lo  = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		statement *ls = scalar_exp(lo, sql, tables, NULL);
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
		  	_("Wrong type used with LIKE statement, should be string %s %s"), atom2string(a), atomtype2string(a));
		       	return NULL;
		}
		return statement_like( ls, statement_atom(atom_dup(a)) );
	}
	case SQL_NOT_LIKE: {
		symbol *lo  = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		statement *ls = scalar_exp(lo, sql, tables, NULL);
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
		if (a->type != type_string){
			snprintf(sql->errstr, ERRSIZE, 
		  	_("Wrong type used with LIKE statement, should be string"));
		       	return NULL;
		}
		return statement_like( ls, statement_atom(atom_dup(a)) );
	}
	case SQL_IN: {
		dlist *l = sc->data.lval;
		symbol *lo  = l->h->data.sym;
		statement *ls = scalar_exp(lo, sql, tables, NULL);
		if (l->h->next->type == type_list){
			dnode *n = l->h->next->data.lval->h;
			list *nl = list_create();
			while(n){
				list_append_atom(nl, atom_dup(n->data.aval));
				n = n->next;
			}
			return statement_exists( ls, nl);
		} else if (l->h->next->type == type_symbol){
			statement *r1 = statement_reverse(ls);
			statement *rs = subquery(sql, l->h->next->data.sym);
			statement *r2 = statement_reverse( rs );
			statement *s = statement_semijoin(r2,r1);
			return statement_reverse(s);
		} else {
			snprintf(sql->errstr, ERRSIZE, 
		  	_("In missing inner query"));
			return NULL;
		}
	} break;
	case SQL_NOT_IN: {
		dlist *l = sc->data.lval;
		symbol *lo  = l->h->data.sym;
		statement *ls = scalar_exp(lo, sql, tables, NULL);
		if (l->h->next->type == type_list){
			dnode *n = l->h->next->data.lval->h;
			list *nl = list_create();
			while(n){
				list_append_atom(nl, atom_dup(n->data.aval));
				n = n->next;
			}
			return statement_exists( ls, nl);
		} else if (l->h->next->type == type_symbol){
			statement *r1 = statement_reverse(ls);
			statement *rs = subquery(sql, l->h->next->data.sym);
			statement *r2 = statement_reverse( rs );
			statement *s = statement_semijoin(r2,r1);
			return statement_reverse(s);
		} else {
			snprintf(sql->errstr, ERRSIZE, 
		  	_("In missing inner query"));
			return NULL;
		}
	} break;
	default:
		snprintf(sql->errstr, ERRSIZE, 
		_("Predicate: time to implement some more"));
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
				table *t = p->data.stval->t;
				if (t->id == st->h->id){
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
				  pivots = npivots;
				  n = list_remove( l, n );
				  break;
				} else
				if (t->id == st->t->id){
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
			snprintf(sql->errstr, ERRSIZE, 
		  	  _("Semantically incorrect query, unrelated tables"));
			return NULL;
		}
	}
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
		int markid = 1000; /* 1000 offset to easily see the difference
				      with normal marks */
		statement *pivots = diamond2pivot(sql, n->data.lval);
		list *cur = pivots->op1.lval;
		n = n->next;
		while(n){
			statement *npivots = diamond2pivot(sql, n->data.lval);
			list *l = npivots->op1.lval;
			list *inserts = list_create();
			/*
			statement *g = NULL;
			*/

			node *m = l->h;
			while(m){
				node *c = cur->h;
				while(c){
				    	if (c->data.stval->t->id == 
					    m->data.stval->t->id){
					    list_append_statement( inserts,
					      statement_insert_column(
					       c->data.stval,
					     	statement_remark( 
					         m->data.stval, 
						 statement_count(c->data.stval),
						 markid
						 )));
				    	}
				    	c = c->next;
				}
				m = m->next;
			}
			cur = inserts;
			/*
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
			n = n->next;
			markid ++;
		}
		return statement_list(cur);
	}
	return NULL;
}

static
statement *query_groupby( context *sql, list *tables, symbol *groupby, statement *st ){
	list *l = list_create();
	statement *cur = NULL;
	dnode *o = groupby->data.lval->h;
	node *p;
	while(o){
	    node *n = st->op1.lval->h;
	    symbol *group = o->data.sym;
	    column *c = column_ref(sql->cat, tables, group );
	    while(n){
		    statement *s = n->data.stval;
		    if (s->t->id == c->table->id ){
			statement *j = statement_join( s, statement_column(c), 
						cmp_equal);
			if (cur) cur = statement_derive( cur, j );
			else cur = statement_group( j );
			break;
		    }
		    n = n->next;
	    }
	    o = o->next;
	}
	p = st->op1.lval->h;
	while(o){
		statement *s = p->data.stval;
		list_append_statement(l, statement_join( cur, s, cmp_equal ));
		p = p->next;
	}
	return statement_list(l);
}

static
statement *query_orderby( context *sql, list *tables, symbol *orderby, statement *st ){
	list *l = list_create();
	statement *cur = NULL;
	dnode *o = orderby->data.lval->h;
	node *p;
	while(o){
	    node *n = st->op1.lval->h;
	    symbol *order = o->data.sym;
	    if (order->token == SQL_COLUMN){
		symbol *col = order->data.lval->h->data.sym;
		int direction = order->data.lval->h->next->data.ival;
		column *c = column_ref(sql->cat, tables, col );
	        while(n){
		    statement *s = n->data.stval;
		    if (s->t->id == c->table->id ){
			statement *j = statement_join( s, statement_column(c), 
						cmp_equal);
			if (cur) cur = statement_reorder( cur, j, direction);
			else cur = statement_order( j, direction );
			break;
		    }
		    n = n->next;
	        }
	    } else {
		printf("order not of type SQL_COLUMN\n" );
	    }
	    o = o->next;
	}
	/*
	cur = statement_mark(cur);
	*/
	p = st->op1.lval->h;
	while(p){
		statement *s = p->data.stval;
		list_append_statement(l, statement_join( cur, s, cmp_equal ));
		p = p->next;
	}
	return statement_list(l);
}

static
statement *substitute( statement *s, statement *c ){
	switch( s->type){
	case st_unique: return statement_unique( c );
	case st_column: return c;
	case st_atom: return s;
	case st_aggr: return statement_aggr( substitute( s->op1.stval, c), 
				      s->op2.aggrval, s->flag );
	case st_binop: return statement_binop( substitute( s->op1.stval, c), 
				       s->op2.stval, s->op3.funcval );
	default:
		      return s;
	}
	return s;
}

static
statement *query( context *sql, int distinct, dlist *selection, dlist *into, 
		dlist *table_exp , symbol *orderby ) {

  statement *s = NULL;

  dlist *from = table_exp->h->data.sym->data.lval;
  list *ftables = NULL;
  list *rl = list_create();
  symbol *where = table_exp->h->next->data.sym;
  symbol *groupby = table_exp->h->next->next->data.sym;
  symbol *having = table_exp->h->next->next->next->data.sym;
  (void)having;

  if (from){ 
	  /* keep lists with tables and with (names) */
	  table *p = (table*)1;
	  dnode *n = from->h;

	  for( ftables = list_create(); (n && p); n = n->next ){
		var *v = NEW(var);
		p = v->t = table_ref( sql, n->data.sym);
		v->vname = table_ref_var( sql, n->data.sym);

	  	list_append_string( ftables, (char*)v);
	  }

	  if (!p){ 
		symbol *s = (n)?n->data.sym:NULL;
		if (s && s->token == SQL_NAME)
			s = s->data.lval->h->data.sym;
		snprintf(sql->errstr, ERRSIZE,
		  _("Unknown table %s"), (s)
			  ?table_name(s->data.lval):"");
		  return NULL;
	  }
  } else {
	  node *n = sql->cat->cur_schema->tables->h;

	  for( ftables = list_create(); (n); n = n->next ){
		var *v = NEW(var);
		v->t = n->data.tval;
		v->vname = n->data.tval->name;

	  	list_append_string( ftables, (char*)v);
	  }
  }


  if (where){
	s = search_condition(where, sql, ftables);
  } else {
	if (list_length(ftables) > 1){
		snprintf(sql->errstr, ERRSIZE, 
		  _("Subquery over multiple tables misses where condition"));
		return NULL;
	}
	if (list_length(((var*)ftables->h->data.sval)->t->columns) < 1 ){
		snprintf(sql->errstr, ERRSIZE, 
		  _("Subquery table %s has no columns"), 
		  ftables->h->data.tval->name);
		return NULL;
	}
  	s = statement_column(((var*)ftables->h->data.sval)->t->columns->h->data.cval);
  }

  if (s){
	if (s->type != st_diamond && s->type != st_pearl)
		s = statement_diamond(s);
	if (s->type == st_pearl){
		s = pearl2pivot(sql, s->op1.lval);
	} else {
	  	s = diamond2pivot(sql, s->op1.lval);
	}

  	if (groupby) s = query_groupby(sql, ftables, groupby, s );
  	if (orderby) s = query_orderby(sql, ftables, orderby, s );
  }

  if (s){
  	if (selection){
		/* todo fix: remove need for substitute, ie. allready
		   do the join during  the column_exp */
	  dnode *n = selection->h;
	  while (n){
		statement *cs = column_exp(sql, ftables, n->data.sym, groupby);
		node *m = s->op1.lval->h;
		while(m){
			statement *ss = m->data.stval;
			if (cs->h == ss->t){
				statement *j, *co = cs;
				while(cs->type != st_column) cs = cs->op1.stval;
				j = statement_join(ss, cs, cmp_equal );
				/* todo add ordering /groupby */
				cs = substitute(co,j);
				list_append_statement(rl, cs);
			}
			m = m->next;
		}
		n = n->next;
	  }
  	} else {
	  /* select * from single table */
	    table *t = ((var*)ftables->h->data.sval)->t;
            node *n = t->columns->h;
	    while(n){
		column *cs = n->data.cval;
		node *m = s->op1.lval->h;
		if(m){
			statement *ss = m->data.stval;
			list_append_statement(rl, 
			  statement_join(ss, statement_column(cs), cmp_equal));
		}
		n = n->next;
	    }
	}
	s = statement_list(rl);
  }

  if (!s && sql->errstr[0] == '\0')
	  snprintf(sql->errstr, ERRSIZE, _("Subquery result missing")); 
  return s;
}

static
statement *create_view( context *sql, schema *schema, dlist *qname, 
		dlist *column_spec, symbol *query, int check){

	catalog *cat = sql->cat;
	char *name = table_name(qname);

	if (cat->bind_table(cat, schema, name)){
		snprintf(sql->errstr, ERRSIZE, 
			_("Create View name %s allready in use"), name);
		return NULL; 
	} else {
		table *table = cat->create_table( cat, 0, schema, name, 0, 
					query->sql);
		statement *st = statement_create_table( table );
		list *newcolumns = list_append_statement(list_create(), st );
		statement *s = subquery( sql, query);

		if (!s) return NULL;
		if (column_spec){
			int seqnr = 0;
			dnode *n = column_spec->h;
			node *m = s->op1.lval->h;
			while(n){
				char *cname = n->data.sval;
				statement *st = m->data.stval;
				char *ctype = column_type(st);
				column *col = cat->create_column( cat, 0,  
					table, cname, ctype, "NULL", 1, seqnr);
				list_append_statement( newcolumns, 
				  statement_create_column(col));
				col->s = st;
				n = n->next;
				m = m->next;
				seqnr++;
			}
		} else {
			int seqnr = 0;
			node *m = s->op1.lval->h;

			while(m){
				statement *st = m->data.stval;
				char *cname = column_name(st);
				char *ctype = column_type(st);
				column *col =cat->create_column( cat, 0,
					table, cname, ctype, "NULL", 1, seqnr);
				list_append_statement( newcolumns, 
				  statement_create_column(col));
				col->s = st;
				m = m->next;
				seqnr++;
			}
		}
		return statement_list(newcolumns);
	}
	return NULL;
}

static
statement *column_option( catalog *cat, symbol *s, column *c, statement *cc ){
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
statement *create_column( catalog *cat, symbol *s, int seqnr, table *table ){
	statement *res = NULL;

	switch(s->token){
	case SQL_COLUMN: { 
		  dlist *l = s->data.lval;
		  char *cname = l->h->data.sval;
		  type *ctype = cat->bind_type( cat, l->h->next->data.sval);
		  dlist *opt_list = l->h->next->next->data.lval;
		  if (cname && ctype){
		  	column *c = cat->create_column( cat, 0, table, 
				cname, ctype->sqlname, "NULL", 1, seqnr);
			res = statement_create_column(c);
		  	if (opt_list){
				dnode *n = opt_list->h;
				while(n){
				   res = column_option(cat,n->data.sym, c, res);
				   n = n->next;
				}
			}
		  } else {
			printf("create_column: type or name missing\n");
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

	if (cat->bind_table(cat, schema, name)){
		snprintf(sql->errstr, ERRSIZE, 
			_("Create Table name %s allready in use"), name);
		return NULL; 
	} else {
		table *table = cat->create_table( cat, 0, schema, name, temp, 
					NULL);
		statement *st = statement_create_table( table );
		list *newcolumns = list_append_statement(list_create(), st );
		dnode *n = columns->h; 
		int seqnr = 0;

		while(n){
			list_append_statement(newcolumns, 
			   create_column( cat, n->data.sym, seqnr++, table ) );
			n = n->next;
		}
		return statement_list(newcolumns);
	}
}

static
statement * drop_table( context *sql, dlist *qname, int drop_action ){
	char *tname = table_name(qname);
	table *t = sql->cat->bind_table(sql->cat, sql->cat->cur_schema, tname);

	if (!t){
		snprintf(sql->errstr, ERRSIZE, 
			_("Drop Table, table %s unknown"), tname);
	} else {
		sql->cat->destroy_table( sql->cat, sql->cat->cur_schema, tname);
		return statement_drop_table(t, drop_action);
	}
	return NULL;
}

static
statement * alter_table( context *sql, dlist *qname, symbol *table_element){
	catalog *cat = sql->cat;
	char *name = table_name(qname);
	table *table = NULL;

	if ((table = cat->bind_table(cat, sql->cat->cur_schema, name)) == NULL){
		snprintf(sql->errstr, ERRSIZE, 
			_("Alter Table name %s doesn't exist"), name);
		return NULL; 
	} else {
		int seqnr = list_length(table->columns);
		statement *c = create_column( cat, 
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
	if (cat->bind_schema(cat,name)){
		snprintf(sql->errstr, ERRSIZE, 
			_("Create Schema name %s allready in use"), name);
		return NULL; 
	} else {
		schema *schema = cat->create_schema( cat, 0, name, auth );
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


statement *check_types( context *sql, column *c, statement *s ){
	char *stype = column_type(s);

	if (stype){
		type *st = sql->cat->bind_type( sql->cat, stype );
		if (st){
			type *t1 = c->tpe;
			type *t = st;

			while( t && strcmp(t->sqlname, t1->sqlname ) != 0 ){
				t = t->cast;
			}
			if( !t || strcmp(t->sqlname, t1->sqlname ) != 0 ){
				snprintf(sql->errstr, ERRSIZE, 
	 			_("Types %s and %s are not equal" ), 
	 			(t)?t->sqlname:"unknown", t1->sqlname);
				_DELETE(stype);
				return NULL;
			} else if (t != st){
				_DELETE(stype);
				return statement_cast( t1->name, s );
			}
		} else {
			snprintf(sql->errstr, ERRSIZE, _("Unknown type %s" ), 
				stype );
			_DELETE(stype);
			return NULL;
		}
	} else {
		snprintf(sql->errstr, ERRSIZE, _("Unknown type" ) );
		return NULL;
	}
	return s;
}

statement *insert_value( context *sql, column *c, statement *id, symbol *s ){
	if (s->token == SQL_ATOM){
		statement *a = statement_insert_atom( atom_dup(s->data.aval) );
		if (!(/*a = */check_types( sql, c, a ))) return NULL;
		return statement_insert( c, id, a );
	} else if (s->token == SQL_NULL) {
		return statement_insert( c, id, NULL);
	}
	return NULL;
}

statement *insert_into( context *sql, dlist *qname, dlist *columns, 
			symbol *val_or_q){
	catalog *cat = sql->cat;
	char *tname = table_name(qname);
	table *t = cat->bind_table( cat,  sql->cat->cur_schema, tname );
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
			column *c = cat->bind_column(cat, t, n->data.sval );
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
		list *l = list_create();
		statement *id = (m)?statement_count( 
					statement_column(m->data.cval) ):NULL;
		while(n && m && id){
		  statement *iv = insert_value( sql, m->data.cval, id, n->data.sym);

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
	    statement *s = subquery(sql, val_or_q);
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
		             statement_column(m->data.cval), n->data.stval));
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
	table *t = sql->cat->bind_table( sql->cat,  sql->cat->cur_schema, tname );

	if (!t){
		snprintf(sql->errstr, ERRSIZE, 
			_("Updating non existing table %s"), tname);
	} else {
		dnode *n;
		list *l = list_create();
		var *v = NEW(var);
		list *tables;
	        
		v->t = t;
		v->vname = t->name;
		tables	= list_append_string( list_create() ,(char*)v); 

		if (opt_where) 
			s = search_condition(opt_where, sql, tables);

		n = assignmentlist->h;
		while (n){
			dlist *assignment = n->data.sym->data.lval;
			column *cl = sql->cat->bind_column(
					sql->cat, t, assignment->h->data.sval);
			if (!cl){
				snprintf(sql->errstr, ERRSIZE, 
				  _("Updating non existing column %s.%s"), 
				  	tname, assignment->h->data.sval);
				return NULL;
			} else {
				symbol *a = assignment->h->next->data.sym;
                                statement *sc =
                                    scalar_exp( a, sql, tables, NULL);

				sc = check_types( sql, cl, sc );
				if (!sc) return NULL;
                                if (sc->nrcols > 0){
                                        statement *j, *co = sc;
                                        while(sc->type != st_column)
                                                sc = sc->op1.stval;
                                        j = statement_semijoin(sc, s );
                                        sc = substitute( co, j );
                                } else { /* constant case */
                                        sc = statement_const( s, sc);
                                }
                                list_append_statement( l,
                                        statement_update( cl, sc ));
			}
			n = n->next;
		}
		return statement_list(l);
	}
	return NULL;
}

statement *delete_searched( context *sql, dlist *qname, symbol *opt_where){
	char *tname = table_name(qname);
	table *t = sql->cat->bind_table( sql->cat,  sql->cat->cur_schema, tname );

	if (!t){
		snprintf(sql->errstr, ERRSIZE, 
			_("Deleting from non existing table %s"), tname);
	} else {
		statement *s = NULL;
		node *n;
		list *l = list_create();
		var *v = NEW(var);
		list *tables;
	        
		v->t = t;
		v->vname = t->name;
		tables	= list_append_string( list_create() ,(char*)v); 

		if (opt_where) 
			s = search_condition(opt_where, sql, tables);

		n = t->columns->h;
		while (n){
			column *cl = n->data.cval;
			list_append_statement( l, statement_delete( cl, s ));  
			n = n->next;
		}
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
/*			ret = statement_dummy();*/
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
			ret = subquery( sql, s);
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


list *semantic( context *s,  dlist *sqllist ){
        int errors = 0;
        list *sl = list_create();
        dnode *n = sqllist->h;
        while(n){
	    if (n->data.sym){
                statement *res;
                if ( (res = sql_statement( s, n->data.sym)) == NULL){
                        fprintf(stderr, "Semanic error: %s\n", s->errstr);
                        fprintf(stderr, "in %s line %d: %s\n",
                                n->data.sym->filename, n->data.sym->lineno,
                                n->data.sym->sql );
                        errors++;
                } else {
                        list_append_statement(sl, res );
                }
	    }
            n = n->next;
        }
        if (errors == 0)
                return sl;
	else
		list_destroy(sl);
        return NULL;
}


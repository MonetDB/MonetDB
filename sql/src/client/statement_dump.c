
#include "mem.h"
#include "statement.h"


static
char *atom_dump( atom *a){
	char buf[1024];
	switch (a->type){
	case int_value: sprintf(buf, "%d", a->data.ival); break;
	case string_value: sprintf(buf, "\"%s\"", a->data.sval); break;
	case float_value: sprintf(buf, "%f", a->data.dval); break;
	case general_value:
			if (a->data.sval)
			  sprintf(buf, "%s(\"%s\")", 
				a->tpe->name, a->data.sval );
			else 
			  sprintf(buf, "%s(nil)", a->tpe->name );
			break;
	}
	return _strdup(buf);
}

static
char *atom_dump_fast( atom *a){
	char buf[BUFSIZ];
	switch (a->type){
	case int_value: sprintf(buf, "%d", a->data.ival); break;
	case string_value: sprintf(buf, "\1%s\1", a->data.sval); break;
	case float_value: sprintf(buf, "%f", a->data.dval); break;
	case general_value:
			if (a->data.sval)
			  sprintf(buf, "%s", 
				a->tpe->name, a->data.sval );
			else 
			  sprintf(buf, "nil", a->tpe->name );
			break;
	}
	return _strdup(buf);
}


int statement_dump( statement *s, int *nr, context *sql ){
    char buf[BUFSIZ+1];
    int len = 0;

    assert (*nr);

    buf[0] = '\0';
    if (s){
    	if (s->nr) return s->nr;

	if (sql->debug&2 && s->type != st_insert_list)
		len += snprintf( buf+len, BUFSIZ,"t0 := time();\n");

	switch(s->type){
	case st_create_schema: {
		schema *schema = s->op1.schema;
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_create_schema(myc, %ld, \"%s\", \"%s\");\n", 
			*nr, schema->id, schema->name, schema->auth );
	} break;
	case st_create_table: {
		table *t = s->op1.tval;
		if (t->sql){
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_create_view(myc, %ld, %ld, \"%s\", \"%s\");\n", 
			*nr, t->id, t->schema->id, t->name, t->sql );
		} else {
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_create_table(myc, %ld, %ld, \"%s\", %s);\n",
		   	*nr, t->id, t->schema->id, t->name, 
		   		(t->temp==0)?"false":"true" );
		}
		s->nr = (*nr)++;
	} break;
	case st_drop_table: {
		table *t = s->op1.tval;
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_drop_table(myc, %ld, %s);\n", 
			*nr, t->id, (s->flag==0)?"false":"true" );
		s->nr = (*nr)++;
	} break;
	case st_create_column: {
		column *c = s->op1.cval;
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_create_column(myc, %ld, %ld, \"%s\", \"%s\", %d);\n",
			*nr, c->id, c->table->id, c->name, 
			c->tpe->sqlname, c->colnr );
		s->nr = (*nr)++;
	} break;
	case st_not_null: {
		int c = statement_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		    "s%d := mvc_not_null(myc, s%d );\n", *nr, c );
		s->nr = (*nr)++;
	} break;
	case st_default: {
		int c = statement_dump( s->op1.stval, nr, sql );
		int d = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		    "s%d := default_val(myc, s%d, s%d );\n", *nr, c, d );
		s->nr = (*nr)++;
	} break;
	case st_select: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		switch(s->flag){
		case cmp_equal:
			if (s->op3.stval){
			    int r2 = statement_dump( s->op3.stval, nr, sql );
			    len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.uselect(s%d, s%d);\n", 
				*nr, l, r, r2 ); 
			} else {
			    len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.uselect(s%d);\n", *nr, l, r ); 
			}
			s->nr = (*nr)++;
			break;
		case cmp_notequal:
			len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.uselect(s%d);\n", *nr, l, r ); 
			len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.kdiff(s%d);\n", *nr+1, l, *nr );
			(void)(*nr)++; s->nr = (*nr)++;
			break;
		case cmp_lt:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.mil_select(\"<in>\", %s(nil), s%d);\n", 
			  *nr, l, tail_type(s)->name, r ); 
			s->nr = (*nr)++;
			break;
		case cmp_lte:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.uselect(%s(nil), s%d);\n", 
			  *nr, l, tail_type(s)->name, r ); 
			s->nr = (*nr)++;
			break;
		case cmp_gt:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.mil_select(\"<in>\", s%d, %s(nil));\n", 
			  *nr, l, r, tail_type(s)->name ); 
			s->nr = (*nr)++;
			break;
		case cmp_gte: 
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.uselect(s%d, %s(nil));\n", 
			  *nr, l, r, tail_type(s)->name ); 
			s->nr = (*nr)++;
			break;
		default:
			len += snprintf( buf+len, BUFSIZ, "error impossible\n");
	  	} 
	} break;
	case st_select2: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r1 = statement_dump( s->op2.stval, nr, sql );
		int r2 = statement_dump( s->op3.stval, nr, sql );
		switch(s->flag){
		case cmp_equal: len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.select(s%d, s%d);\n", 
			  *nr, l, r1, r2 ); 
			s->nr = (*nr)++;
			break;
		case cmp_notequal: 
			len += snprintf( buf+len, BUFSIZ,
			  "s%d := s%d.select(s%d, s%d);\n", *nr, l, r1, r2 ); 
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.kdiff(s%d);\n", *nr+1, l, *nr );
			(void)(*nr)++; s->nr = (*nr)++;
			break;
		}
	} break;
	case st_like: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.likeselect(s%d);\n", *nr, l, r ); 
		s->nr = (*nr)++;
	} break;
	case st_semijoin: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.semijoin(s%d);\n", *nr, l, r ); 
		s->nr = (*nr)++;
	} break;
	case st_diff: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.kdiff(s%d);\n", *nr, l, r ); 
		s->nr = (*nr)++;
	} break;
	case st_intersect: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.sintersect(s%d);\n", *nr, l, r ); 
		s->nr = (*nr)++;
	} break;
	case st_union: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.kunion(s%d);\n", *nr, l, r ); 
		s->nr = (*nr)++;
	} break;
	case st_join: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		switch(s->flag){
		case cmp_equal:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d);\n", *nr, l, r ); 
			s->nr = (*nr)++;
			break;
		case cmp_notequal:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d, \"!=\");\n", *nr, l, r ); 
			s->nr = (*nr)++;
			break;
		case cmp_lt:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d, \"<\");\n", *nr, l, r ); 
			s->nr = (*nr)++;
			break;
		case cmp_lte: 
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d, \"<=\");\n", *nr, l, r );
			s->nr = (*nr)++;
			break;
		case cmp_gt: 
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d, \">\" );\n", *nr, l, r); 
			s->nr = (*nr)++;
			break;
		case cmp_gte: 
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d, \">=\" );\n", *nr, l, r);
			s->nr = (*nr)++;
			break;
		default:
			len += snprintf( buf+len, BUFSIZ, "error impossible\n");
	  	} break;
	}
	case st_column:
		if (s->op1.cval->s){
			s->nr = statement_dump( s->op1.cval->s, nr, sql );
		} else {
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := mvc_bind(myc, %ld); # %s.%s\n", 
			  *nr, s->op1.cval->id, s->op1.cval->table->name, 
			  s->op1.cval->name );

			s->nr = (*nr)++;
			if (sql->debug&4){
				len += snprintf( buf+len, BUFSIZ, 
				"s%d.print();\n", s->nr );
			}
		}
		break;
	case st_reverse: {
		int l = statement_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.reverse();\n", *nr, l);
		s->nr = (*nr)++;
	} 	break;
	case st_count: {
		int l = statement_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.count();\n", *nr, l);
		s->nr = (*nr)++;
	} 	break;
	case st_const: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		/*len += snprintf( buf+len, BUFSIZ, 
		 * "s%d := [ s%d ~ s%d ];\n", *nr, l, r);*/
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.project(s%d);\n", *nr, l, r);
		s->nr = (*nr)++;
	} 	break;
	case st_mark: {
		int l = statement_dump( s->op1.stval, nr, sql );
		if (s->op2.stval){
			int r = statement_dump( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.reverse().mark(oid(s%d)).reverse();\n", 
			  *nr, l, r);
		} else if (s->flag >= 0){
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.reverse().mark(oid(%d)).reverse();\n", 
			  *nr, l, s->flag);
		} else {
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.reverse().mark().reverse();\n", *nr, l);
		}
		s->nr = (*nr)++;
	} 	break;
	case st_group: {
		int l = statement_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.group();\n", *nr, l);
		s->nr = (*nr)++;
	} 	break;
	case st_derive: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.group(s%d);\n", *nr, l, r);
		s->nr = (*nr)++;
	} 	break;
	case st_unique: {
		int l = statement_dump( s->op1.stval, nr, sql );
		if (s->op2.stval){
			int r = statement_dump( s->op2.stval, nr, sql );
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.group(s%d);\n", (*nr), l, r);
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.tunique().mirror().join(s%d);\n", 
				(*nr)+1, *nr, r);
			(*nr)++;
		} else if (s->op1.stval->type == st_group || 
		           s->op1.stval->type == st_derive){
			/* dirty optimization, use CThistolinks tunique */
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.tunique().mirror();\n", *nr, l);
		} else {
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.reverse().kunique().reverse();\n", *nr, l);
		}
		s->nr = (*nr)++;
	} 	break;
	case st_order: {
		int l = statement_dump( s->op1.stval, nr, sql );
		/*
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.order(%s);\n", *nr, l, 
				s->flag?"desc":"asc");
				*/
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.reverse().sort().reverse();\n", *nr, l );
		s->nr = (*nr)++;
	} 	break;
	case st_reorder: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.CTrefine(s%d);\n", *nr, l, r); 
		/* s->flag?"desc":"asc"); */
		s->nr = (*nr)++;
	} 	break;
	case st_unop: {
		int l = statement_dump( s->op1.stval, nr, sql );
		if (s->op1.stval->nrcols)
		  len += snprintf( buf+len, BUFSIZ, 
		   "s%d := [%s](s%d);\n", *nr, s->op2.funcval->imp, l );
		else 
		  len += snprintf( buf+len, BUFSIZ, 
		   "s%d := %s(s%d);\n", *nr, s->op2.funcval->imp, l);
		s->nr = (*nr)++;
	} 	break;
	case st_binop: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		if (s->op1.stval->nrcols || s->op2.stval->nrcols ){
		  	if (!s->op1.stval->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ, 
		    		"s%d := [s%d ~ s%d];\n", n, r, l ); 
				l = n;
		  	}
		  	if (!s->op2.stval->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ, 
		    		"s%d := [s%d ~ s%d];\n", n, l, r ); 
				r = n;
		  	}
		  	len += snprintf( buf+len, BUFSIZ, 
		    	"s%d := [%s](s%d,s%d);\n", 
			*nr, s->op3.funcval->imp, l, r );
		} else  {
		  	len += snprintf( buf+len, BUFSIZ, 
		    	"s%d := %s(s%d,s%d);\n", 
			*nr, s->op3.funcval->imp, l,r );
		}
		s->nr = (*nr)++;
	} 	break;
	case st_triop: {
		statement *op1 = s->op1.lval->h->data.stval;
		statement *op2 = s->op1.lval->h->next->data.stval;
		statement *op3 = s->op1.lval->h->next->next->data.stval;
		int r1 = statement_dump( op1, nr, sql );
		int r2 = statement_dump( op2, nr, sql );
		int r3 = statement_dump( op3, nr, sql );
		if (op1->nrcols || op2->nrcols || op3->nrcols){
			int l = 0;
			if (op1->nrcols) l = r1;
			if (op2->nrcols) l = r2;
			if (op3->nrcols) l = r3;
		  	if (!op1->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ, 
		    		"s%d := [ s%d ~ s%d];\n", n, l, r1 ); 
				r1 = n;
		  	}
		  	if (!op2->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ, 
		    		"s%d := [ s%d ~ s%d];\n", n, l, r2 ); 
				r2 = n;
		  	}
		  	if (!op3->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ, 
		    		"s%d := [ s%d ~ s%d];\n", n, l, r3 ); 
				r3 = n;
		  	}
		  	len += snprintf( buf+len, BUFSIZ, 
		    	"s%d := [%s](s%d,s%d,s%d);\n", 
			*nr, s->op2.funcval->imp, r1, r2, r3 );
		} else {
		  len += snprintf( buf+len, BUFSIZ, 
		    "s%d := %s(s%d,s%d,s%d);\n", *nr, s->op2.funcval->imp, 
		    	r1, r2, r3 );
		}
		s->nr = (*nr)++;
	} 	break;
	case st_aggr: {
		int l = statement_dump( s->op1.stval, nr, sql );
		if (s->op3.stval){
			int r = statement_dump( s->op3.stval, nr, sql );

			if (s->op1.stval == s->op3.stval){
				len += snprintf( buf+len, BUFSIZ, 
				"s%d := {%s}(s%d.reverse(), s%d.tunique());\n", 
				*nr, s->op2.aggrval->imp, l, r);
			} else if (s->op1.stval->type == st_column){
				len += snprintf( buf+len, BUFSIZ, 
				"s%d := {%s}(s%d, s%d, s%d.tunique());\n", 
				*nr, s->op2.aggrval->imp, l, r, r);
			} else {
				len += snprintf( buf+len, BUFSIZ, 
				"s%d := {%s}(s%d.reverse().join(s%d),s%d.tunique());\n",
			        *nr, s->op2.aggrval->imp, r, l, r);
			}
		} else {
			len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.%s();\n", 
				*nr, l, s->op2.aggrval->imp );
			len += snprintf( buf+len, BUFSIZ, 
				"s%d := new(oid,%s);\n"
				, *nr+1, s->op2.aggrval->res->name );
			len += snprintf( buf+len, BUFSIZ, 
				"s%d.insert(oid(0),s%d);\n"
				, *nr+1, *nr );
			(*nr)++;
		}
		s->nr = (*nr)++;
	} 	break;
	case st_exists: {
		int l = statement_dump( s->op1.stval, nr, sql );
		node *n = s->op2.lval->h;
		int k = *nr;
		int r = 1;
		if (n){
		  	char *a = (char*)atom_type(n->data.aval )->name;
			len += snprintf( buf+len, BUFSIZ, 
				"s%d := new(%s,oid);\n", *nr, a );
		}
		k++;
		while(n){
			len += snprintf( buf+len, BUFSIZ, "s%d := %s;\n", k, 
				atom_dump(n->data.aval) );
			len += snprintf( buf+len, BUFSIZ, 
				"s%d.insert(s%d, oid(%d));\n", *nr, k++, r++);
			n = n->next;
		}
		len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.join(s%d);\n", k, l, *nr);
		*nr = k;
		s->nr = (*nr)++;
	} 	break;
	case st_atom: {
		len += snprintf( buf+len, BUFSIZ, 
				"s%d := %s;\n", *nr, atom_dump(s->op1.aval));
		s->nr = (*nr)++;
	} break;
	case st_insert_column: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := insert(s%d.access(BAT_WRITE),s%d);\n", *nr, l, r);
		s->nr = (*nr)++;
	} break;
	case st_update: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := [oid](s%d.reverse()).reverse().access(BAT_WRITE).replace(s%d);\n", 
		  *nr, l, r);
		s->nr = (*nr)++;
	} break;
	case st_delete: {
		if (s->op2.stval){
			int l = statement_dump( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_bind(myc, %ld).delete(s%d);\n", 
			*nr, s->op1.cval->id, l);
		} else {
			len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_bind(myc, %ld).clear();\n",
			  *nr, s->op1.cval->id );
		}
		s->nr = (*nr)++;
	} break;
	case st_name: s->nr = statement_dump( s->op1.stval, nr, sql );
		break;
	case st_diamond: {
		int l = 0;
		node *n = s->op1.lval->h;
		while(n){
			l = statement_dump( n->data.stval, nr, sql );
			n = n->next;
		}
		s->nr = l;
	} break;
	case st_pearl: {
		int r;
		node *n = s->op1.lval->h;
		while(n){
			list *l = n->data.lval;
			node *m = l->h;
			while(m){
				r = statement_dump( m->data.stval, nr, sql );
				m = m->next;
			}
			n = n->next;
		}
		s->nr = r;
	} break;
	case st_list: {
		int l = 0;
		node *n = s->op1.lval->h;
		while(n){
			l = statement_dump( n->data.stval, nr, sql );
			n = n->next;
		}
		s->nr = l;
	} break;
	case st_insert_list: {
		node *n = s->op1.lval->h;

		if (!(sql->optimize & SQL_FAST_INSERT)){
			while(n){
				statement *r = n->data.stval;
				statement_dump( r, nr, sql );
				n = n->next;
			}
		} else {
			len += snprintf( buf+len, BUFSIZ, "0,%d,", 
				 	list_length(s->op1.lval) );
			while(n){
				statement *r = n->data.stval;
				len += snprintf( buf+len, BUFSIZ, "%ld,", 
					r->op1.cval->id );
				if (r->op3.stval){
					char *s = NULL;
					statement *a = r->op3.stval;
					while(a->type == st_unop){ /* cast */
						a = a->op1.stval;
					}
					len += snprintf( buf+len, BUFSIZ, "%s,",
					   	s = atom_dump_fast(a->op1.aval) );
					_DELETE(s);
				} else {
					len += snprintf( buf+len, BUFSIZ, 
							"NULL,");
				}
				n = n->next;
			}
			len += snprintf( buf+len, BUFSIZ, "\n" );
		}
		s->nr = (*nr)++;
	} break;
	case st_ordered: {
		int l =  statement_dump( s->op1.stval, nr, sql );
		(void)statement_dump( s->op2.stval, nr, sql );
		s->nr = l;
	} break;
	case st_output: {
		statement *order = NULL;
		statement *lst = s->op1.stval;
		statement_dump( lst, nr, sql );
		if (sql->debug&32){
			len += snprintf( buf+len, BUFSIZ,
			"stream_write(Output,\"0\\n\");stream_flush(Output);\n");
			if (lst->type == st_list){
				list *l = lst->op1.lval;
				node *n = l->h;

				len += snprintf( buf+len, BUFSIZ, "table(\n");
				if (n){
					len += snprintf( buf+len, BUFSIZ,
						"s%d", n->data.stval->nr);
					n = n->next;
				}
				while(n){
					len += snprintf( buf+len, BUFSIZ,
						", s%d", n->data.stval->nr);
					n = n->next;
				}
				len += snprintf( buf+len, BUFSIZ, ");\n");
			}
			break;
		}
		if (lst->type == st_ordered){
			order = lst->op1.stval; 
			lst = lst->op2.stval; 
		}
		if (lst->type == st_list){
			list *l = lst->op1.lval;
			node *n = l->h;
			if (n){
			  if (!order){
			    order = n->data.stval;
			  }
			  len += snprintf( buf+len, BUFSIZ,
				"output_count(s%d, Output);\n", order->nr);
			}
			len += snprintf( buf+len, BUFSIZ,
				"server_output(Output, s%d ", order->nr);
			while(n){
				len += snprintf( buf+len, BUFSIZ,
					", s%d", n->data.stval->nr);
				n = n->next;
			}
			len += snprintf( buf+len, BUFSIZ,");\n");
			len += snprintf( buf+len, BUFSIZ,
					"stream_flush(Output);\n");
		} else {
			fprintf(stderr, "not a valid output list %d %d %d\n",
					lst->type, st_list, st_ordered);
		}
	} break;

	case st_insert: {
                int r = statement_dump( s->op2.stval, nr, sql );
                if (s->op3.stval){
                        int l = statement_dump( s->op3.stval, nr, sql );
                        len += snprintf( buf+len, BUFSIZ,
                        "s%d := mvc_bind(myc, %ld).insert(oid(s%d),s%d);\n",
                        *nr, s->op1.cval->id, r, l);
                } else {
                        len += snprintf( buf+len, BUFSIZ,
                        "s%d := mvc_bind(myc, %ld).insert(oid(s%d),%s(nil));\n",
                        *nr, s->op1.cval->id, r, s->op1.cval->tpe->name );
                }
                s->nr = (*nr)++;
        } break;

	}
	if (sql->debug&2 && s->type != st_insert_list)
		len += snprintf( buf+len, BUFSIZ,"t1 := time(); printf(\"%d %%d\\n\", t1 - t0);\n", s->nr);

	buf[len] = '\0';
	sql->out->write( sql->out, buf, 1, len );

	if (sql->debug&8)
		fwrite( buf, 1, len, stderr);

    	return s->nr;
    }
    return 0;
}


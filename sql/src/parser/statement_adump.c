
#include "mem.h"
#include "statement.h"

int statement_dump( statement *s, int *nr, context *sql ){
    char buf[BUFSIZ+1];
    int len = 0;

    buf[0] = '\0';
    if (s){
    	if (s->nr) return s->nr;
	switch(s->type){
	case st_create_table: {
		if (s->op2.sval){
		  len += snprintf( buf+len, BUFSIZ, "s%d := create_view(\"%s\", \"%s\");\n", 
			*nr, s->op1.sval, s->op2.sval );
		} else {
		  len += snprintf( buf+len, BUFSIZ, "s%d := create_table(\"%s\", %s);\n", 
		   	*nr, s->op1.sval, (s->flag==0)?"false":"true" );
		}
		s->nr = (*nr)++;
	} break;
	case st_drop_table: {
		len += snprintf( buf+len, BUFSIZ, "s%d := drop_table(\"%s\", %s);\n", 
				*nr, s->op1.tval->name, 
				(s->flag==0)?"false":"true" );
		s->nr = (*nr)++;
	} break;
	case st_create_column: {
		int t = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := create_column( s%d, \"%s\", \"%s\", %d);\n", 
				*nr, t, s->op1.cval->name, 
					s->op1.cval->tpe->sqlname,
				        s->op1.cval->colnr );
		s->nr = (*nr)++;
	} break;
	case st_not_null: {
		int c = statement_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := not_null( s%d );\n", *nr, c );
		s->nr = (*nr)++;
	} break;
	case st_default: {
		int c = statement_dump( s->op1.stval, nr, sql );
		int d = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := default( s%d, s%d );\n", *nr, c, d );
		s->nr = (*nr)++;
	} break;
	case st_select: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		switch(s->flag){
		case cmp_equal:
			if (s->op3.stval){
			    int r2 = statement_dump( s->op3.stval, nr, sql );
			    len += snprintf( buf+len, BUFSIZ, "s%d := s%d.select(s%d, s%d);\n", 
					    *nr, l, r, r2 ); 
			} else {
			    len += snprintf( buf+len, BUFSIZ, "s%d := s%d.select(s%d);\n", *nr, l, r ); 
			}
			s->nr = (*nr)++;
			break;
		case cmp_notequal:
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.select(s%d);\n", *nr, l, r ); 
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.kdiff(s%d);\n", *nr+1, l, *nr );
			(void)(*nr)++; s->nr = (*nr)++;
			break;
		case cmp_lt:
		case cmp_lte: /* broken */
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.select(%s(nil), s%d);\n", 
			  *nr, l, s->op1.stval->op1.cval->tpe->name, r ); 
			s->nr = (*nr)++;
			break;
		case cmp_gt:
		case cmp_gte: /* broken */
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.select(s%d, %s(nil));\n", 
			  *nr, l, r, s->op1.stval->op1.cval->tpe->name ); 
			s->nr = (*nr)++;
			break;
		default:
			len += snprintf( buf+len, BUFSIZ, "error impossible\n");
	  	} 
	} break;
	case st_like: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := s%d.like(s%d);\n", *nr, l, r ); 
		s->nr = (*nr)++;
	} break;
	case st_semijoin: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := s%d.semijoin(s%d);\n", *nr, l, r ); 
		s->nr = (*nr)++;
	} break;
	case st_intersect: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := s%d.intersect(s%d);\n", *nr, l, r ); 
		s->nr = (*nr)++;
	} break;
	case st_join: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		switch(s->flag){
		case cmp_equal:
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.join(s%d);\n", *nr, l, r ); 
			s->nr = (*nr)++;
			break;
		case cmp_notequal:
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.join(s%d);\n", *nr, l, r ); 
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.kdiff(s%d);\n", *nr+1, l, *nr );
			(void)(*nr)++; s->nr = (*nr)++;
			break;
		case cmp_lt:
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.join(s%d, \"<\");\n", *nr, l, r ); 
			s->nr = (*nr)++;
			break;
		case cmp_lte: /* broken */
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.join(s%d, \"<=\");\n", *nr, l, r );
			s->nr = (*nr)++;
			break;
		case cmp_gt: 
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.join(s%d, \">\" );\n", *nr, l, r); 
			s->nr = (*nr)++;
			break;
		case cmp_gte: /* broken */
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.join(s%d, \">=\" );\n", *nr, l, r);
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
			len += snprintf( buf+len, BUFSIZ, "s%d := resolve_column(\"%s\",\"%s\");\n", *nr, 
				s->op1.cval->table->name,
				s->op1.cval->name);
			s->nr = (*nr)++;
		}
		break;
	case st_reverse: {
		int l = statement_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := s%d.reverse();\n", *nr, l);
		s->nr = (*nr)++;
	} 	break;
	case st_count: {
		int l = statement_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := s%d.count();\n", *nr, l);
		s->nr = (*nr)++;
	} 	break;
	case st_const: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := [ s%d ~ s%d ];\n", *nr, l, r);
		s->nr = (*nr)++;
	} 	break;
	case st_mark: {
		int l = statement_dump( s->op1.stval, nr, sql );
		if (s->op2.stval){
			int r = statement_dump( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.reverse().mark(oid(s%d)).reverse();\n", *nr, l, r);
		} else {
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.reverse().mark(oid(%d)).reverse();\n", *nr, l, s->flag);
		}
		s->nr = (*nr)++;
	} 	break;
	case st_group: {
		int l = statement_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := s%d.group();\n", *nr, l);
		s->nr = (*nr)++;
	} 	break;
	case st_derive: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := s%d.derive(s%d);\n", *nr, l, r);
		s->nr = (*nr)++;
	} 	break;
	case st_unique: {
		int l = statement_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := s%d.unique();\n", *nr, l);
		s->nr = (*nr)++;
	} 	break;
	case st_order: {
		int l = statement_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := s%d.order(%s);\n", *nr, l, 
				s->flag?"desc":"asc");
		s->nr = (*nr)++;
	} 	break;
	case st_reorder: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := s%d.reorder(s%d,%s);\n", *nr, l, r, 
				s->flag?"desc":"asc");
		s->nr = (*nr)++;
	} 	break;
	case st_unop: {
		int l = statement_dump( s->op1.stval, nr, sql );
		if (s->op1.stval->nrcols)
		  len += snprintf( buf+len, BUFSIZ, "s%d := [%s](s%d);\n", *nr, s->op2.funcval->name, l );
		else 
		  len += snprintf( buf+len, BUFSIZ, "s%d := %s(s%d);\n", *nr, s->op2.funcval->name, l);
		s->nr = (*nr)++;
	} 	break;
	case st_binop: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := [%s](s%d,s%d);\n", *nr, s->op3.funcval->name, l,r );
		s->nr = (*nr)++;
	} 	break;
	case st_aggr: {
		int l = statement_dump( s->op1.stval, nr, sql );
		if (s->flag)
			len += snprintf( buf+len, BUFSIZ, "s%d := {%s}(s%d);\n", 
					*nr, s->op2.aggrval->name, l);
		else 
			len += snprintf( buf+len, BUFSIZ, "s%d := s%d.%s();\n", 
					*nr, l, s->op2.aggrval->name );
		s->nr = (*nr)++;
	} 	break;
	case st_exists: {
		int l = statement_dump( s->op1.stval, nr, sql );
		node *n = s->op2.lval->h;
		int k = *nr;
		if (n){
			char *tpe = atomtype2string(n->data.aval );
			type *t = sql->cat->bind_type( sql->cat, s->op1.sval );
			len += snprintf( buf+len, BUFSIZ, "s%d := bat(oid,%s);\n", *nr, t->name );
		}
		k++;
		while(n){
			len += snprintf( buf+len, BUFSIZ, "s%d := atom(%s);\n", k++, 
					atom2string(n->data.aval) );
			len += snprintf( buf+len, BUFSIZ, "insert(s%d, s%d);\n", *nr, k);
			n = n->next;
		}
		len += snprintf( buf+len, BUFSIZ, "s%d := s%d.semijoin(s%d);\n", k, l, *nr);
		*nr = k;
		s->nr = (*nr)++;
	} 	break;
	case st_atom: {
		len += snprintf( buf+len, BUFSIZ, "s%d := atom(%s);\n", *nr, atom2string(s->op1.aval));
		s->nr = (*nr)++;
	} break;
	case st_cast: {
		int l = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := %s(s%d);\n", *nr, s->op1.sval, l);
		s->nr = (*nr)++;
	} break;
	case st_insert: {
                int r = statement_dump( s->op2.stval, nr, sql );
                if (s->op3.stval){
                        int l = statement_dump( s->op3.stval, nr, sql );
                        len += snprintf( buf+len, BUFSIZ, "s%d := resolve_column(
\"%s\",\"%s\").insert(oid(s%d),s%d);\n", 
                        *nr, s->op1.cval->table->name, s->op1.cval->name, r, l);
                } else {
                        len += snprintf( buf+len, BUFSIZ, "s%d := resolve_column(
\"%s\",\"%s\").insert(oid(s%d),%s(nil));\n",
                        *nr, s->op1.cval->table->name, s->op1.cval->name, r,
			s->op1.cval->tpe->name  );
                }
                s->nr = (*nr)++;
	} break;
	case st_insert_column: {
		int l = statement_dump( s->op1.stval, nr, sql );
		int r = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := insert(s%d,s%d);\n", *nr, l, r);
		s->nr = (*nr)++;
	} break;
	case st_update: {
		int l = statement_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, "s%d := resolve_column(\"%s\",\"%s\").replace(s%d);\n", 
			*nr, s->op1.cval->table->name, s->op1.cval->name, l);
		s->nr = (*nr)++;
	} break;
	case st_delete: {
		if (s->op2.stval){
			int l = statement_dump( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ, "s%d := resolve_column(\"%s\",\"%s\").delete(s%d);\n", 
			   *nr, s->op1.cval->table->name, s->op1.cval->name, l);
		} else {
			len += snprintf( buf+len, BUFSIZ, "s%d := resolve_column(\"%s\",\"%s\").clear();\n",
			  *nr, s->op1.cval->table->name, s->op1.cval->name );
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
	case st_output: {
		statement_dump( s->op1.stval, nr, sql );
		if (s->op1.stval->type == st_list){
			list *l = s->op1.stval->op1.lval;
			node *n = l->h;
			if (n){
				len += snprintf( buf+len, BUFSIZ,"output_count(s%d, Output);\n", n->data.stval->nr);
			}
			len += snprintf( buf+len, BUFSIZ,"server_output(Output ");
			while(n){
				len += snprintf( buf+len, BUFSIZ,", s%d", n->data.stval->nr);
				n = n->next;
			}
			len += snprintf( buf+len, BUFSIZ,");\n");
		}
	} break;
	}
	buf[len] = '\0';
	sql->out->write( sql->out, buf, 1, len );
	/*
	if ( (s->nr%10000) == 0){
		len = 0;
		len += snprintf( buf+len, BUFSIZ, "print(%d);\n", s->nr );
		sql->out->write( sql->out, buf, 1, len );
	}
	*/
    	return s->nr;
    }
    return 0;
}


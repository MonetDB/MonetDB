
#include "mem.h"
#include "statement.h"


static
char *atom_dump( atom *a){
	char buf[BUFSIZ];
	switch (a->type){
	case int_value: snprintf(buf, BUFSIZ, "%d", a->data.ival); break;
	case string_value: snprintf(buf, BUFSIZ, "\"%s\"", a->data.sval); break;
	case float_value: snprintf(buf, BUFSIZ, "%f", a->data.dval); break;
	case general_value:
			if (a->data.sval)
			  snprintf(buf, BUFSIZ, "%s(\"%s\")", 
				a->tpe->name, a->data.sval );
			else 
			  snprintf(buf, BUFSIZ, "%s(nil)", a->tpe->name );
			break;
	}
	return _strdup(buf);
}

static
char *atom_dump_fast( atom *a){
	char buf[BUFSIZ];
	switch (a->type){
	case int_value: snprintf(buf, BUFSIZ, "%d", a->data.ival); break;
	case string_value: snprintf(buf, BUFSIZ, "\1%s\1", a->data.sval); break;
	case float_value: snprintf(buf, BUFSIZ, "%f", a->data.dval); break;
	case general_value:
			if (a->data.sval)
			  snprintf(buf, BUFSIZ, "%s", 
				a->tpe->name, a->data.sval );
			else 
			  snprintf(buf, BUFSIZ, "nil", a->tpe->name );
			break;
	}
	return _strdup(buf);
}


int stmt_dump( stmt *s, int *nr, context *sql ){
    char buf[BUFSIZ+1];
    int len = 0;
    node *n;

    assert (*nr);

    buf[0] = '\0';
    if (s){
    	if (s->nr > 0) return s->nr;

	if (s->nr == 0)
		s->nr = -(*nr)++;

	if (sql->debug&2 && s->type != st_insert)
		len += snprintf( buf+len, BUFSIZ-len,"t0 := time();\n");

	switch(s->type){
	case st_none: break;
	case st_release: 
		len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_release(myc,\"%s\");\n",
			-s->nr, s->op1.sval);
		break;
	case st_commit: {
		char *name = s->op2.sval;
		len += snprintf(buf + len, BUFSIZ-len,
			"s%d := mvc_commit(myc,%d,\"%s\");\n",
			-s->nr, s->op1.ival, (name)?name:"");
	} break;
	case st_rollback: {
		char *name = s->op2.sval;
		len += snprintf(buf + len, BUFSIZ-len,
			"s%d := mvc_rollback(myc,%d,\"%s\");\n",
			-s->nr, s->op1.ival, (name)?name:"");
	} break;
	case st_schema: {
		schema *sc = s->op1.schema;
		  len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_bind_schema(myc, \"%s\");\n", 
			-s->nr, sc->name );
	} break;
	case st_table: {
		int sc = stmt_dump( s->op1.stval, nr, sql );
		table *t = s->op2.tval;
		  len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_bind_table(myc, s%d, \"%s\");\n", 
			-s->nr, sc, t->name );
	} break;
	case st_column: {
		int t = stmt_dump( s->op1.stval, nr, sql );
		column *c = s->op2.cval;
		  len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_bind_column(myc, s%d, \"%s\");\n", 
			-s->nr, t, c->name );
	} break;
	case st_key: {
		int t = stmt_dump( s->op1.stval, nr, sql );
		key *k = s->op2.kval;
		node *cc;

		len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_bind_key(myc, s%d", -s->nr, t);
		for (cc = k->columns->h; cc; cc = cc->next){
			column *c = cc->data;
		  	len += snprintf( buf+len, BUFSIZ-len, 
		  		", \"%s\"", c->name );
		}
		len += snprintf( buf+len, BUFSIZ-len, ");\n"); 
	} break;
	case st_create_schema: {
		schema *schema = s->op1.schema;
		len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_create_schema(myc, \"%s\", \"%s\");\n",
			-s->nr, schema->name, schema->auth );
	} break;
	case st_drop_schema: {
		len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_drop_schema(myc, \"%s\");\n", 
			-s->nr, s->op1.schema->name );
	} break;
	case st_create_table: {
		int sc = stmt_dump( s->op1.stval, nr, sql );
		table *t = s->op2.tval;
		if (t->sql){
		  	len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_create_view(myc, s%d, \"%s\", \"%s\");\n", 
			-s->nr, sc, t->name, t->sql );
		} else {
		  	len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_create_table(myc, s%d, \"%s\", %s);\n",
		   	-s->nr, sc, t->name, 
		   		(t->temp==0)?"false":"true" );
		}
	} break;
	case st_drop_table: {
		int sc = stmt_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_drop_table(myc, s%d, \"%s\", %s);\n", 
			-s->nr, sc, s->op2.sval, 
				(s->flag==0)?"false":"true" );
	} break;
	case st_create_column: {
		int t = stmt_dump( s->op1.stval, nr, sql );
		column *c = s->op2.cval;
		len += snprintf( buf+len, BUFSIZ-len, 
		"s%d := mvc_create_column(myc, s%d, \"%s\", \"%s\", %d);\n",
		-s->nr, t, c->name, c->tpe->sqlname, c->colnr );
	} break;
	case st_not_null: {
		int c = stmt_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		    "s%d := mvc_not_null(myc, s%d );\n", -s->nr, c );
	} break;
	case st_default: {
		int c = stmt_dump( s->op1.stval, nr, sql );
		int d = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		    "s%d := default_val(myc, s%d, s%d );\n", -s->nr, c, d );
	} break;
	/* todo: change to simple mvc_create_key(myc, key, list_of_string); */
	case st_create_key: {
		int t = stmt_dump( s->op1.stval, nr, sql );
		if (s->flag == fkey){
			int ft = stmt_dump( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ-len, 
		    	"s%d := mvc_create_key(myc, s%d, %d, s%d );\n", 
			-s->nr, t, s->flag, ft );
		} else {
			len += snprintf( buf+len, BUFSIZ-len, 
		    	"s%d := mvc_create_key(myc, s%d, %d, sql_key(nil));\n",
			-s->nr, t, s->flag );
		}
	} break;
	case st_add_col: {
		int k = stmt_dump( s->op1.stval, nr, sql );
		int c = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		    	"s%d := mvc_key_add_column(myc, s%d, s%d);\n", 
			-s->nr, k, c );
	} break;
	case st_select: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		if (s->op2.stval->nrcols >= 1){
			char *op = "=";
			switch(s->flag){
			case cmp_equal: op = "="; break;
			case cmp_notequal: op = "!="; break;
			case cmp_lt: op = "<"; break;
			case cmp_lte: op = "<="; break;
			case cmp_gt: op = ">"; break;
			case cmp_gte: op = ">="; break;
			default:
				len += snprintf( buf+len, BUFSIZ-len, 
					"error impossible\n");
	  		} 
			len += snprintf( buf+len, BUFSIZ-len, 
				"s%d := [%s](s%d,s%d).select(TRUE);\n", 
				-s->nr, op, l, r ); 
		} else {
		switch(s->flag){
		case cmp_equal:
			len += snprintf( buf+len, BUFSIZ-len, 
				"s%d := s%d.uselect(s%d);\n", -s->nr, l, r ); 
			break;
		case cmp_notequal:
			(void)(*nr)++; 
			len += snprintf( buf+len, BUFSIZ-len, 
				"s%d := s%d.uselect(s%d);\n", *nr, l, r ); 
			len += snprintf( buf+len, BUFSIZ-len, 
				"s%d := s%d.kdiff(s%d);\n", -s->nr, l, *nr );
			break;
		case cmp_lt:
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.mil_select(\"<in>\", %s(nil), s%d);\n", 
			  -s->nr, l, tail_type(s)->name, r ); 
			break;
		case cmp_lte:
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.uselect(%s(nil), s%d);\n", 
			  -s->nr, l, tail_type(s)->name, r ); 
			break;
		case cmp_gt:
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.mil_select(\"<in>\", s%d, %s(nil));\n", 
			  -s->nr, l, r, tail_type(s)->name ); 
			break;
		case cmp_gte: 
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.uselect(s%d, %s(nil));\n", 
			  -s->nr, l, r, tail_type(s)->name ); 
			break;
		default:
			len += snprintf( buf+len, BUFSIZ-len, 
					"error impossible\n");
	  	} 
		}
	} break;
	case st_select2: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r1 = stmt_dump( s->op2.stval, nr, sql );
		int r2 = stmt_dump( s->op3.stval, nr, sql );
		switch(s->flag){
		case cmp_equal: len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.select(s%d, s%d);\n", 
			  -s->nr, l, r1, r2 ); 
			break;
		case cmp_notequal: 
			(void)(*nr)++; 
			len += snprintf( buf+len, BUFSIZ-len,
			  "s%d := s%d.select(s%d, s%d);\n", *nr, l, r1, r2 ); 
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.kdiff(s%d);\n", -s->nr, l, *nr );
			break;
		}
	} break;
	case st_like: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.likeselect(s%d);\n", -s->nr, l, r ); 
	} break;
	case st_semijoin: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.semijoin(s%d);\n", -s->nr, l, r ); 
	} break;
	case st_diff: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.kdiff(s%d);\n", -s->nr, l, r ); 
	} break;
	case st_intersect: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.sintersect(s%d);\n", -s->nr, l, r ); 
	} break;
	case st_union: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.kunion(s%d);\n", -s->nr, l, r ); 
	} break;
	case st_outerjoin:
	case st_join: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		char *jt = "join";
		if (s->type == st_outerjoin)
			jt = "outerjoin";
		switch(s->flag){
		case cmp_equal:
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.%s(s%d);\n", -s->nr, l, jt, r ); 
			break;
		case cmp_notequal:
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.%s(s%d, \"!=\");\n", -s->nr, l, jt, r ); 
			break;
		case cmp_lt:
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.%s(s%d, \"<\");\n", -s->nr, l, jt, r ); 
			break;
		case cmp_lte: 
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.%s(s%d, \"<=\");\n", -s->nr, l, jt, r );
			break;
		case cmp_gt: 
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.%s(s%d, \">\" );\n", -s->nr, l, jt, r); 
			break;
		case cmp_gte: 
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.%s(s%d, \">=\" );\n", -s->nr, l, jt, r);
			break;
		case cmp_all: /* aka cross table */
			len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := s%d.project(0).join(s%d.reverse().project(0).reverse());\n", -s->nr, l, r ); 
			break;
		default:
			len += snprintf( buf+len, BUFSIZ-len, 
					"error impossible\n");
	  	} break;
	}
	case st_bat:
	case st_ubat: {
		char *type = (s->type==st_dbat)?"":"_ubat";
		if (table_isview(s->op1.cval->table)){
			s->nr = -stmt_dump( s->op1.cval->s, nr, sql );
		} else {
			len += snprintf( buf+len, BUFSIZ-len, 
			  	"s%d := mvc_bind%s(myc, \"%s\", \"%s\", %d)",
			  -s->nr, type, s->op1.cval->table->name, 
			  s->op1.cval->name, s->flag);

			if (s->flag > RDONLY){
				len += snprintf( buf+len, BUFSIZ-len, 
			  		".access(BAT_WRITE)"); 
			}
			len += snprintf( buf+len, BUFSIZ-len, "; #%s\n", 
			  s->h->tname?s->h->tname:s->h->t->name );
			if (sql->debug&4){
		  		len += snprintf( buf+len, BUFSIZ-len, 
				"s%d.info.print();", -s->nr);
			}
		}
	} break;
	case st_dbat:
	case st_obat: {
		char type = (s->type==st_dbat)?'d':'o';
		len += snprintf( buf+len, BUFSIZ-len, 
		  	"s%d := mvc_bind_%cbat(myc, \"%s\", %d)",
			  -s->nr, type, s->op1.tval->name, s->flag);

		if (s->flag > RDONLY){
			len += snprintf( buf+len, BUFSIZ-len, 
		  		".access(BAT_WRITE)"); 
		}
		len += snprintf( buf+len, BUFSIZ-len, ";\n" ); 
		if (sql->debug&4){
			len += snprintf( buf+len, BUFSIZ-len, 
			"s%d.info.print();\n", -s->nr);
		}
	} break;
	case st_reverse: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.reverse();\n", -s->nr, l);
	} 	break;
	case st_count: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.count();\n", -s->nr, l);
	} 	break;
	case st_const: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.project(s%d);\n", -s->nr, l, r);
	} 	break;
	case st_mark: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		if (s->op2.stval){
			int r = stmt_dump( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ-len, 
			 "s%d := s%d.reverse().mark(oid(s%d)).reverse();# %s\n",
			  -s->nr, l, r, 
			  	(!s->t)?"unknown":
			  	s->t->tname?s->t->tname:s->t->t->name);
		} else if (s->flag >= 0){
			len += snprintf( buf+len, BUFSIZ-len, 
			 "s%d := s%d.reverse().mark(oid(%d)).reverse();# %s\n", 
			  -s->nr, l, s->flag, 
			  	(!s->t)?"unknown":
			  	s->t->tname?s->t->tname:s->t->t->name);
		} else {
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.reverse().mark().reverse();\n", -s->nr, l);
		}
	} 	break;
	case st_group: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.group();\n", -s->nr, l);
	} 	break;
	case st_group_ext: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.tunique().mirror();\n", -s->nr, l);
	} 	break;
	case st_derive: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.group(s%d);\n", -s->nr, l, r);
	} 	break;
	case st_unique: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		if (s->op2.gval){
			int g = stmt_dump( s->op2.gval->grp, nr, sql );

		  	len += snprintf( buf+len, BUFSIZ-len, 
			"s%dg := s%d.group(s%d);\n", -s->nr, g, l);
		  	len += snprintf( buf+len, BUFSIZ-len, 
			"s%de := s%dg.tunique().mirror();\n", -s->nr, -s->nr);
		  	len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := s%d.semijoin(s%de);\n", -s->nr, l, -s->nr );
		} else {
		  	len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := s%d.reverse().kunique().reverse();\n", 
			-s->nr, l);
		}
	} 	break;
	case st_order: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := s%d.reverse().sort().reverse();\n", -s->nr, l );
	} 	break;
	case st_reorder: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := s%d.CTrefine(s%d);\n", -s->nr, l, r); 
		/* s->flag?"desc":"asc"); */
	} 	break;
	case st_unop: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		if (s->op1.stval->nrcols)
		  len += snprintf( buf+len, BUFSIZ-len, 
		   "s%d := [%s](s%d);\n", -s->nr, s->op2.funcval->imp, l );
		else 
		  len += snprintf( buf+len, BUFSIZ-len, 
		   "s%d := %s(s%d);\n", -s->nr, s->op2.funcval->imp, l);
	} 	break;
	case st_binop: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		if (s->op1.stval->nrcols || s->op2.stval->nrcols ){
		  	if (!s->op1.stval->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ-len, 
		    		"s%d := [s%d ~ s%d];\n", n, r, l ); 
				l = n;
		  	}
		  	if (!s->op2.stval->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ-len, 
		    		"s%d := [s%d ~ s%d];\n", n, l, r ); 
				r = n;
		  	}
		  	len += snprintf( buf+len, BUFSIZ-len, 
		    	"s%d := [%s](s%d,s%d);\n", 
			-s->nr, s->op3.funcval->imp, l, r );
		} else  {
		  	len += snprintf( buf+len, BUFSIZ-len, 
		    	"s%d := %s(s%d,s%d);\n", 
			-s->nr, s->op3.funcval->imp, l,r );
		}
	} 	break;
	case st_triop: {
		stmt *op1 = s->op1.lval->h->data;
		stmt *op2 = s->op1.lval->h->next->data;
		stmt *op3 = s->op1.lval->h->next->next->data;
		int r1 = stmt_dump( op1, nr, sql );
		int r2 = stmt_dump( op2, nr, sql );
		int r3 = stmt_dump( op3, nr, sql );
		if (op1->nrcols || op2->nrcols || op3->nrcols){
			int l = 0;
			if (op1->nrcols) l = r1;
			if (op2->nrcols) l = r2;
			if (op3->nrcols) l = r3;
		  	if (!op1->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ-len, 
		    		"s%d := [ s%d ~ s%d];\n", n, l, r1 ); 
				r1 = n;
		  	}
		  	if (!op2->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ-len, 
		    		"s%d := [ s%d ~ s%d];\n", n, l, r2 ); 
				r2 = n;
		  	}
		  	if (!op3->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ-len, 
		    		"s%d := [ s%d ~ s%d];\n", n, l, r3 ); 
				r3 = n;
		  	}
		  	len += snprintf( buf+len, BUFSIZ-len, 
		    	"s%d := [%s](s%d,s%d,s%d);\n", 
			-s->nr, s->op2.funcval->imp, r1, r2, r3 );
		} else {
		  len += snprintf( buf+len, BUFSIZ-len, 
		    "s%d := %s(s%d,s%d,s%d);\n", -s->nr, s->op2.funcval->imp, 
		    	r1, r2, r3 );
		}
	} 	break;
	case st_aggr: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		if (s->op3.gval){
			int g = stmt_dump( s->op3.gval->grp, nr, sql );
			int e = stmt_dump( s->op3.gval->ext, nr, sql );
			len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := {%s}(s%d, s%d, s%d);\n", 
				-s->nr, s->op2.aggrval->imp, l, g, e);
		} else {
			(*nr)++;
			len += snprintf( buf+len, BUFSIZ-len, 
				"s%d := s%d.%s();\n", 
				*nr, l, s->op2.aggrval->imp );
			len += snprintf( buf+len, BUFSIZ-len, 
				"s%d := new(oid,%s);\n"
				, -s->nr, s->op2.aggrval->res->name );
			len += snprintf( buf+len, BUFSIZ-len, 
				"s%d.insert(oid(0),s%d);\n"
				, -s->nr, *nr );
		}
	} 	break;
	case st_exists: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = 1;

		n = s->op2.lval->h;

		if (n){
		  	char *a = (char*)atom_type(n->data)->name;
			len += snprintf( buf+len, BUFSIZ-len, 
				"s%db := new(%s,oid);\n", -s->nr, a );
		}
		while(n){
			len += snprintf( buf+len, BUFSIZ-len, "s%dv := %s;\n", 
					-s->nr, atom_dump(n->data) );
			len += snprintf( buf+len, BUFSIZ-len, 
				"s%db.insert(s%dv, oid(%d));\n", 
					-s->nr, -s->nr, r++);
			n = n->next;
		}
		len += snprintf( buf+len, BUFSIZ-len, 
				"s%d := s%d.join(s%db);\n", -s->nr, l, -s->nr);
	} 	break;
	case st_atom: {
		len += snprintf( buf+len, BUFSIZ, 
				"s%d := %s;\n", -s->nr, atom_dump(s->op1.aval));
	} break;
	case st_insert_column: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := insert(s%d,s%d);\n", -s->nr, l, r);
	} break;
	case st_replace: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := replace(s%d,s%d);\n", -s->nr, l, r);
	} break;
	case st_delete: {
		if (s->op2.stval){
			int l = stmt_dump( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_delete(myc, \"%s\", s%d);\n", 
			-s->nr, s->op1.tval->name, l);
		} else {
			len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := mvc_delete(myc, \"%s\", nil);\n",
			  -s->nr, s->op1.tval->name );
		}
	} break;
	case st_name: 
		s->nr = - stmt_dump( s->op1.stval, nr, sql );
		break;
	case st_set: {
		for (n = s->op1.lval->h; n; n = n->next ){
			(void)stmt_dump( n->data, nr, sql );
		}
	} break;
	case st_sets: {
		for(n = s->op1.lval->h; n; n->next ){
			list *l = n->data;
			node *m = l->h;
			while(m){
				(void)stmt_dump( m->data, nr, sql );
				m = m->next;
			}
		}
	} break;
	case st_list: {
		for( n = s->op1.lval->h; n; n = n->next ){
			(void)stmt_dump( n->data, nr, sql );
		}
	} break;
	case st_copyfrom: {
		node *m = s->op2.lval->h;
		char *file = m->data;
		char *tsep = m->next->data;
		char *rsep = m->next->next->data;
		len += snprintf( buf+len, BUFSIZ-len, 
			"input(myc, \"%s\", \"%s\",\"%s\", \"%s\", %d);\n",
				s->op1.tval->name, file, tsep, rsep, s->flag);
	} break;
	case st_insert: {
		stmt *r = s->op2.lval->h->data;
		for( n = s->op2.lval->h ;n; n = n->next ){
			stmt_dump(n->data, nr, sql);
		}
		len += snprintf( buf+len, BUFSIZ-len, 
			"mvc_insert(myc, \"%s\"", s->op1.tval->name );
		for( n = s->op2.lval->h ;n; n = n->next ){
			r = n->data;
			len += snprintf( buf+len, BUFSIZ-len, ",s%d", r->nr);
		}
		len += snprintf( buf+len, BUFSIZ-len, ");\n" );
	} break;
	case st_ordered: {
		int l =  stmt_dump( s->op1.stval, nr, sql );
		(void)stmt_dump( s->op2.stval, nr, sql );
		s->nr = -l;
	} break;
	case st_output: {
		stmt *order = NULL;
		stmt *lst = s->op1.stval;
		stmt_dump( lst, nr, sql );
		if (sql->debug&32){
			len += snprintf( buf+len, BUFSIZ-len,
			"stream_write(Output,\"0\\n\");stream_flush(Output);\n");
			if (lst->type == st_list){
				list *l = lst->op1.lval;

				n = l->h;
				len += snprintf( buf+len, BUFSIZ-len, 
						"table(\n");
				if (n){
					stmt *r = n->data;
					len += snprintf( buf+len, BUFSIZ-len,
						"s%d", r->nr);
					n = n->next;
				}
				while(n){
					stmt *r = n->data;
					len += snprintf( buf+len, BUFSIZ-len,
						", s%d", r->nr);
					n = n->next;
				}
				len += snprintf( buf+len, BUFSIZ-len, ");\n");
			}
			break;
		}
		if (lst->type == st_ordered){
			order = lst->op1.stval; 
			lst = lst->op2.stval; 
		}
		if (lst->type == st_list){
			list *l = lst->op1.lval;

			n = l->h;
			if (n){
			  if (!order){
			    order = n->data;
			  }
			}
			len += snprintf( buf+len, BUFSIZ-len,
				"server_output(Output, s%d ", order->nr);
			while(n){
				stmt *r = n->data;
				len += snprintf( buf+len, BUFSIZ-len,
					", s%d", r->nr);
				n = n->next;
			}
			len += snprintf( buf+len, BUFSIZ-len,");\n");
		} else {
			fprintf(stderr, "not a valid output list %d %d %d\n",
					lst->type, st_list, st_ordered);
		}
	} break;
	case st_result: {
		stmt *k = s->op1.stval;
		int l =  stmt_dump( k, nr, sql );
		if (k->type != st_insert){
			len += snprintf( buf+len, BUFSIZ-len,
			"result(Output,mvc_type(myc),mvc_status(myc));\n",l);
		} else {
			stmt *r = k->op2.lval->h->data;
			if (r->nrcols > 0)  /* insert columns */
			  len += snprintf( buf+len, BUFSIZ-len,
			  "result(Output,mvc_type(myc),mvc_status(myc));\n",l);
		} 
	} break;

	}

    	if (s->nr > 0) 
		return s->nr;
	else
		s->nr = -s->nr;

	if (sql->debug&2 && s->type != st_insert)
		len += snprintf( buf+len, BUFSIZ-len,
		"t1 := time(); printf(\"%d %%d\\n\", t1 - t0);\n", s->nr);

	buf[len] = '\0';
	sql->out->write( sql->out, buf, 1, len );

	/*
	for (n = s->uses->h; n; n = n->next)
		stmt_dump(n->data, nr, sql );
		*/

	if (sql->debug&8)
		fwrite( buf, 1, len, stderr);

    	return s->nr;
    }
    return 0;
}


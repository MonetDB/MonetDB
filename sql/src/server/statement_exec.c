/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
 *
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 *
 * The Original Code is the Monet Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.
 * All Rights Reserved.
 *
 * Contributor(s):
 * 		Martin Kersten  <Martin.Kersten@cwi.nl>
 * 		Peter Boncz  <Peter.Boncz@cwi.nl>
 * 		Niels Nes  <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

#include "mem.h"
#include "statement.h"
#include <string.h>


static void atom_dump( atom *a, context *sql){
	stream *s = sql->out;
	int i = 0;
	char buf[BUFSIZ];

	switch (a->type){
	case int_value: 
			i=snprintf(buf, BUFSIZ, "%s(%d)", 
					a->tpe->type->name, a->data.ival); 
			break;
	case string_value: 
			i=snprintf(buf, BUFSIZ-2, "%s(\"%s\")", 
					a->tpe->type->name, a->data.sval); 
 			break;
	case float_value: 
			/* float/double requires string as 
			 * else dbl(0.1), will first be a float and
			 * then converted to dbl, which results in the
			 * wrong value */ 
			i=snprintf(buf, BUFSIZ, "%s(\"%f\")", 
					a->tpe->type->name, a->data.dval); 
			break;
	case general_value:
			if (a->data.sval)
			  i=snprintf(buf, BUFSIZ, "%s(\"%s\")", 
				a->tpe->type->name, a->data.sval );
			else 
			  i=snprintf(buf, BUFSIZ, "%s(nil)", 
				a->tpe->type->name );
			break;
	default:
			break;
	}
	s->write(s, buf, 1, i);

	if (sql->debug&8)
		fwrite( buf, 1, i, stderr);
}

static void write_head( context *sql, int nr )
{
	if (sql->debug&2){
		char *t0 = "t0 := time();\n";
		int l = strlen(t0);
		sql->out->write( sql->out, t0, 1, l );
	}
}

static void write_tail( context *sql, int nr)
{
	if (sql->debug&2){
		char dbg[BUFSIZ];
		int l = snprintf( dbg, BUFSIZ,
		"t1 := time(); printf(\"%d %%d\\n\", t1 - t0);\n", nr);
		sql->out->write( sql->out, dbg, 1, l );
	}
}

static void write_part( context *sql, char *buf, int len )
{
	buf[len] = '\0';
	sql->out->write( sql->out, buf, 1, len );

	if (sql->debug&8)
		fwrite( buf, 1, len, stderr);
}

static void write_command( context *sql, char *buf )
{
	if (sql->debug&(1024+2048)) {
		int l;
		char v[BUFSIZ];
		char *s = strdup(buf), *c = s;
		while (c = strchr(c,'"'))	*c = '`';
		c = s;
		while (c = strchr(c,'\n'))	*c = '\t';
		l = snprintf( v, BUFSIZ, "printf(\"< %s >\\n\");\n", s );
		sql->out->write( sql->out, v, 1, l );
		free(s);
	}
}

static void write_result( context *sql, char *buf )
{
	if (sql->debug&(1024+2048)) {
		int l;
		char v[BUFSIZ], *a = buf, *b, *y, z;
		while (a && (y = strstr(a," := "))) {
			z = *y;
			*y = '\0';
			if (  b = strrchr(a,' ' ) )	a = b;
			if (!(b = strrchr(a,'\n')))	b = a;
			l = snprintf( v, BUFSIZ, 
				"if (type(%s) != 4) {"
				"	print(%s);"
				"} else {"
				"	var x := count(%s);"
				"	printf(\"| %%d * { %%s , %%s } |\\n\",x,head(%s),tail(%s));",
				b, b, b, b, b );
			if (sql->debug&2048) {
			    l += snprintf( v+l, BUFSIZ-l,
				"	if (x < 40) {"
				"		print(%s);"
				"	} else {"
				"		print(slice(%s,0,9));"
				"		print(\"...\");"
				"		print(sample(slice(%s,10,x - 11),10));"
				"		print(\"...\");"
				"		print(slice(%s,x - 10,x - 1));"
				"	}",
				b, b, b, b );
			}
			l += snprintf( v+l, BUFSIZ-l,
				"}\n") ;
			sql->out->write( sql->out, v, 1, l );
			*y = z;
			a = strchr(y,';');
		}
	}
}

static void dump( context *sql, char *buf, int len, int nr )
{
	write_head(sql,nr);
	write_command(sql,buf);
	write_part(sql,buf,len);
	write_result(sql,buf);
	write_tail(sql,nr);
}

void dump_header( context *sql, list *l ){
	char buf[BUFSIZ+1];
	int len = 0;
	node *n;

	len += snprintf(buf+len, BUFSIZ-len, "output_header(Output,%d);\n",
			list_length(l));
	for (n=l->h; n; n = n->next){
		stmt *s = n->data;
		char *name = column_name(s);
		sql_subtype *type = tail_type(s);

		len += snprintf(buf+len, BUFSIZ-len,
			"output_column(Output,\"%s\", \"%s\");\n",
			(name)?name:"",(type)?type->type->sqlname:"");
	}
	len += snprintf(buf+len, BUFSIZ-len, "stream_flush(Output);\n");
	write_command(sql,buf);
	write_part(sql,buf,len);
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

	switch(s->type){
	case st_none: break;
	case st_release: 
		len = snprintf( buf, BUFSIZ, 
			"s%d := mvc_release(myc,\"%s\");\n",
			-s->nr, s->op1.sval);
		dump(sql,buf,len,-s->nr);
		break;
	case st_commit: {
		char *name = s->op2.sval;
		len = snprintf(buf, BUFSIZ,
			"s%d := mvc_commit(myc,%d,\"%s\");\n",
			-s->nr, s->op1.ival, (name)?name:"");
		dump(sql,buf,len,-s->nr);
	} break;
	case st_rollback: {
		char *name = s->op2.sval;
		len = snprintf(buf, BUFSIZ,
			"s%d := mvc_rollback(myc,%d,\"%s\");\n",
			-s->nr, s->op1.ival, (name)?name:"");
		dump(sql,buf,len,-s->nr);
	} break;
	case st_schema: {
		schema *sc = s->op1.schema;
		len = snprintf( buf, BUFSIZ, 
			"s%d := mvc_bind_schema(myc, \"%s\");\n", 
			-s->nr, sc->name );
		dump(sql,buf,len,-s->nr);
	} break;
	case st_table: {
		int sc = stmt_dump( s->op1.stval, nr, sql );
		table *t = s->op2.tval;
		len = snprintf( buf, BUFSIZ, 
			"s%d := mvc_bind_table(myc, s%d, \"%s\");\n", 
			-s->nr, sc, t->name );
		dump(sql,buf,len,-s->nr);
	} break;
	case st_column: {
		int t = stmt_dump( s->op1.stval, nr, sql );
		column *c = s->op2.cval;
		len = snprintf( buf, BUFSIZ, 
			"s%d := mvc_bind_column(myc, s%d, \"%s\");\n", 
			-s->nr, t, c->name );
		dump(sql,buf,len,-s->nr);
	} break;
	case st_key: {
		int t = stmt_dump( s->op1.stval, nr, sql );
		key *k = s->op2.kval;
		len = snprintf( buf, BUFSIZ, 
			"s%d := mvc_bind_key(myc, s%d, \"%s\");\n", 
			-s->nr, t, k->name);
		dump(sql,buf,len,-s->nr);
	} break;
	case st_create_schema: {
		schema *schema = s->op1.schema;
		len = snprintf( buf, BUFSIZ, 
			"s%d := mvc_create_schema(myc, \"%s\", \"%s\");\n",
			-s->nr, schema->name, schema->auth );
		dump(sql,buf,len,-s->nr);
	} break;
	case st_drop_schema: {
		len = snprintf( buf, BUFSIZ, 
			"s%d := mvc_drop_schema(myc, \"%s\");\n", 
			-s->nr, s->op1.schema->name );
		dump(sql,buf,len,-s->nr);
	} break;
	case st_create_table: {
		int sc = stmt_dump( s->op1.stval, nr, sql );
		table *t = s->op2.tval;
		if (t->sql){
		  	len = snprintf( buf, BUFSIZ, 
			"s%d := mvc_create_view(myc, s%d, \"%s\", \"%s\");\n", 
			-s->nr, sc, t->name, t->sql );
		} else {
		  	len = snprintf( buf, BUFSIZ, 
			"s%d := mvc_create_table(myc, s%d, \"%s\", %d);\n",
		   	-s->nr, sc, t->name, t->type );
		}
		dump(sql,buf,len,-s->nr);
	} break;
	case st_drop_table: {
		int sc = stmt_dump( s->op1.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
			"s%d := mvc_drop_table(myc, s%d, \"%s\", %s);\n", 
			-s->nr, sc, s->op2.sval, 
				(s->flag==0)?"false":"true" );
		dump(sql,buf,len,-s->nr);
	} break;
	case st_create_column: {
		int t = stmt_dump( s->op1.stval, nr, sql );
		column *c = s->op2.cval;
		len = snprintf( buf, BUFSIZ, 
		"s%d := mvc_create_column(myc, s%d, \"%s\", \"%s\", %d, %d, %d);\n",
			-s->nr, t, c->name, c->tpe->type->sqlname, 
			c->tpe->digits, c->tpe->scale, c->colnr );
		dump(sql,buf,len,-s->nr);
	} break;
	case st_null: {
		int c = stmt_dump( s->op1.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
		    "s%d := mvc_null(myc, s%d, %d );\n", -s->nr, c, s->flag );
		dump(sql,buf,len,-s->nr);
	} break;
	case st_default: {
		int c = stmt_dump( s->op1.stval, nr, sql );
		int d = stmt_dump( s->op2.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
		    "s%d := mvc_default(myc, s%d, str(s%d) );\n", -s->nr, c, d);
		dump(sql,buf,len,-s->nr);
	} break;
	case st_create_key: {
		node *n;
		key *k = s->op1.kval;
		
		write_head(sql,-s->nr);
		len = snprintf( buf, BUFSIZ, "b%d := new(str,int);\n", -s->nr);
		write_command(sql,buf);
		write_part(sql,buf,len);
		for(n=k->columns->h;n; n = n->next){
			kc *kc = n->data;
			len = snprintf( buf, BUFSIZ, 
		    	"b%d.insert(\"%s\", %d);\n", -s->nr, kc->c->name, kc->trunc);
			write_part(sql,buf,len);
		}
		len = snprintf( buf, BUFSIZ, "b%d := ", -s->nr);
		write_result(sql,buf);
		if (s->flag == fkey){
			int ft = stmt_dump( s->op2.stval, nr, sql );
			len = snprintf( buf, BUFSIZ, 
		    	"s%d := mvc_create_key(myc, \"%s\", \"%s\", \"%s\", %d, b%d, s%d );\n",
			-s->nr, k->t->schema->name, k->t->name, (k->name)?k->name:"", k->type, -s->nr, ft );
		} else {
			len = snprintf( buf, BUFSIZ, 
		    	"s%d := mvc_create_key(myc, \"%s\", \"%s\", \"%s\", %d, b%d, sql_key(nil));\n",
			-s->nr, k->t->schema->name, k->t->name, (k->name)?k->name:"", k->type, -s->nr );
		}
		write_command(sql,buf);
		write_part(sql,buf,len);
		write_result(sql,buf);
		write_tail(sql,-s->nr);
	} break;
	case st_create_role: {
		len = snprintf( buf, BUFSIZ, 
		    	"s%d := mvc_create_role(myc, \"%s\", %d);\n", 
			-s->nr, s->op1.sval, s->flag );
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_drop_role: {
		len = snprintf( buf, BUFSIZ, 
		    	"s%d := mvc_drop_role(myc, \"%s\");\n", 
			-s->nr, s->op1.sval );
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_grant_role: {
		len = snprintf( buf, BUFSIZ, 
		    	"s%d := mvc_grant_role(myc, \"%s\", \"%s\");\n", 
			-s->nr, s->op1.sval, s->op2.sval );
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_revoke_role: {
		len = snprintf( buf, BUFSIZ, 
		    	"s%d := mvc_revoke_role(myc, \"%s\", \"%s\");\n", 
			-s->nr, s->op1.sval, s->op2.sval );
		dump(sql,buf,len,-s->nr);
	} 	break;
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
				len = snprintf( buf, BUFSIZ, 
					"error impossible\n");
	  		} 
			len = snprintf( buf, BUFSIZ, 
				"s%d := [%s](s%d,s%d).select(TRUE);\n", 
				-s->nr, op, l, r ); 
		} else {
		switch(s->flag){
		case cmp_equal:
			len = snprintf( buf, BUFSIZ, 
				"s%d := s%d.uselect(s%d);\n", -s->nr, l, r ); 
			break;
		case cmp_notequal:
			(void)(*nr)++; 
			len = snprintf( buf, BUFSIZ, 
				"s%d := s%d.uselect(s%d);\n", *nr, l, r ); 
			len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.kdiff(s%d);\n", -s->nr, l, *nr );
			break;
		case cmp_lt:
			len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.mil_select(\"<in>\", %s(nil), s%d);\n", 
			  -s->nr, l, tail_type(s)->type->name, r ); 
			break;
		case cmp_lte:
			len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.uselect(%s(nil), s%d);\n", 
			  -s->nr, l, tail_type(s)->type->name, r ); 
			break;
		case cmp_gt:
			len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.mil_select(\"<in>\", s%d, %s(nil));\n", 
			  -s->nr, l, r, tail_type(s)->type->name ); 
			break;
		case cmp_gte: 
			len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.uselect(s%d, %s(nil));\n", 
			  -s->nr, l, r, tail_type(s)->type->name ); 
			break;
		default:
			len = snprintf( buf, BUFSIZ, 
					"error impossible\n");
	  	} 
		}
		dump(sql,buf,len,-s->nr);
	} break;
	case st_select2: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r1 = stmt_dump( s->op2.stval, nr, sql );
		int r2 = stmt_dump( s->op3.stval, nr, sql );
		switch(s->flag){
		case cmp_equal: len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.select(s%d, s%d);\n", 
			  -s->nr, l, r1, r2 ); 
			break;
		case cmp_notequal: 
			(void)(*nr)++; 
			len = snprintf( buf, BUFSIZ,
			  "s%d := s%d.select(s%d, s%d);\n", *nr, l, r1, r2 ); 
			len += snprintf( buf+len, BUFSIZ-len, 
			  "s%d := s%d.kdiff(s%d);\n", -s->nr, l, *nr );
			break;
		}
		dump(sql,buf,len,-s->nr);
	} break;
	case st_like: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
		  "s%d := s%d.likeselect(s%d);\n", -s->nr, l, r ); 
		dump(sql,buf,len,-s->nr);
	} break;
	case st_semijoin: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
		  "s%d := s%d.semijoin(s%d);\n", -s->nr, l, r ); 
		dump(sql,buf,len,-s->nr);
	} break;
	case st_diff: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
		  "s%d := s%d.kdiff(s%d);\n", -s->nr, l, r ); 
		dump(sql,buf,len,-s->nr);
	} break;
	case st_intersect: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
		  "s%d := s%d.sintersect(s%d);\n", -s->nr, l, r ); 
		dump(sql,buf,len,-s->nr);
	} break;
	case st_union: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
		  "s%d := s%d.kunion(s%d);\n", -s->nr, l, r ); 
		dump(sql,buf,len,-s->nr);
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
			len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.%s(s%d);\n", -s->nr, l, jt, r ); 
			break;
		case cmp_notequal:
			len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.%s(s%d, \"!=\");\n", -s->nr, l, jt, r ); 
			break;
		case cmp_lt:
			len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.%s(s%d, \"<\");\n", -s->nr, l, jt, r ); 
			break;
		case cmp_lte: 
			len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.%s(s%d, \"<=\");\n", -s->nr, l, jt, r );
			break;
		case cmp_gt: 
			len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.%s(s%d, \">\" );\n", -s->nr, l, jt, r); 
			break;
		case cmp_gte: 
			len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.%s(s%d, \">=\" );\n", -s->nr, l, jt, r);
			break;
		case cmp_all: /* aka cross table */
			len = snprintf( buf, BUFSIZ, 
			"s%d := s%d.project(0).join(s%d.reverse().project(0).reverse());\n", -s->nr, l, r ); 
			break;
		default:
			len = snprintf( buf, BUFSIZ, 
					"error impossible\n");
	  	} 
		if (sql->debug&4){
			len += snprintf(buf +len, BUFSIZ-len, 
				"s%d.info.print;\n", -s->nr);
		}
		dump(sql,buf,len,-s->nr);
	} break;
	case st_ibat: 
		s->nr = -stmt_dump( s->op1.stval, nr, sql );
		break;
	case st_bat:
	case st_ubat: {
		char *type = (s->type==st_bat)?"":"_ubat";
		if (s->op1.cval->table->type == tt_view){
			s->nr = -stmt_dump( s->op1.cval->s, nr, sql );
		} else {
			char *hname = NULL;
			if (s->h->type == st_basetable){
				hname = s->h->op1.tval->name;
			}
			len = snprintf( buf, BUFSIZ, 
			   "s%d := mvc_bind%s(myc, \"%s\", \"%s\", \"%s\", %d)",
			  -s->nr, type, 
			  s->op1.cval->table->schema->name, 
			  s->op1.cval->table->name, 
			  s->op1.cval->name, s->flag);

			if (s->flag > RDONLY){
				len += snprintf( buf+len, BUFSIZ-len, 
			  		".access(BAT_WRITE)"); 
			}
			len += snprintf( buf+len, BUFSIZ-len, "; #%s\n", hname);
			if (sql->debug&4){
		  		len += snprintf( buf+len, BUFSIZ-len, 
				"s%d.info.print();", -s->nr);
			}
		}
		dump(sql,buf,len,-s->nr);
	} break;
	case st_dbat:
	case st_obat: {
		char type = (s->type==st_dbat)?'d':'o';
		len = snprintf( buf, BUFSIZ, 
		  	"s%d := mvc_bind_%cbat(myc, \"%s\", \"%s\", %d)",
			  -s->nr, type, 
			  s->op1.tval->schema->name, 
			  s->op1.tval->name, 
			  s->flag);

		if (s->flag > RDONLY){
			len += snprintf( buf+len, BUFSIZ-len, 
		  		".access(BAT_WRITE)"); 
		}
		len += snprintf( buf+len, BUFSIZ-len, ";\n" ); 
		if (sql->debug&4){
			len += snprintf( buf+len, BUFSIZ-len, 
			"s%d.info.print();\n", -s->nr);
		}
		dump(sql,buf,len,-s->nr);
	} break;
	case st_kbat: {
		len = snprintf( buf, BUFSIZ, 
		  	"s%d := mvc_bind_kbat(myc, \"%s\", \"%s\", \"%s\", %d)",
			  -s->nr, 
			  s->op1.kval->t->schema->name, 
			  s->op1.kval->t->name, 
			  s->op1.kval->name,
			  s->flag);

		if (s->flag > RDONLY){
			len += snprintf( buf+len, BUFSIZ-len, 
		  		".access(BAT_WRITE)"); 
		}
		len += snprintf( buf+len, BUFSIZ-len, ";\n" ); 
		if (sql->debug&4){
			len += snprintf( buf+len, BUFSIZ-len, 
			"s%d.info.print();\n", -s->nr);
		}
		dump(sql,buf,len,-s->nr);
	} break;
	case st_reverse: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
		  "s%d := s%d.reverse();\n", -s->nr, l);
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_count: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
		  "s%d := s%d.count();\n", -s->nr, l);
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_const: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.project(s%d);\n", -s->nr, l, r);
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_mark: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		char *tname = NULL;

		if (s->t && s->t->type == st_basetable){
			tname = s->t->op1.tval->name;
		}
		if (s->op2.stval){
			int r = stmt_dump( s->op2.stval, nr, sql );
			len = snprintf( buf, BUFSIZ, 
			 "s%d := s%d.reverse().mark(oid(s%d)).reverse();# %s\n",
			  -s->nr, l, r, 
			  	(!tname)?"unknown":tname);
		} else if (s->flag >= 0){
			len = snprintf( buf, BUFSIZ, 
			 "s%d := s%d.reverse().mark(oid(%d)).reverse();# %s\n", 
			  -s->nr, l, s->flag, 
			  	(!tname)?"unknown":tname);
		} else {
			len = snprintf( buf, BUFSIZ, 
			  "s%d := s%d.reverse().mark().reverse();\n", -s->nr, l);
		}
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_group: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.CTgroup();\n", -s->nr, l);
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_group_ext: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.tunique().mirror();\n", -s->nr, l);
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_derive: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := s%d.CTgroup(s%d);\n", -s->nr, l, r);
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_unique: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		if (s->op2.stval){
			int g = stmt_dump( s->op2.stval, nr, sql );

		  	len += snprintf( buf+len, BUFSIZ-len, 
			"s%dg := s%d.CTgroup(s%d);\n", -s->nr, g, l);
		  	len += snprintf( buf+len, BUFSIZ-len, 
			"s%de := s%dg.tunique().mirror();\n", -s->nr, -s->nr);
		  	len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := s%d.semijoin(s%de);\n", -s->nr, l, -s->nr );
		} else {
		  	len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := s%d.reverse().kunique().reverse();\n", 
			-s->nr, l);
		}
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_limit: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
			"s%d := s%d.slice(0, %d - 1);\n", -s->nr, l, s->flag );
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_order: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
			"s%d := s%d.reverse().sort()", -s->nr, l );
		if (!s->flag)
			len += snprintf( buf+len, BUFSIZ-len,
				".access(BAT_WRITE).revert().access(BAT_READ)" );
		len += snprintf( buf+len, BUFSIZ-len, ".reverse();\n" );
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_reorder: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len = snprintf( buf, BUFSIZ, 
			"s%d := s%d.CTrefine(s%d)", -s->nr, l, r); 
/*
		if (!s->flag)
			len += snprintf( buf+len, BUFSIZ-len,
				".access(BAT_WRITE).revert.access(BAT_READ)" );
*/
		len += snprintf( buf+len, BUFSIZ-len, ";\n" );
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_op: {
		len = snprintf( buf, BUFSIZ, 
		   "s%d := %s();\n", -s->nr, s->op4.funcval->imp);
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_unop: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		if (s->op1.stval->nrcols)
		  len = snprintf( buf, BUFSIZ, 
		   "s%d := [%s](s%d);\n", -s->nr, s->op4.funcval->imp, l );
		else 
		  len = snprintf( buf, BUFSIZ, 
		   "s%d := %s(s%d);\n", -s->nr, s->op4.funcval->imp, l);
		dump(sql,buf,len,-s->nr);
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
			-s->nr, s->op4.funcval->imp, l, r );
		} else  {
		  	len += snprintf( buf+len, BUFSIZ-len, 
		    	"s%d := %s(s%d,s%d);\n", 
			-s->nr, s->op4.funcval->imp, l,r );
		}
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_triop: {
		int r1 = stmt_dump( s->op1.stval, nr, sql );
		int r2 = stmt_dump( s->op2.stval, nr, sql );
		int r3 = stmt_dump( s->op3.stval, nr, sql );
		if (s->op1.stval->nrcols || 
	 	    s->op2.stval->nrcols || 
		    s->op3.stval->nrcols){
			int l = 0;
			if (s->op1.stval->nrcols) l = r1;
			if (s->op2.stval->nrcols) l = r2;
			if (s->op3.stval->nrcols) l = r3;
		  	if (!s->op1.stval->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ-len, 
		    		"s%d := [ s%d ~ s%d];\n", n, l, r1 ); 
				r1 = n;
		  	}
		  	if (!s->op2.stval->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ-len, 
		    		"s%d := [ s%d ~ s%d];\n", n, l, r2 ); 
				r2 = n;
		  	}
		  	if (!s->op3.stval->nrcols){
				int n = (*nr)++; 
		  		len += snprintf( buf+len, BUFSIZ-len, 
		    		"s%d := [ s%d ~ s%d];\n", n, l, r3 ); 
				r3 = n;
		  	}
		  	len += snprintf( buf+len, BUFSIZ-len, 
		    	"s%d := [%s](s%d,s%d,s%d);\n", 
			-s->nr, s->op4.funcval->imp, r1, r2, r3 );
		} else {
		  len += snprintf( buf+len, BUFSIZ-len, 
		    "s%d := %s(s%d,s%d,s%d);\n", -s->nr, s->op4.funcval->imp, 
		    	r1, r2, r3 );
		}
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_aggr: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		if (s->op3.gval){
			int g = stmt_dump( s->op2.stval, nr, sql );
			int e = stmt_dump( s->op3.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ-len, 
			"s%d := {%s}(s%d, s%d, s%d);\n", 
				-s->nr, s->op4.aggrval->imp, l, g, e);
		} else {
			len += snprintf( buf+len, BUFSIZ-len, 
				"s%d := s%d.%s();\n", 
				-s->nr, l, s->op4.aggrval->imp );
		}
		dump(sql,buf,len,-s->nr);
	} 	break;
	case st_temp: 
		len = snprintf( buf, BUFSIZ, "s%d := new(oid,%s);\n", 
			-s->nr, basecolumn(s->op1.stval)->tpe->type->name );
		dump(sql,buf,len,-s->nr);
		break;
	case st_atom: {
		write_head(sql,-s->nr);
		len = snprintf( buf, BUFSIZ, "s%d := ", -s->nr);
		write_command(sql,buf);
		write_part(sql,buf,len);
		atom_dump(s->op1.aval, sql);
		len = snprintf( buf, BUFSIZ, ";\n");
		write_part(sql,buf,len);
		len = snprintf( buf, BUFSIZ, "s%d := ", -s->nr);
		write_result(sql,buf);
		write_tail(sql,-s->nr);
	} break;
	case st_insert: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		if (s->op2.stval->nrcols){
			len = snprintf( buf, BUFSIZ, 
		  	"s%d := insert(s%d.access(BAT_WRITE),s%d);\n", -s->nr, l, r);
		} else {
			len = snprintf( buf, BUFSIZ, 
		  	"s%d := insert(s%d,s%d);\n", -s->nr, l, r);
		}
		dump(sql,buf,len,-s->nr);
	} break;
	case st_append: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		if (s->op2.stval->nrcols){
			len = snprintf( buf, BUFSIZ, 
		  	"s%d := append(s%d.access(BAT_WRITE),s%d);\n", -s->nr, l, r);
		} else {
			len = snprintf( buf, BUFSIZ, 
		  	"s%d := append(s%d,s%d);\n", -s->nr, l, r);
		}
		dump(sql,buf,len,-s->nr);
	} break;
	case st_replace: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "s%d := replace(s%d.access(BAT_WRITE),s%d);\n", -s->nr, l, r);
		dump(sql,buf,len,-s->nr);
	} break;
	case st_exception: {
		int l = stmt_dump( s->op1.stval, nr, sql );
		int r = stmt_dump( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ-len, 
		  "if (bit(s%d)){ ERROR(s%d); }\n", l, r);
		dump(sql,buf,len,-s->nr);
	}
	case st_alias: 
	case st_column_alias: 
		s->nr = - stmt_dump( s->op1.stval, nr, sql );
		break;
	case st_set: {
		for (n = s->op1.lval->h; n; n = n->next ){
			(void)stmt_dump( n->data, nr, sql );
		}
	} break;
	case st_sets: {
		for(n = s->op1.lval->h; n; n = n->next ){
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
		char *tsep = m->data;
		char *rsep = m->next->data;
		
		len += snprintf( buf+len, BUFSIZ-len, 
			"Bs%d := new(void,str);\n", -s->nr);
		if (s->op3.lval){
			node *n;
			for(n = s->op3.lval->h; n; n = n->next){
				char *file = n->data;
				len += snprintf( buf+len, BUFSIZ-len, 
				  	"Bs%d.insert(oid(nil),\"%s\");\n" , 
				  	-s->nr, file );
			}
		}
		len += snprintf( buf+len, BUFSIZ-len, 
		    "input(myc, Input, \"%s\", Bs%d,\"%s\", \"%s\", %d);\n",
			s->op1.tval->name, -s->nr, tsep, rsep, s->flag);
		dump(sql,buf,len,-s->nr);
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

		write_head(sql,-s->nr);
		if (sql->debug&1){
			if (lst->type == st_list){
				list *l = lst->op1.lval;

				n = l->h;
				while(n){
					stmt *r = n->data;
					len += snprintf( buf+len, BUFSIZ-len,
						"print(s%d);\n", r->nr);
					n = n->next;
				}
			}
		}
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
		if (len) {
			write_command(sql,buf);
			write_part(sql,buf,len);
		}
		len = 0;
		if (lst->type == st_ordered){
			order = lst->op1.stval; 
			lst = lst->op2.stval; 
		}
		if (lst->type == st_list){
			list *l = lst->op1.lval;

			dump_header(sql,l);
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
		write_command(sql,buf);
		write_part(sql,buf,len);
		write_tail(sql,-s->nr);
	} break;

	/* todo */
	case st_basetable: 
	case st_grant:
	case st_revoke:
	case st_ptable:
	case st_pivot:
		printf("not implemented stmt\n");
		assert(0);
	}

    	if (s->nr > 0) 
		assert(s->nr <= 0);
	else
		s->nr = -s->nr;

    	return s->nr;
    }
    return 0;
}


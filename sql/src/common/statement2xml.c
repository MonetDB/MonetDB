
#include "mem.h"
#include "statement.h"
#include <stdarg.h>

extern int statement2xml( statement *s, int *nr, context *sql );

static
char *atom2xml( atom *a){
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
char *atom2xml_fast( atom *a){
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


/*********************************
 * XML output, a bit hard-coded:)
 */
int xml_level=0; /* current depth of the tree */
void xml_enter() /* increase depth */
{
	xml_level+=1;
}
void xml_leave() /* return - decrease depth */
{
	xml_level-=1;
}
void xml_margin() /* print margin for the xml-tree */
{
	int i;
	for (i=0;i<xml_level;i++)
		fprintf(stderr,"| ");
}
void xml_output(char *format, ...) /* output some string with margin */
{
	va_list ap;
	xml_margin();
	va_start(ap,format);
	vfprintf(stderr, format, ap);
	va_end(ap);
}
char* xml_node(char *format, ...) /* print node, enter it */
{
	va_list ap;
	char *node;

	node=malloc(BUFSIZ+1);
	va_start(ap,format);
	vsnprintf(node, BUFSIZ, format, ap);
	va_end(ap);
	xml_margin();
	fprintf(stderr, "<%s>\n", node);
	xml_enter();
	return node;
}
void xml_freenode(char *node) /* close node */
{
	xml_leave();
	xml_margin();
	fprintf(stderr, "</%s>\n", node);
	if(node)
		free(node);
}
void xml_field(char *fldname, char *format, ...) /* print field in 1 line */
{
	va_list ap;
	char buf[BUFSIZ+1];
	char *i;

	xml_margin();
	fprintf(stderr, "<%s>", fldname);
	va_start(ap,format);
	vsnprintf(buf, BUFSIZ, format, ap);
	va_end(ap);
	for(i=buf;*i;i++)
		if (*i=='\n') 
			*i=' ';
	fprintf(stderr, " %s </%s>\n", buf, fldname);
}
/* XML end 
 **********/

int statement2xml( statement *s, int *nr, context *sql ){
    char buf[BUFSIZ+1];
    int len = 0;
    node *n;
/* some XML stuff */
	char *xn;
#define MILOUT xml_field("_MIL_", "%s", buf);
#define XNODE(s) xn=xml_node(s)
#define FXNODE { if (len) MILOUT; xml_freenode(xn); }

    assert (*nr);

    buf[0] = '\0';
    if (s){
    	if (s->nr != 0) return s->nr;

	s->nr = (*nr)++;

	switch(s->type){
	case st_none: break;
	case st_begin: 
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_trans_begin(myc);\n", s->nr );  
		XNODE("mvc_trans_begin");
		FXNODE;
		break;
	case st_commit: 
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_trans_commit(myc);\n", s->nr );      
		XNODE("mvc_trans_commit");
		FXNODE;
		break;
	case st_rollback: 
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_trans_rollback(myc);\n", s->nr );      
		XNODE("mvc_trans_rollback");
		FXNODE;
		break;
	case st_create_schema: {
		schema *schema = s->op1.schema;
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_create_schema(myc, %ld, \"%s\", \"%s\");\n", 
			s->nr, schema->id, schema->name, schema->auth );
		XNODE("mvc_create_schema");
		FXNODE;
	} break;
	case st_create_table: {
		table *t = s->op1.tval;
		if (t->sql){
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_create_view(myc, %ld, %ld, \"%s\", \"%s\");\n", 
			s->nr, t->id, t->schema->id, t->name, t->sql );
			XNODE("mvc_create_view");
				xml_field("name", "%s", t->name);
			FXNODE;
		} else {
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_create_table(myc, %ld, %ld, \"%s\", %s);\n",
		   	s->nr, t->id, t->schema->id, t->name, 
		   		(t->temp==0)?"false":"true" );
			XNODE("mvc_create_table");
				xml_field("name", "%s", t->name);
			FXNODE;
		}
	} break;
	case st_drop_table: {
		table *t = s->op1.tval;
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_drop_table(myc, %ld, %s);\n", 
			s->nr, t->id, (s->flag==0)?"false":"true" );
		XNODE("mvc_drop_table");
		FXNODE;
	} break;
	case st_create_column: {
		column *c = s->op1.cval;
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_create_column(myc, %ld, %ld, \"%s\", \"%s\", %d);\n",
			s->nr, c->id, c->table->id, c->name, 
			c->tpe->sqlname, c->colnr );
		XNODE("mvc_create_column");
			xml_field("name", "%s", c->name);
		FXNODE;
	} break;
	case st_not_null: {
		int c;
		XNODE("mvc_not_null");
		c = statement2xml( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		    "s%d := mvc_not_null(myc, s%d );\n", s->nr, c );
		xml_field("column_var", "s%d", c);
		FXNODE;
	} break;
	case st_default: {
		int c,d;
		XNODE("default");
		c = statement2xml( s->op1.stval, nr, sql );
		d = statement2xml( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		    "s%d := default_val(myc, s%d, s%d );\n", s->nr, c, d );
		FXNODE;
	} break;
	case st_select: {
		int l,r;
		XNODE("select");		
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
		xml_field("flag", "%d", s->flag);
		switch(s->flag){
		case cmp_equal:
			if (s->op3.stval){
			    int r2 = statement2xml( s->op3.stval, nr, sql );
			    len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.uselect(s%d, s%d);\n", 
				s->nr, l, r, r2 ); 
			} else {
			    len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.uselect(s%d);\n", s->nr, l, r ); 
			}
			break;
		case cmp_notequal:
			(void)(*nr)++; 
			len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.uselect(s%d);\n", *nr, l, r ); 
			len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.kdiff(s%d);\n", s->nr, l, *nr );
			break;
		case cmp_lt:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.mil_select(\"<in>\", %s(nil), s%d);\n", 
			  s->nr, l, tail_type(s)->name, r ); 
			break;
		case cmp_lte:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.uselect(%s(nil), s%d);\n", 
			  s->nr, l, tail_type(s)->name, r ); 
			break;
		case cmp_gt:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.mil_select(\"<in>\", s%d, %s(nil));\n", 
			  s->nr, l, r, tail_type(s)->name ); 
			break;
		case cmp_gte: 
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.uselect(s%d, %s(nil));\n", 
			  s->nr, l, r, tail_type(s)->name ); 
			break;
		default:
			len += snprintf( buf+len, BUFSIZ, "error impossible\n");
	  	} 
		FXNODE;
	} break;
	case st_select2: {
		int l,r1,r2;
		XNODE("select2");
		l = statement2xml( s->op1.stval, nr, sql );
		r1 = statement2xml( s->op2.stval, nr, sql );
		r2 = statement2xml( s->op3.stval, nr, sql );
		switch(s->flag){
		case cmp_equal: len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.select(s%d, s%d);\n", 
			  s->nr, l, r1, r2 ); 
			break;
		case cmp_notequal: 
			len += snprintf( buf+len, BUFSIZ,
			  "s%d := s%d.select(s%d, s%d);\n", s->nr, l, r1, r2 ); 
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.kdiff(s%d);\n", *nr+1, l, s->nr );
			(void)(*nr)++; 
			break;
		}
		FXNODE;
	} break;
	case st_like: {
		int l,r;
		XNODE("like");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.likeselect(s%d);\n", s->nr, l, r ); 
		FXNODE;
	} break;
	case st_semijoin: {
		int l,r;
		XNODE("semijoin");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.semijoin(s%d);\n", s->nr, l, r ); 
		FXNODE;
	} break;
	case st_diff: {
		int l,r;
		XNODE("diff");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.kdiff(s%d);\n", s->nr, l, r ); 
		FXNODE;
	} break;
	case st_intersect: {
		int l,r;
		XNODE("intersect");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.sintersect(s%d);\n", s->nr, l, r ); 
		FXNODE;
	} break;
	case st_union: {
		int l,r;
		XNODE("union");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.kunion(s%d);\n", s->nr, l, r ); 
		FXNODE;
	} break;
	case st_join: {
		int l,r;
		XNODE("join");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
		xml_field("s_flag", "%d", s->flag);
		switch(s->flag){
		case cmp_equal:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d);\n", s->nr, l, r ); 
			break;
		case cmp_notequal:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d, \"!=\");\n", s->nr, l, r ); 
			break;
		case cmp_lt:
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d, \"<\");\n", s->nr, l, r ); 
			break;
		case cmp_lte: 
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d, \"<=\");\n", s->nr, l, r );
			break;
		case cmp_gt: 
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d, \">\" );\n", s->nr, l, r); 
			break;
		case cmp_gte: 
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.join(s%d, \">=\" );\n", s->nr, l, r);
			break;
		case cmp_all: /* aka cross table */
			len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.project(0).join(s%d.reverse().project(0).reverse());\n", s->nr, l, r ); 
			break;
		default:
			len += snprintf( buf+len, BUFSIZ, "error impossible\n");
	  	} 
		FXNODE;
		break;
	}
	case st_column:
		XNODE("column");
		if (s->op1.cval->s){
			s->nr = statement2xml( s->op1.cval->s, nr, sql );
		} else {
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := mvc_bind(myc, %ld); # %s.%s\n", 
			  s->nr, s->op1.cval->id, s->op1.cval->table->name, 
			  s->op1.cval->name );
		}
		FXNODE;
		break;
	case st_reverse: {
		int l;
		XNODE("reverse");
		l = statement2xml( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.reverse();\n", s->nr, l);
		FXNODE;
	} 	break;
	case st_count: {
		int l;
		XNODE("count");
		l = statement2xml( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.count();\n", s->nr, l);
		FXNODE;
	} 	break;
	case st_const: {
		int l,r;
		XNODE("const-project");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.project(s%d);\n", s->nr, l, r);
		FXNODE;
	} 	break;
	case st_mark: {
		int l;
		XNODE("mark");
		l = statement2xml( s->op1.stval, nr, sql );
		if (s->op2.stval){
			int r = statement2xml( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.reverse().mark(oid(s%d)).reverse();\n", 
			  s->nr, l, r);
		} else if (s->flag >= 0){
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.reverse().mark(oid(%d)).reverse();\n", 
			  s->nr, l, s->flag);
		} else {
			len += snprintf( buf+len, BUFSIZ, 
			  "s%d := s%d.reverse().mark().reverse();\n", s->nr, l);
		}
		FXNODE;
	} 	break;
	case st_group: {
		int l;
		XNODE("group");
		l = statement2xml( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.group();\n", s->nr, l);
		FXNODE;
	} 	break;
	case st_derive: {
		int l,r;
		XNODE("group-derive");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := s%d.group(s%d);\n", s->nr, l, r);
		FXNODE;
	} 	break;
	case st_unique: {
		int l;
		XNODE("unique");
		l = statement2xml( s->op1.stval, nr, sql );
		if (s->op2.stval){
			int r = statement2xml( s->op2.stval, nr, sql );
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.group(s%d);\n", (*nr)+1, l, r);
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.tunique().mirror().join(s%d);\n", 
				s->nr, (*nr)+1, r);
			(*nr)++;
		} else if (s->op1.stval->type == st_group || 
		           s->op1.stval->type == st_derive){
			/* dirty optimization, use CThistolinks tunique */
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.tunique().mirror();\n", s->nr, l);
		} else {
		  	len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.reverse().kunique().reverse();\n", s->nr, l);
		}
		FXNODE;
	} 	break;
	case st_order: {
		int l;
		XNODE("order");
		l = statement2xml( s->op1.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.reverse().sort().reverse();\n", s->nr, l );
		FXNODE;
	} 	break;
	case st_reorder: {
		int l,r;
		XNODE("reorder");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ, 
			"s%d := s%d.CTrefine(s%d);\n", s->nr, l, r); 
		/* s->flag?"desc":"asc"); */
		FXNODE;
	} 	break;
	case st_unop: {
		int l;
		XNODE("unop");
		l = statement2xml( s->op1.stval, nr, sql );
		if (s->op1.stval->nrcols)
		  len += snprintf( buf+len, BUFSIZ, 
		   "s%d := [%s](s%d);\n", s->nr, s->op2.funcval->imp, l );
		else 
		  len += snprintf( buf+len, BUFSIZ, 
		   "s%d := %s(s%d);\n", s->nr, s->op2.funcval->imp, l);
		FXNODE;
	} 	break;
	case st_binop: {
		int l,r;
		XNODE("binop");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
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
			s->nr, s->op3.funcval->imp, l, r );
		} else  {
		  	len += snprintf( buf+len, BUFSIZ, 
		    	"s%d := %s(s%d,s%d);\n", 
			s->nr, s->op3.funcval->imp, l,r );
		}
		FXNODE;
	} 	break;
	case st_triop: {
		statement *op1 = s->op1.lval->h->data.stval;
		statement *op2 = s->op1.lval->h->next->data.stval;
		statement *op3 = s->op1.lval->h->next->next->data.stval;
		int r1,r2,r3;
		XNODE("triop");
		r1 = statement2xml( op1, nr, sql );
		r2 = statement2xml( op2, nr, sql );
		r3 = statement2xml( op3, nr, sql );
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
			s->nr, s->op2.funcval->imp, r1, r2, r3 );
		} else {
		  len += snprintf( buf+len, BUFSIZ, 
		    "s%d := %s(s%d,s%d,s%d);\n", s->nr, s->op2.funcval->imp, 
		    	r1, r2, r3 );
		}
		FXNODE;
	} 	break;
	case st_aggr: {
		int l ;
		XNODE("aggr");
		l = statement2xml( s->op1.stval, nr, sql );
		if (s->op3.stval){
			int r = statement2xml( s->op3.stval, nr, sql );

			if (s->op1.stval == s->op3.stval){
				len += snprintf( buf+len, BUFSIZ, 
				"s%d := {%s}(s%d.reverse(), s%d.tunique());\n", 
				s->nr, s->op2.aggrval->imp, l, r);
			} else if (s->op1.stval->type == st_column){
				len += snprintf( buf+len, BUFSIZ, 
				"s%d := {%s}(s%d, s%d, s%d.tunique());\n", 
				s->nr, s->op2.aggrval->imp, l, r, r);
			} else {
				len += snprintf( buf+len, BUFSIZ, 
				"s%d := {%s}(s%d.reverse().join(s%d),s%d.tunique());\n",
			        s->nr, s->op2.aggrval->imp, r, l, r);
			}
		} else {
			len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.%s();\n", 
				s->nr, l, s->op2.aggrval->imp );
			len += snprintf( buf+len, BUFSIZ, 
				"s%d := new(oid,%s);\n"
				, *nr+1, s->op2.aggrval->res->name );
			len += snprintf( buf+len, BUFSIZ, 
				"s%d.insert(oid(0),s%d);\n"
				, *nr+1, s->nr );
			(*nr)++;
		}
		FXNODE;
	} 	break;
	case st_exists: {
		int l,k,r;
		XNODE("exists-new");
		l = statement2xml( s->op1.stval, nr, sql );
		k = *nr;
		r = 1;

		n = s->op2.lval->h;

		if (n){
		  	char *a = (char*)atom_type(n->data.aval )->name;
			len += snprintf( buf+len, BUFSIZ, 
				"s%d := new(%s,oid);\n", s->nr, a );
		}
		k++;
		while(n){
			len += snprintf( buf+len, BUFSIZ, "s%d := %s;\n", k, 
				atom2xml(n->data.aval) );
			len += snprintf( buf+len, BUFSIZ, 
				"s%d.insert(s%d, oid(%d));\n", s->nr, k++, r++);
			n = n->next;
		}
		len += snprintf( buf+len, BUFSIZ, 
				"s%d := s%d.join(s%d);\n", k, l, s->nr);
		*nr = k;
		FXNODE;
	} 	break;
	case st_atom: {
		XNODE("atom");
		len += snprintf( buf+len, BUFSIZ, 
				"s%d := %s;\n", s->nr, atom2xml(s->op1.aval));
		FXNODE;
	} break;
	case st_insert_column: {
		int l,r;
		XNODE("insert_column");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := insert(s%d.access(BAT_WRITE),s%d);\n", s->nr, l, r);
		FXNODE;
	} break;
	case st_update: {
		int r;
		XNODE("update");
		r = statement2xml( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := mvc_update(myc, oid(%ld), oid(%ld), s%d);\n", 
		    s->nr,	s->op1.cval->table->id, s->op1.cval->id, r);
		FXNODE;
	} break;
	case st_replace: {
		int l,r;
		XNODE("update");
		l = statement2xml( s->op1.stval, nr, sql );
		r = statement2xml( s->op2.stval, nr, sql );
		len += snprintf( buf+len, BUFSIZ, 
		  "s%d := [oid](s%d.reverse()).reverse().access(BAT_WRITE).replace(s%d);\n", 
		  s->nr, l, r);
		FXNODE;
	} break;
	case st_delete: {
		XNODE("delete");
		if (s->op2.stval){
			int l = statement2xml( s->op2.stval, nr, sql );
			len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_delete(myc, oid(%ld), s%d);\n", 
			s->nr, s->op1.tval->id, l);
		} else {
			len += snprintf( buf+len, BUFSIZ, 
			"s%d := mvc_delete(myc, %ld, nil);\n",
			  s->nr, s->op1.tval->id );
		}
		FXNODE;
	} break;
	case st_name: 
		XNODE("name");
		s->nr = statement2xml( s->op1.stval, nr, sql );
		FXNODE;
		break;
	case st_set: {
		XNODE("set");
		for (n = s->op1.lval->h; n; n = n->next ){
			(void)statement2xml( n->data.stval, nr, sql );
		}
		FXNODE;
	} break;
	case st_sets: {
		XNODE("sets");
		for(n = s->op1.lval->h; n; n->next ){
			list *l = n->data.lval;
			node *m = l->h;
			while(m){
				(void)statement2xml( m->data.stval, nr, sql );
				m = m->next;
			}
		}
		FXNODE;
	} break;
	case st_list: {
		XNODE("list");
		for( n = s->op1.lval->h; n; n = n->next ){
			(void)statement2xml( n->data.stval, nr, sql );
		}
		FXNODE;
	} break;
	case st_insert: {
		XNODE("insert");
		if (!(sql->optimize & SQL_FAST_INSERT)){
			for( n = s->op2.lval->h ;n; n = n->next ){
				statement2xml(n->data.stval, nr, sql);
			}
			len += snprintf( buf+len, BUFSIZ, 
				"mvc_insert(myc, %ld", s->op1.tval->id );
			for( n = s->op2.lval->h ;n; n = n->next ){
				len += snprintf( buf+len, BUFSIZ, 
					",s%d", n->data.stval->nr);
			}
			len += snprintf( buf+len, BUFSIZ, ");\n" );
		} else {
			len += snprintf( buf+len, BUFSIZ, "0,%ld,", 
				      s->op1.tval->id );
			for( n = s->op2.lval->h ;n; n = n->next ){
				char *s = NULL;
				statement *r = n->data.stval;
				if (r->op3.stval){
					statement *a = r->op3.stval;
					while(a->type == st_unop){ /* cast */
						a = a->op1.stval;
					}
					len += snprintf( buf+len, BUFSIZ, "%s,",
					   s = atom2xml_fast(a->op1.aval) );
				} else {
					len += snprintf( buf+len, BUFSIZ, "%s,",
					   s = atom2xml_fast(r->op1.aval) );
				}
				_DELETE(s);
			}
			len += snprintf( buf+len, BUFSIZ, "\n" );
		}
		FXNODE;
	} break;
	case st_ordered: {
		int l;
		XNODE("ordered");
		l =  statement2xml( s->op1.stval, nr, sql );
		(void)statement2xml( s->op2.stval, nr, sql );
		s->nr = l;
		FXNODE;
	} break;
	case st_output: {
		statement *order = NULL;
		statement *lst = s->op1.stval;
		XNODE("output");
		statement2xml( lst, nr, sql );
		if (lst->type == st_ordered){
			order = lst->op1.stval; 
			lst = lst->op2.stval; 
		}
		if (lst->type == st_list){
			list *l = lst->op1.lval;

			n = l->h;
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
		FXNODE;
	} break;

	}

    	return s->nr;
    }
    return 0;
}


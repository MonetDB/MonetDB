
#include <mem.h>
#include "sql.h"

#include "atom.h"
/* todo, be able to handle generic atom types, also other the SQL parser
   supports */

static int atom_debug = 0;

atom *atom_int( int val ){
	atom *a = NEW(atom);
	a->data.ival = val;
	a->type = int_value;
	if(atom_debug)
		fprintf(stderr, "atom_int(%d)\n", val );
	return a;
}
atom *atom_string( char *val ){
	atom *a = NEW(atom);
	a->data.sval = val;
	a->type = string_value;
	if(atom_debug)
		fprintf(stderr, "atom_string(%s)\n", val );
	return a;
}
atom *atom_float( double val ){
	atom *a = NEW(atom);
	a->data.dval = val;
	a->type = float_value;
	if(atom_debug)
		fprintf(stderr, "atom_float(%f)\n", val );
	return a;
}

void atom_destroy( atom *a ){
	if (a->type == string_value)
		_DELETE(a->data.sval);
	_DELETE(a);
}


char *atom2string( atom *a){
	char buf[1024];
	switch (a->type){
	case int_value: sprintf(buf, "%d", a->data.ival); break;
	case string_value: return addQuotes( a->data.sval);
	case float_value: sprintf(buf, "%f", a->data.dval); break;
	}
	return _strdup(buf);
}

char *atomtype2string( atom *a){
	switch (a->type){
	case string_value: return _strdup("STRING");
	case int_value: return _strdup("INTEGER");
	case float_value: return _strdup("DOUBLE");
	}
	return _strdup("");
}

atom *atom_dup( atom *a ){
	atom *r = NEW(atom);
	*r = *a;
	if (a->type == string_value)
		r->data.sval = _strdup(a->data.sval);
	return r;
}

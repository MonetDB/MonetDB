
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
atom *atom_date( char *val ){
	atom *a = NEW(atom);
	a->data.sval = val;
	a->type = date_value;
	if(atom_debug)
		fprintf(stderr, "atom_date(%s)\n", val );
	return a;
}
atom *atom_time( char *val ){
	atom *a = NEW(atom);
	a->data.sval = val;
	a->type = time_value;
	if(atom_debug)
		fprintf(stderr, "atom_time(%s)\n", val );
	return a;
}
atom *atom_timestamp( char *val ){
	atom *a = NEW(atom);
	a->data.sval = val;
	a->type = timestamp_value;
	if(atom_debug)
		fprintf(stderr, "atom_timestamp(%s)\n", val );
	return a;
}
atom *atom_month_interval( int val ){
	atom *a = NEW(atom);
	a->data.ival = val;
	a->type = month_interval_value;
	if(atom_debug)
		fprintf(stderr, "atom_month_interval(%d)\n", val );
	return a;
}
atom *atom_sec_interval( int val ){
	atom *a = NEW(atom);
	a->data.ival = val;
	a->type = sec_interval_value;
	if(atom_debug)
		fprintf(stderr, "atom_sec_interval(%d)\n", val );
	return a;
}

void atom_destroy( atom *a ){
	if (a->type == string_value || a->type == date_value 
	 || a->type == time_value || a->type == timestamp_value)
		_DELETE(a->data.sval);
	_DELETE(a);
}


char *atom2string( atom *a){
	char buf[1024];
	switch (a->type){
	case int_value: sprintf(buf, "%d", a->data.ival); break;
	case string_value: return addQuotes( a->data.sval);
	case float_value: sprintf(buf, "%f", a->data.dval); break;
	case date_value: sprintf(buf, "%sdate(\"%s\")", atom_prefix,a->data.sval); break;
	case time_value: sprintf(buf, "%stime(\"%s\")", atom_prefix,a->data.sval); break;
	case timestamp_value: 
			sprintf(buf, "%stimestamp(\"%s\")", atom_prefix,a->data.sval); break;
	case month_interval_value: 
			sprintf(buf, "%d", a->data.ival); break;
	case sec_interval_value: 
			sprintf(buf, "%d", a->data.ival); break;
	default:
	}
	return _strdup(buf);
}

const char *atomtype2string( atom *a){
	switch (a->type){
	case string_value: return ("STRING");
	case int_value: return ("INTEGER");
	case float_value: return ("DOUBLE");
	case date_value: return ("DATE");
	case time_value: return ("TIME");
	case timestamp_value: return ("TIMESTAMP");
	case month_interval_value: return ("MONTH_INTERVAL");
	case sec_interval_value: return ("SEC_INTERVAL");
	}
	return ("");
}

atom *atom_dup( atom *a ){
	atom *r = NEW(atom);
	*r = *a;
	if (a->type == string_value || a->type == date_value 
	 || a->type == time_value || a->type == timestamp_value)
		r->data.sval = _strdup(a->data.sval);
	return r;
}

#ifndef _ATOM_H_
#define _ATOM_H_

#define atom_prefix ""

typedef	enum atomtype {
	string_value,
	int_value,
	float_value,
	date_value,
	time_value,
	timestamp_value,
	month_interval_value,
	sec_interval_value,
} atomtype;

typedef struct atom {
	atomtype type;
	union { 
		int ival;
		char *sval;
		double dval;
	} data;
} atom;


extern atom *atom_int( int val );
extern atom *atom_string( char *val );
extern atom *atom_float( double val );

extern atom *atom_date( char *val );
extern atom *atom_time( char *val );
extern atom *atom_timestamp( char *val );
extern atom *atom_month_interval( int val );
extern atom *atom_sec_interval( int val );

/* duplicate atom */
extern atom *atom_dup( atom *a );

extern char *atom2string(atom *a);
extern const char *atomtype2string(atom *a);

extern void atom_destroy( atom *a );
#endif /* _ATOM_H_ */

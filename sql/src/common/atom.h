#ifndef _ATOM_H_
#define _ATOM_H_

/* change to generic interface 
 * 
 * Need atom2str atom2typestring
 * Atom will be a wrapper around specific atom's like
 * intAtom, strAtom etc.
 */

typedef	enum atomtype {
	string_value,
	int_value,
	float_value,
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
/* duplicate atom */
extern atom *atom_dup( atom *a );

extern char *atom2string(atom *a);
extern const char *atomtype2string(atom *a);

extern void atom_destroy( atom *a );
#endif /* _ATOM_H_ */

#ifndef _ATOM_H_
#define _ATOM_H_

#include "catalog.h"

typedef enum atomtype {
	string_value,
	int_value,
	float_value,
	general_value
} atomtype;

typedef struct atom {
	sql_type *tpe;
	atomtype type;
	union {
		int ival;
		char *sval;
		double dval;
	} data;
} atom;


extern atom *atom_int(sql_type * tpe, int val);
extern atom *atom_string(sql_type * tpe, char *val);
extern atom *atom_float(sql_type * tpe, double val);
extern atom *atom_general(sql_type * tpe, char *val);

/* duplicate atom */
extern atom *atom_dup(atom * a);

extern char *atom2string(atom * a);
extern sql_type *atom_type(atom * a);

extern void atom_destroy(atom * a);
#endif				/* _ATOM_H_ */

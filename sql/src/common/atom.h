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
	sql_subtype tpe;
	atomtype type;
	union {
		int ival;
		char *sval;
		double dval;
	} data;
} atom;

extern atom *atom_int(sql_subtype * tpe, int val);
extern atom *atom_string(sql_subtype * tpe, char *val);
extern atom *atom_float(sql_subtype * tpe, double val);
extern atom *atom_general(sql_subtype * tpe, char *val);

/* duplicate atom */
extern atom *atom_dup(atom * a);

extern char *atom2string(atom * a);
extern sql_subtype *atom_type(atom * a);

extern void atom_destroy(atom * a);
#endif				/* _ATOM_H_ */

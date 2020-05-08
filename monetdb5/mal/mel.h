#ifndef _MEL_H_
#define _MEL_H_

#include <stdbool.h>
#include <stdio.h>

#define MAX_ARG 8

typedef void* (*fptr)(void*);

typedef struct mel_atom {
	char *name;
	char *basetype;
	fptr tostr;
	fptr fromstr;
	fptr cmp;
	fptr hash;
	fptr null;
	fptr read;
	fptr write;
	fptr put;
	fptr del;
	fptr length;
	fptr heap;
} mel_atom;

typedef struct mel_arg {
	char *name;
	char *type;
	bool isbat;
	bool vargs;
} mel_arg;

typedef struct mel_func {
	bool command;
	char *mod;
	char *fcn;
	fptr imp;
	bool unsafe;
	char *comment;
	mel_arg args[MAX_ARG];
	mel_arg res[MAX_ARG];
} mel_func;

#endif /* _MEL_H_ */

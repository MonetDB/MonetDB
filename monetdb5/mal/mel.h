#ifndef _MEL_H_
#define _MEL_H_

#include <stdbool.h>
#include <stdio.h>

#define MEL_STR

#define MEL_OK 0
#define MEL_ERR 1

typedef void* (*fptr)(void*);

typedef struct mel_atom {
	char name[14];
	char basetype[14];
	int size;
	fptr tostr;
	fptr fromstr;
	fptr cmp;
	fptr nequal;
	fptr hash;
	fptr null;
	fptr read;
	fptr write;
	fptr put;
	fptr del;
	fptr length;
	fptr heap;
	fptr fix;
	fptr unfix;
	fptr storage;
} mel_atom;

/*strings */
#ifdef MEL_STR

#define command(MOD,FCN,IMP,UNSAFE,COMMENT,ARGS) { .command=true, .mod=MOD, .fcn=FCN, .imp=(fptr)&IMP, .cname=#IMP, .unsafe=UNSAFE, .args=ARGS }
#define pattern(MOD,FCN,IMP,UNSAFE,COMMENT,ARGS) { .command=false, .mod=MOD, .fcn=FCN, .imp=(fptr)&IMP, .cname=#IMP, .unsafe=UNSAFE, .args=ARGS }

//#define args(RETC,ARGC,...) (mel_arg[ARGC?ARGC:1]){__VA_ARGS__}, .retc=RETC, .argc=ARGC
#define args(RETC,ARGC,...) (mel_arg[ARGC]){__VA_ARGS__}, .retc=RETC, .argc=ARGC
#define noargs		    NULL, .retc=0, .argc=0

#define arg(n,t) 		{ /*.name=n,*/ .type=# t }
#define vararg(n,t) 		{ /*.name=n,*/ .type=# t, .vargs=1 }
#define batarg(n,t) 		{ /*.name=n,*/ .type=# t, .isbat=1 }
#define batvararg(n,t) 		{ /*.name=n,*/ .type=# t, .isbat=1, .vargs=1 }
#define argany(n,a) 		{ /*.name=n,*/ .nr=a, }
#define varargany(n,a) 		{ /*.name=n,*/ .nr=a, .vargs=1, }
#define batargany(n,a) 		{ /*.name=n,*/ .isbat=1, .nr=a, }
#define batvarargany(n,a) 	{ /*.name=n,*/ .isbat=1, .vargs=1, .nr=a, }

typedef struct mel_arg {
	//char *name;
	char type[15];
	char isbat:1,
	     vargs:1,
	     nr:4;
} mel_arg;

typedef struct mel_func {
	char mod[14];
	char fcn[30];
	char *cname;
	short
	     command:1,
	     unsafe:1,
	     retc:6,
	     argc:6; 
//#ifdef NDEBUG
	//char *comment;
//#endif
	fptr imp;
	mel_arg *args;
} mel_func;

#else

#define TYPE_bstream TYPE_ptr
#define TYPE_streams TYPE_ptr
#define TYPE_url 17
#define TYPE_json 18
#define TYPE_uuid 19
#define TYPE_blob 20
#define TYPE_identifier 21
#define TYPE_inet 22
#define TYPE_xml 23
#define TYPE_color 24
#define TYPE_wkb 25

//#ifdef NDEBUG
#define command(MOD,FCN,IMP,UNSAFE,COMMENT,ARGS) { .command=true, .mod=MOD, .fcn=FCN, .imp=(fptr)&IMP, .unsafe=UNSAFE, .args=ARGS }
#define pattern(MOD,FCN,IMP,UNSAFE,COMMENT,ARGS) { .command=false, .mod=MOD, .fcn=FCN, .imp=(fptr)&IMP, .unsafe=UNSAFE, .args=ARGS }
//#else
//#define command(MOD,FCN,IMP,UNSAFE,COMMENT,ARGS) { .command=true, .mod=MOD, .fcn=FCN, .imp=(fptr)&IMP, .unsafe=UNSAFE, .comment=COMMENT, .args=ARGS }
//#define pattern(MOD,FCN,IMP,UNSAFE,COMMENT,ARGS) { .command=false, .mod=MOD, .fcn=FCN, .imp=(fptr)&IMP, .unsafe=UNSAFE, .comment=COMMENT, .args=ARGS }
//#endif

#define args(RETC,ARGC,...) {__VA_ARGS__}, .retc=RETC, .argc=ARGC

#define arg(n,t) 		{ /*.name=n,*/ .type=TYPE_##t }
#define vararg(n,t) 		{ /*.name=n,*/ .type=TYPE_##t, .vargs=1 }
#define batarg(n,t) 		{ /*.name=n,*/ .type=TYPE_##t, .isbat=1 }
#define batvararg(n,t) 		{ /*.name=n,*/ .type=TYPE_##t, .isbat=1, .vargs=1 }
#define argany(n,a) 		{ /*.name=n,*/ .nr=a, .type=TYPE_any }
#define varargany(n,a) 		{ /*.name=n,*/ .nr=a, .vargs=1, .type=TYPE_any }
#define batargany(n,a) 		{ /*.name=n,*/ .isbat=1, .nr=a, .type=TYPE_any }
#define batvarargany(n,a) 	{ /*.name=n,*/ .isbat=1, .vargs=1, .nr=a, .type=TYPE_any }

typedef struct mel_arg {
	unsigned short  type:8,
			isbat:1,
	     		vargs:1,
	     		nr:4;
} mel_arg;

typedef struct mel_func {
	char mod[14];
	char fcn[30];
	short
	     command:1,
	     unsafe:1,
	     retc:6,
	     argc:6; 
//#ifdef NDEBUG
	//char *comment;
//#endif
	fptr imp;
	mel_arg args[20];
} mel_func;

#endif

typedef str(*mel_init)(void);

typedef struct mel_func_arg {
	unsigned short  type:8,
		 	isbat:1,
	     		vargs:1,
	     		nr:4;
} mel_func_arg;

/* var arg of arguments of type mel_func_arg */
int melFunction(bool command, char *mod, char *fcn, fptr imp, char *fname, bool unsafe, char *comment, int retc, int argc, ...);

#ifdef SPECS
typedef struct mal_spec{
	fptr imp;
	char *mal;
} mal_spec;
#endif

#endif /* _MEL_H_ */

/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _MEL_H_
#define _MEL_H_

#include <stdbool.h>
#include <stdio.h>

#define MEL_OK 0
#define MEL_ERR 1

struct CLIENT;
struct MALBLK;
struct MALSTK;
struct INSTR;

typedef struct __attribute__((__designated_init__)) mel_atom {
	char name[14];
	char basetype[14];
	int size;
	ssize_t (*tostr)(char **, size_t *, const void *, bool);
	ssize_t (*fromstr)(const char *, size_t *, void **, bool);
	int (*cmp)(const void *, const void *);
	int (*nequal)(const void *, const void *);
	BUN (*hash)(const void *);
	const void *(*null)(void);
	void *(*read)(void *, size_t *, stream *, size_t);
	gdk_return (*write)(const void *, stream *, size_t);
	var_t (*put)(BAT *, var_t *, const void *);
	void (*del)(Heap *, var_t *);
	size_t (*length)(const void *);
	gdk_return (*heap)(Heap *, size_t);
	int (*storage)(void);
} mel_atom;

#define command(MOD,FCN,IMP,UNSAFE,COMMENT,ARGS) { .command=1, .mod=MOD, .fcn=FCN, .imp=(MALfcn)IMP, .cname=#IMP, .unsafe=UNSAFE, .args=ARGS, .comment=COMMENT }
#define pattern(MOD,FCN,IMP,UNSAFE,COMMENT,ARGS) { .command=0, .mod=MOD, .fcn=FCN, .pimp=IMP, .cname=#IMP, .unsafe=UNSAFE, .args=ARGS, .comment=COMMENT }

/* ARGC = arg-count + ret-count */
//#define args(RETC,ARGC,...) (mel_arg[ARGC?ARGC:1]){__VA_ARGS__}, .retc=RETC, .argc=ARGC
#define args(RETC,ARGC,...) (mel_arg[ARGC]){__VA_ARGS__}, .retc=RETC, .argc=ARGC
#define noargs		    NULL, .retc=0, .argc=0

#define arg(n,t)			{ /*.name=n,*/ .type=# t }
#define optbatarg(n,t)		{ /*.name=n,*/ .type=# t, .opt=1 }
#define vararg(n,t)			{ /*.name=n,*/ .type=# t, .vargs=true }
#define batarg(n,t)			{ /*.name=n,*/ .type=# t, .isbat=true }
#define batvararg(n,t)		{ /*.name=n,*/ .type=# t, .isbat=true, .vargs=true }
#define argany(n,a)			{ /*.name=n,*/ .nr=a, }
#define varargany(n,a)		{ /*.name=n,*/ .nr=a, .vargs=true, }
#define batargany(n,a)		{ /*.name=n,*/ .isbat=true, .nr=a, }
#define optbatargany(n,a)	{ /*.name=n,*/ .nr=a, .opt=1, }
#define batvarargany(n,a)	{ /*.name=n,*/ .isbat=true, .vargs=true, .nr=a, }

typedef struct __attribute__((__designated_init__)) mel_arg {
	//char *name;
	char type[14];
	uint16_t typeid:8, nr:2, isbat:1, vargs:1, opt:1;
} mel_arg;

/* nr for any types 0, 1,2 */

typedef struct __attribute__((__designated_init__)) mel_func {
	const char *mod;
	const char *fcn;
	const char *cname;
	const char *comment;
	uint32_t command:1, unsafe:1, vargs:1, vrets:1, poly:3, retc:5, argc:5;
	union {
		MALfcn imp;
		char *(*pimp)(struct CLIENT *, struct MALBLK *, struct MALSTK *, struct INSTR *);
	};
	mel_arg *args;
} mel_func;

typedef str (*mel_init)(void);

typedef struct __attribute__((__designated_init__)) mel_func_arg {
	uint16_t type:8, nr:2, isbat:1, vargs:1, opt:1;
} mel_func_arg;

/* var arg of arguments of type mel_func_arg */
int melFunction(bool command, const char *mod, const char *fcn, MALfcn imp,
				const char *fname, bool unsafe, const char *comment, int retc,
				int argc, ...);

#ifdef SPECS
typedef struct __attribute__((__designated_init__)) mal_spec {
	union {
		MALfcn imp;
		char *(*pimp)(struct CLIENT *, struct MALBLK *, struct MALSTK *, struct INSTR *);
	};
	char *mal;
} mal_spec;
#endif

#endif /* _MEL_H_ */

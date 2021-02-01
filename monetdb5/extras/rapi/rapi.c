/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * H. Muehleisen, M. Kersten
 * The R interface
 */
#include "monetdb_config.h"
#include "mal.h"
#include "mal_stack.h"
#include "mal_linker.h"
#include "gdk_utils.h"
#include "gdk.h"
#include "sql_catalog.h"
#include "sql_execute.h"
#include "mutils.h"

#define RAPI_MAX_TUPLES 2147483647L

// R headers
#define R_INTERFACE_PTRS 1
#define CSTACK_DEFNS 1

/* R redefines these */
#undef SIZEOF_SIZE_T
#undef ERROR

#define USE_RINTERNALS 1

#include <Rversion.h>
#include <Rembedded.h>
#include <Rdefines.h>
#include <Rinternals.h>
#include <R_ext/Parse.h>

// other headers
#include <string.h>

//#define _RAPI_DEBUG_

// this macro blows up mmath.h pragmas
#ifdef warning
# undef warning
#endif

#define RSTR(somestr) mkCharCE(somestr, CE_UTF8)

//Element-wise conversion functions, use no-op as passthrough when no conversion required
#define M_TO_R_NOOP(v)               (v)
#define R_TO_M_NOOP(v)               (v)
#define M_TO_R_DATE(v)               mDate_to_rDate(v)
#define R_TO_M_DATE(v)               rDate_to_mDate(v)

#define BAT_TO_SXP(bat,tpe,retsxp,newfun,ptrfun,ctype,naval,memcopy,mapfun)	\
	do {																\
		tpe v; size_t j;												\
		ctype *valptr = NULL;											\
		tpe* p = (tpe*) Tloc(bat, 0);									\
		retsxp = PROTECT(newfun(BATcount(bat)));						\
		if (!retsxp) break;												\
		valptr = ptrfun(retsxp);										\
		if (bat->tnonil && !bat->tnil) {								\
			if (memcopy) {												\
				memcpy(valptr, p,										\
					BATcount(bat) * sizeof(tpe));						\
			} else {													\
				for (j = 0; j < BATcount(bat); j++) {					\
					valptr[j] = mapfun((ctype) p[j]);					\
				}														\
			}															\
		} else {														\
		for (j = 0; j < BATcount(bat); j++) {							\
			v = p[j];													\
			if ( is_##tpe##_nil(v))										\
				valptr[j] = naval;										\
			else														\
				valptr[j] = mapfun((ctype) v);							\
		}}																\
	} while (0)

#define BAT_TO_INTSXP(bat,tpe,retsxp,memcopy)						\
	BAT_TO_SXP(bat,tpe,retsxp,NEW_INTEGER,INTEGER_POINTER,int,NA_INTEGER,memcopy,M_TO_R_NOOP)\

#define BAT_TO_REALSXP(bat,tpe,retsxp,memcopy)						\
	BAT_TO_SXP(bat,tpe,retsxp,NEW_NUMERIC,NUMERIC_POINTER,double,NA_REAL,memcopy,M_TO_R_NOOP)\

//DATE stored as integer in MonetDB with epoch 0, R uses double and epoch 1970
#define BAT_TO_DATESXP(bat,tpe,retsxp,memcopy)							\
	BAT_TO_SXP(bat,tpe,retsxp,NEW_NUMERIC,NUMERIC_POINTER,double,NA_REAL,memcopy, M_TO_R_DATE); \
	SEXP klass = mkString("Date");										\
	classgets(retsxp, klass);

#define SXP_TO_BAT(tpe, access_fun, na_check, mapfun)					\
	do {																\
		tpe *p, prev = tpe##_nil; size_t j;								\
		b = COLnew(0, TYPE_##tpe, cnt, TRANSIENT);						\
		if (!b) break;                                                  \
		b->tnil = false; b->tnonil = true; b->tkey = false;				\
		b->tsorted = true; b->trevsorted = true;						\
		b->tseqbase = oid_nil;											\
		p = (tpe*) Tloc(b, 0);											\
		for( j = 0; j < cnt; j++, p++){								    \
			*p = mapfun((tpe) access_fun(s)[j]);						\
			if (na_check){ b->tnil = true; 	b->tnonil = false; 	*p= tpe##_nil;} \
			if (j > 0){													\
				if (b->trevsorted && !is_##tpe##_nil(*p) && (is_##tpe##_nil(prev) || *p > prev)){ \
					b->trevsorted = false;								\
				} else													\
					if (b->tsorted && !is_##tpe##_nil(prev) && (is_##tpe##_nil(*p) || *p < prev)){ \
						b->tsorted = false;								\
					}													\
			}															\
			prev = *p;													\
		}																\
		BATsetcount(b, cnt);											\
		BATsettrivprop(b);												\
	} while (0)

// DATE epoch differs betwen MonetDB (00-01-01) and R (1970-01-01)
// no c API for R date handling so use fixed offset
// >>`-as.double(as.Date(0, origin="0-1-1"))`
static const int days0To1970 = 719528;

static int
mDate_to_rDate(int v)
{
	return v-days0To1970;
}

static int
rDate_to_mDate(int v)
{
	return v+days0To1970;
}

static SEXP
bat_to_sexp(BAT* b, int type)
{
	SEXP varvalue = NULL;
	// TODO: deal with SQL types (DECIMAL/TIME/TIMESTAMP)
	switch (ATOMstorage(b->ttype)) {
	case TYPE_void: {
		size_t i = 0;
		varvalue = PROTECT(NEW_LOGICAL(BATcount(b)));
		if (!varvalue) {
			return NULL;
		}
		for (i = 0; i < BATcount(b); i++) {
			LOGICAL_POINTER(varvalue)[i] = NA_LOGICAL;
		}
		break;
	}
	case TYPE_bte:
		BAT_TO_INTSXP(b, bte, varvalue, 0);
		break;
	case TYPE_sht:
		BAT_TO_INTSXP(b, sht, varvalue, 0);
		break;
	case TYPE_int:
		//Storage is int but the actual defined type may be different
		switch (type) {
		case TYPE_int:
			//Storage is int but the actual defined type may be different
			switch (type) {
				case TYPE_int: {
					// special case: memcpy for int-to-int conversion without NULLs
					BAT_TO_INTSXP(b, int, varvalue, 1);
				} break;
				default: {
					if (type == ATOMindex("date")) {
						BAT_TO_DATESXP(b, int, varvalue, 0);
					} else {
						//Type stored as int but no implementation to decode into native R type
						BAT_TO_INTSXP(b, int, varvalue, 1);
					}
				}
			}
			break;
		default:
			if (type == TYPE_date) {
				BAT_TO_DATESXP(b, int, varvalue, 0);
			} else {
				//Type stored as int but no implementation to decode into native R type
				BAT_TO_INTSXP(b, int, varvalue, 1);
			}
			break;
		}
		break;
#ifdef HAVE_HGE
	case TYPE_hge: /* R's integers are stored as int, so we cannot be sure hge will fit */
		BAT_TO_REALSXP(b, hge, varvalue, 0);
		break;
#endif
	case TYPE_flt:
		BAT_TO_REALSXP(b, flt, varvalue, 0);
		break;
	case TYPE_dbl:
		// special case: memcpy for double-to-double conversion without NULLs
		BAT_TO_REALSXP(b, dbl, varvalue, 1);
		break;
	case TYPE_lng: /* R's integers are stored as int, so we cannot be sure long will fit */
		BAT_TO_REALSXP(b, lng, varvalue, 0);
		break;
	case TYPE_str: { // there is only one string type, thus no macro here
		BUN p, q, j = 0;
		BATiter li = bat_iterator(b);
		varvalue = PROTECT(NEW_STRING(BATcount(b)));
		if (varvalue == NULL) {
			return NULL;
		}
		/* special case where we exploit the duplicate-eliminated string heap */
		if (GDK_ELIMDOUBLES(b->tvheap)) {
			SEXP* sexp_ptrs = GDKzalloc(b->tvheap->free * sizeof(SEXP));
			if (!sexp_ptrs) {
				return NULL;
			}
			BATloop(b, p, q) {
				const char *t = (const char *) BUNtvar(li, p);
				ptrdiff_t offset = t - b->tvheap->base;
				if (!sexp_ptrs[offset]) {
					if (strNil(t)) {
						sexp_ptrs[offset] = NA_STRING;
					} else {
						sexp_ptrs[offset] = RSTR(t);
					}
				}
				SET_STRING_ELT(varvalue, j++, sexp_ptrs[offset]);
			}
			GDKfree(sexp_ptrs);
		}
		else {
			if (b->tnonil) {
				BATloop(b, p, q) {
					SET_STRING_ELT(varvalue, j++, RSTR(
									   (const char *) BUNtvar(li, p)));
				}
			}
			else {
				BATloop(b, p, q) {
					const char *t = (const char *) BUNtvar(li, p);
					if (strNil(t)) {
						SET_STRING_ELT(varvalue, j++, NA_STRING);
					} else {
						SET_STRING_ELT(varvalue, j++, RSTR(t));
					}
				}
			}
		}
	} 	break;
	}
	return varvalue;
}

static BAT* sexp_to_bat(SEXP s, int type) {
	BAT* b = NULL;
	BUN cnt = LENGTH(s);
	switch (type) {
	case TYPE_int:
		if (!IS_INTEGER(s)) {
			return NULL;
		}
		SXP_TO_BAT(int, INTEGER_POINTER, *p==NA_INTEGER, R_TO_M_NOOP);
		break;
	case TYPE_lng:
		if (!IS_INTEGER(s)) {
			return NULL;
		}
		SXP_TO_BAT(lng, INTEGER_POINTER, *p==NA_INTEGER, R_TO_M_NOOP);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (!IS_INTEGER(s)) {
			return NULL;
		}
		SXP_TO_BAT(hge, INTEGER_POINTER, *p==NA_INTEGER, R_TO_M_NOOP);
		break;
#endif
	case TYPE_bte:
	case TYPE_bit:			 // only R logical types fit into bit BATs
		if (!IS_LOGICAL(s)) {
			return NULL;
		}
		SXP_TO_BAT(bit, LOGICAL_POINTER, *p==NA_LOGICAL, R_TO_M_NOOP);
		break;
	case TYPE_dbl:
		if (!IS_NUMERIC(s)) {
			return NULL;
		}
		SXP_TO_BAT(dbl, NUMERIC_POINTER, (ISNA(*p) || isnan(*p) || isinf(*p)), R_TO_M_NOOP);
		break;
	case TYPE_str: {
		SEXP levels;
		size_t j;
		if (!IS_CHARACTER(s) && !isFactor(s)) {
			return NULL;
		}
		b = COLnew(0, TYPE_str, cnt, TRANSIENT);
		if (!b) return NULL;
		b->tnil = false;
		b->tnonil = true;
		b->tkey = false;
		b->tsorted = false;
		b->trevsorted = false;
		/* get levels once, since this is a function call */
		levels = GET_LEVELS(s);

		for (j = 0; j < cnt; j++) {
			SEXP rse;
			if (isFactor(s)) {
				int ii = INTEGER(s)[j];
				if (ii == NA_INTEGER) {
					rse = NA_STRING;
				} else {
					rse = STRING_ELT(levels, ii - 1);
				}
			} else {
				rse = STRING_ELT(s, j);
			}
			if (rse == NA_STRING) {
				b->tnil = true;
				b->tnonil = false;
				if (BUNappend(b, str_nil, false) != GDK_SUCCEED) {
					BBPreclaim(b);
					return NULL;
				}
			} else {
				if (BUNappend(b, CHAR(rse), false) != GDK_SUCCEED) {
					BBPreclaim(b);
					return NULL;
				}
			}
		}
		break;
	}
	default:
		if (type == TYPE_date) {
			if (!IS_NUMERIC(s)) {
				return NULL;
			}
			SXP_TO_BAT(date, NUMERIC_POINTER, *p==NA_REAL, R_TO_M_DATE);
		}
	}

	if (b) {
		BATsetcount(b, cnt);
		BBPkeepref(b->batCacheid);
	}
	return b;
}

const char* rapi_enableflag = "embedded_r";

static bool RAPIEnabled(void) {
	return (GDKgetenv_istrue(rapi_enableflag)
			|| GDKgetenv_isyes(rapi_enableflag));
}

// The R-environment should be single threaded, calling for some protective measures.
static MT_Lock rapiLock = MT_LOCK_INITIALIZER(rapiLock);
static bool rapiInitialized = false;
#if 0
static char* rtypenames[] = { "NIL", "SYM", "LIST", "CLO", "ENV", "PROM",
		"LANG", "SPECIAL", "BUILTIN", "CHAR", "LGL", "unknown", "unknown",
		"INT", "REAL", "CPLX", "STR", "DOT", "ANY", "VEC", "EXPR", "BCODE",
		"EXTPTR", "WEAKREF", "RAW", "S4" };
#endif

static Client rapiClient = NULL;


#if 0
// helper function to translate R TYPEOF() return values to something readable
char* rtypename(int rtypeid) {
	if (rtypeid < 0 || rtypeid > 25) {
		return "unknown";
	} else
		return rtypenames[rtypeid];
}
#endif

static void writeConsoleEx(const char * buf, int buflen, int foo) {
	(void) buflen;
	(void) foo;
	(void) buf; // silence compiler
#ifdef _RAPI_DEBUG_
	printf("# %s", buf);
#endif
}

static void writeConsole(const char * buf, int buflen) {
	writeConsoleEx(buf, buflen, -42);
}

static void clearRErrConsole(void) {
	// Do nothing?
}

static char *RAPIinstalladdons(void);

/* UNIX-like initialization */
#ifndef WIN32

#define R_INTERFACE_PTRS 1
#define CSTACK_DEFNS 1
#include <Rinterface.h>

static char *RAPIinitialize(void) {
// TODO: check for header/library version mismatch?
	char *e;

	// set R_HOME for packages etc. We know this from our configure script
	putenv("R_HOME=" RHOME);

	// set some command line arguments
	{
		structRstart rp;
		char *rargv[] = { "R",
#if R_VERSION >= R_Version(4,0,0)
						  "--no-echo",
#else
						  "--slave",
#endif
						  "--vanilla" };
		int stat = 0;

		R_DefParams(&rp);
#if R_VERSION >= R_Version(4,0,0)
		rp.R_NoEcho = (Rboolean) TRUE;
#else
		rp.R_Slave = (Rboolean) TRUE;
#endif
		rp.R_Quiet = (Rboolean) TRUE;
		rp.R_Interactive = (Rboolean) FALSE;
		rp.R_Verbose = (Rboolean) FALSE;
		rp.LoadSiteFile = (Rboolean) FALSE;
		rp.LoadInitFile = (Rboolean) FALSE;
		rp.RestoreAction = SA_NORESTORE;
		rp.SaveAction = SA_NOSAVE;
		rp.NoRenviron = TRUE;
		stat = Rf_initialize_R(2, rargv);
		if (stat < 0) {
			return "Rf_initialize failed";
		}
		R_SetParams(&rp);
	}

	/* disable stack checking, because threads will throw it off */
	R_CStackLimit = (uintptr_t) -1;
	/* redirect input/output and set error handler */
	R_Outputfile = NULL;
	R_Consolefile = NULL;
	/* we do not want R to handle any signal, will interfere with monetdbd */
	R_SignalHandlers = 0;
	/* we want control R's output and input */
	ptr_R_WriteConsoleEx = writeConsoleEx;
	ptr_R_WriteConsole = writeConsole;
	ptr_R_ReadConsole = NULL;
	ptr_R_ClearerrConsole = clearRErrConsole;

	// big boy here
	setup_Rmainloop();

	if ((e = RAPIinstalladdons()) != 0) {
		return e;
	}
	// patch R internals to disallow quit and system. Setting them to NULL produces an error.
	SET_INTERNAL(install("quit"), R_NilValue);
	// install.packages() uses system2 to call gcc etc., so we cannot disable it (perhaps store the pointer somewhere just for that?)
	//SET_INTERNAL(install("system"), R_NilValue);

	rapiInitialized = true;
	return NULL;
}
#else

#define	S_IRWXU		0000700

static char *RAPIinitialize(void) {
	return "Sorry, no R API on Windows";
}

#endif


static char *RAPIinstalladdons(void) {
	int evalErr;
	ParseStatus status;
	char rlibs[FILENAME_MAX];
	char rapiinclude[BUFSIZ];
	SEXP librisexp;
	int len;

	// r library folder, create if not exists
	len = snprintf(rlibs, sizeof(rlibs), "%s%c%s", GDKgetenv("gdk_dbpath"), DIR_SEP, "rapi_packages");
	if (len == -1 || len >= FILENAME_MAX)
		return "cannot create rapi_packages directory because the path is too large";

	if (MT_mkdir(rlibs) != 0 && errno != EEXIST) {
		return "cannot create rapi_packages directory";
	}
#ifdef _RAPI_DEBUG_
	printf("# R libraries installed in %s\n",rlibs);
#endif

	PROTECT(librisexp = allocVector(STRSXP, 1));
	SET_STRING_ELT(librisexp, 0, mkChar(rlibs));
	Rf_defineVar(Rf_install(".rapi.libdir"), librisexp, R_GlobalEnv);
	UNPROTECT(1);

	// run rapi.R environment setup script
	{
		char *f = locate_file("rapi", ".R", 0);
		snprintf(rapiinclude, sizeof(rapiinclude), "source(\"%s\")", f);
		GDKfree(f);
	}
#if DIR_SEP != '/'
	{
		char *p;
		for (p = rapiinclude; *p; p++)
			if (*p == DIR_SEP)
				*p = '/';
	}
#endif
	R_tryEvalSilent(
		VECTOR_ELT(
			R_ParseVector(mkString(rapiinclude), 1, &status,
						  R_NilValue), 0), R_GlobalEnv, &evalErr);

	// of course the script may contain errors as well
	if (evalErr != FALSE) {
		return "failure running R setup script";
	}
	return NULL;
}

static str RAPIeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bit grouped) {
	sql_func * sqlfun = NULL;
	str exprStr = *getArgReference_str(stk, pci, pci->retc + 1);

	SEXP x, env, retval;
	SEXP varname = R_NilValue;
	SEXP varvalue = R_NilValue;
	ParseStatus status;
	int i = 0;
	char argbuf[64];
	char *argnames = NULL;
	size_t argnameslen;
	size_t pos;
	char* rcall = NULL;
	size_t rcalllen;
	int ret_cols = 0; /* int because pci->retc is int, too*/
	str *args;
	int evalErr;
	char *msg = MAL_SUCCEED;
	BAT *b;
	node * argnode;
	int seengrp = FALSE;

	rapiClient = cntxt;

	if (!RAPIEnabled()) {
		throw(MAL, "rapi.eval",
			  "Embedded R has not been enabled. Start server with --set %s=true",
			  rapi_enableflag);
	}
	if (!rapiInitialized) {
		throw(MAL, "rapi.eval",
			  "Embedded R initialization has failed");
	}

	if (!grouped) {
		sql_subfunc *sqlmorefun = (*(sql_subfunc**) getArgReference(stk, pci, pci->retc));
		if (sqlmorefun) sqlfun = (*(sql_subfunc**) getArgReference(stk, pci, pci->retc))->func;
	} else {
		sqlfun = *(sql_func**) getArgReference(stk, pci, pci->retc);
	}

	args = (str*) GDKzalloc(sizeof(str) * pci->argc);
	if (args == NULL) {
		throw(MAL, "rapi.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	// get the lock even before initialization of the R interpreter, as this can take a second and must be done only once.
	MT_lock_set(&rapiLock);

	env = PROTECT(eval(lang1(install("new.env")), R_GlobalEnv));
	assert(env != NULL);

	// first argument after the return contains the pointer to the sql_func structure
	// NEW macro temporarily renamed to MNEW to allow including sql_catalog.h

	if (sqlfun != NULL && sqlfun->ops->cnt > 0) {
		int carg = pci->retc + 2;
		argnode = sqlfun->ops->h;
		while (argnode) {
			char* argname = ((sql_arg*) argnode->data)->name;
			args[carg] = GDKstrdup(argname);
			carg++;
			argnode = argnode->next;
		}
	}
	// the first unknown argument is the group, we don't really care for the rest.
	argnameslen = 2;
	for (i = pci->retc + 2; i < pci->argc; i++) {
		if (args[i] == NULL) {
			if (!seengrp && grouped) {
				args[i] = GDKstrdup("aggr_group");
				seengrp = TRUE;
			} else {
				snprintf(argbuf, sizeof(argbuf), "arg%i", i - pci->retc - 1);
				args[i] = GDKstrdup(argbuf);
			}
		}
		argnameslen += strlen(args[i]) + 2; /* extra for ", " */
	}

	// install the MAL variables into the R environment
	// we can basically map values to int ("INTEGER") or double ("REAL")
	for (i = pci->retc + 2; i < pci->argc; i++) {
		int bat_type = getBatType(getArgType(mb,pci,i));
		// check for BAT or scalar first, keep code left
		if (!isaBatType(getArgType(mb,pci,i))) {
			b = COLnew(0, getArgType(mb, pci, i), 0, TRANSIENT);
			if (b == NULL) {
				msg = createException(MAL, "rapi.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto wrapup;
			}
			if ( getArgType(mb,pci,i) == TYPE_str) {
				if (BUNappend(b, *getArgReference_str(stk, pci, i), false) != GDK_SUCCEED) {
					BBPreclaim(b);
					b = NULL;
					msg = createException(MAL, "rapi.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto wrapup;
				}
			} else {
				if (BUNappend(b, getArgReference(stk, pci, i), false) != GDK_SUCCEED) {
					BBPreclaim(b);
					b = NULL;
					msg = createException(MAL, "rapi.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto wrapup;
				}
			}
		} else {
			b = BATdescriptor(*getArgReference_bat(stk, pci, i));
			if (b == NULL) {
				msg = createException(MAL, "rapi.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto wrapup;
			}
		}

		// check the BAT count, if it is bigger than RAPI_MAX_TUPLES, fail
		if (BATcount(b) > RAPI_MAX_TUPLES) {
			msg = createException(MAL, "rapi.eval",
								  "Got "BUNFMT" rows, but can only handle "LLFMT". Sorry.",
								  BATcount(b), (lng) RAPI_MAX_TUPLES);
			BBPunfix(b->batCacheid);
			goto wrapup;
		}
		varname = PROTECT(Rf_install(args[i]));
		varvalue = bat_to_sexp(b, bat_type);
		if (varvalue == NULL) {
			msg = createException(MAL, "rapi.eval", "unknown argument type ");
			goto wrapup;
		}
		BBPunfix(b->batCacheid);

		// install vector into R environment
		Rf_defineVar(varname, varvalue, env);
		UNPROTECT(2);
	}

	/* we are going to evaluate the user function within an anonymous function call:
	 * ret <- (function(arg1){return(arg1*2)})(42)
	 * the user code is put inside the {}, this keeps our environment clean (TM) and gives
	 * a clear path for return values, namely using the builtin return() function
	 * this is also compatible with PL/R
	 */
	pos = 0;
	argnames = malloc(argnameslen);
	if (argnames == NULL) {
		msg = createException(MAL, "rapi.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto wrapup;
	}
	argnames[0] = '\0';
	for (i = pci->retc + 2; i < pci->argc; i++) {
		pos += snprintf(argnames + pos, argnameslen - pos, "%s%s",
						args[i], i < pci->argc - 1 ? ", " : "");
	}
	rcalllen = 2 * pos + strlen(exprStr) + 100;
	rcall = malloc(rcalllen);
	if (rcall == NULL) {
		msg = createException(MAL, "rapi.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto wrapup;
	}
	snprintf(rcall, rcalllen,
			 "ret <- as.data.frame((function(%s){%s})(%s), nm=NA, stringsAsFactors=F)\n",
			 argnames, exprStr, argnames);
	free(argnames);
	argnames = NULL;
#ifdef _RAPI_DEBUG_
	printf("# R call %s\n",rcall);
#endif

	x = R_ParseVector(mkString(rcall), 1, &status, R_NilValue);

	if (LENGTH(x) != 1 || status != PARSE_OK) {
		msg = createException(MAL, "rapi.eval",
							  "Error parsing R expression '%s'. ", exprStr);
		goto wrapup;
	}

	retval = R_tryEval(VECTOR_ELT(x, 0), env, &evalErr);
	if (evalErr != FALSE) {
		char* errormsg = strdup(R_curErrorBuf());
		size_t c;
		if (errormsg == NULL) {
			msg = createException(MAL, "rapi.eval", "Error running R expression.");
			goto wrapup;
		}
		// remove newlines from error message so it fits into a MAPI error (lol)
		for (c = 0; c < strlen(errormsg); c++) {
			if (errormsg[c] == '\r' || errormsg[c] == '\n') {
				errormsg[c] = ' ';
			}
		}
		msg = createException(MAL, "rapi.eval",
							  "Error running R expression: %s", errormsg);
		free(errormsg);
		goto wrapup;
	}

	// ret should be a data frame with exactly as many columns as we need from retc
	ret_cols = LENGTH(retval);
	if (ret_cols != pci->retc) {
		msg = createException(MAL, "rapi.eval",
							  "Expected result of %d columns, got %d", pci->retc, ret_cols);
		goto wrapup;
	}

	// collect the return values
	for (i = 0; i < pci->retc; i++) {
		SEXP ret_col = VECTOR_ELT(retval, i);
		int bat_type = getBatType(getArgType(mb,pci,i));
		if (bat_type == TYPE_any || bat_type == TYPE_void) {
			getArgType(mb,pci,i) = bat_type;
			msg = createException(MAL, "rapi.eval",
								  "Unknown return value, possibly projecting with no parameters.");
			goto wrapup;
		}

		// hand over the vector into a BAT
		b = sexp_to_bat(ret_col, bat_type);
		if (b == NULL) {
			msg = createException(MAL, "rapi.eval",
								  "Failed to convert column %i", i);
			goto wrapup;
		}
		// bat return
		if (isaBatType(getArgType(mb,pci,i))) {
			*getArgReference_bat(stk, pci, i) = b->batCacheid;
		} else { // single value return, only for non-grouped aggregations
			BATiter li = bat_iterator(b);
			if (VALinit(&stk->stk[pci->argv[i]], bat_type,
						BUNtail(li, 0)) == NULL) { // TODO BUNtail here
				msg = createException(MAL, "rapi.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto wrapup;
			}
		}
		msg = MAL_SUCCEED;
	}
	/* unprotect environment, so it will be eaten by the GC. */
	UNPROTECT(1);
  wrapup:
	MT_lock_unset(&rapiLock);
	if (argnames)
		free(argnames);
	if (rcall)
		free(rcall);
	for (i = 0; i < pci->argc; i++)
		GDKfree(args[i]);
	GDKfree(args);

	return msg;
}

static str RAPIevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
							InstrPtr pci) {
	return RAPIeval(cntxt, mb, stk, pci, 0);
}
static str RAPIevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
							 InstrPtr pci) {
	return RAPIeval(cntxt, mb, stk, pci, 1);
}

/* used for loopback queries from R
 * see test rapi10 in monetdb5/extras/rapi */
extern
#ifdef WIN32
__declspec(dllexport)
#endif
void *RAPIloopback(void *query);

void *
RAPIloopback(void *query) {
	res_table* output = NULL;
	char* querystr = (char*)CHAR(STRING_ELT(query, 0));
	char* err = SQLstatementIntern(rapiClient, querystr, "name", 1, 0, &output);

	if (err) { // there was an error
		return ScalarString(RSTR(err));
	}
	if (output) {
		int ncols = output->nr_cols;
		if (ncols > 0) {
			int i;
			SEXP retlist, names, varvalue = R_NilValue;
			retlist = PROTECT(allocVector(VECSXP, ncols));
			names = PROTECT(NEW_STRING(ncols));
			for (i = 0; i < ncols; i++) {
				BAT *b = BATdescriptor(output->cols[i].b);
				if (b == NULL || !(varvalue = bat_to_sexp(b, TYPE_any))) {
					UNPROTECT(i + 3);
					if (b)
						BBPunfix(b->batCacheid);
					return ScalarString(RSTR("Conversion error"));
				}
				BBPunfix(b->batCacheid);
				SET_STRING_ELT(names, i, RSTR(output->cols[i].name));
				SET_VECTOR_ELT(retlist, i, varvalue);
			}
			res_table_destroy(output);
			SET_NAMES(retlist, names);
			UNPROTECT(ncols + 2);
			return retlist;
		}
		res_table_destroy(output);
	}
	return ScalarLogical(1);
}

static str RAPIprelude(void *ret) {
	(void) ret;

	if (RAPIEnabled()) {
		MT_lock_set(&rapiLock);
		/* startup internal R environment  */
		if (!rapiInitialized) {
			char *initstatus;
			initstatus = RAPIinitialize();
			if (initstatus != 0) {
				MT_lock_unset(&rapiLock);
				throw(MAL, "rapi.eval",
					  "failed to initialize R environment (%s)", initstatus);
			}
			Rf_defineVar(Rf_install("MONETDB_LIBDIR"), ScalarString(RSTR(LIBDIR)), R_GlobalEnv);

		}
		MT_lock_unset(&rapiLock);
		printf("# MonetDB/R   module loaded\n");
	}
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func rapi_init_funcs[] = {
 pattern("rapi", "eval", RAPIevalStd, false, "Execute a simple R script returning a single value", args(1,3, argany("",0),arg("fptr",ptr),arg("expr",str))),
 pattern("rapi", "eval", RAPIevalStd, false, "Execute a simple R script value", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 pattern("rapi", "subeval_aggr", RAPIevalAggr, false, "grouped aggregates through R", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 pattern("rapi", "eval_aggr", RAPIevalAggr, false, "grouped aggregates through R", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 command("rapi", "prelude", RAPIprelude, false, "", args(1,1, arg("",void))),
 pattern("batrapi", "eval", RAPIevalStd, false, "Execute a simple R script value", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 pattern("batrapi", "subeval_aggr", RAPIevalAggr, false, "grouped aggregates through R", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 pattern("batrapi", "eval_aggr", RAPIevalAggr, false, "grouped aggregates through R", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_rapi_mal)
{ mal_module("rapi", NULL, rapi_init_funcs); }

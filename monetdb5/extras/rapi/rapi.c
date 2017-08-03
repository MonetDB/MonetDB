/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
#include "mmath.h"
#include "sql_catalog.h"
#include "sql_execute.h"
#include "rapi.h"

// R headers
#define R_INTERFACE_PTRS 1
#define CSTACK_DEFNS 1

/* R redefines these */
#undef SIZEOF_SIZE_T
#undef ERROR

#define USE_RINTERNALS 1

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

/* we need the BAT-SEXP-BAT conversion in two places, here and in tools/embedded */
#include "converters.c.h"

const char* rapi_enableflag = "embedded_r";

int RAPIEnabled(void) {
	return (GDKgetenv_istrue(rapi_enableflag)
			|| GDKgetenv_isyes(rapi_enableflag));
}

// The R-environment should be single threaded, calling for some protective measures.
static MT_Lock rapiLock MT_LOCK_INITIALIZER("rapiLock");
static int rapiInitialized = FALSE;
static char* rtypenames[] = { "NIL", "SYM", "LIST", "CLO", "ENV", "PROM",
		"LANG", "SPECIAL", "BUILTIN", "CHAR", "LGL", "unknown", "unknown",
		"INT", "REAL", "CPLX", "STR", "DOT", "ANY", "VEC", "EXPR", "BCODE",
		"EXTPTR", "WEAKREF", "RAW", "S4" };

static Client rapiClient = NULL;


// helper function to translate R TYPEOF() return values to something readable
char* rtypename(int rtypeid) {
	if (rtypeid < 0 || rtypeid > 25) {
		return "unknown";
	} else
		return rtypenames[rtypeid];
}

void writeConsoleEx(const char * buf, int buflen, int foo) {
	(void) buflen;
	(void) foo;
	(void) buf; // silence compiler
#ifdef _RAPI_DEBUG_
	THRprintf(GDKout, "# %s", buf);
#endif
}

void writeConsole(const char * buf, int buflen) {
	writeConsoleEx(buf, buflen, -42);
}

void clearRErrConsole(void) {
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
	setenv("R_HOME", RHOME, TRUE);

	// set some command line arguments
	{
		structRstart rp;
		Rstart Rp = &rp;
		char *rargv[] = { "R", "--slave", "--vanilla" };
		int stat = 0;

		R_DefParams(Rp);
		Rp->R_Slave = (Rboolean) TRUE;
		Rp->R_Quiet = (Rboolean) TRUE;
		Rp->R_Interactive = (Rboolean) FALSE;
		Rp->R_Verbose = (Rboolean) FALSE;
		Rp->LoadSiteFile = (Rboolean) FALSE;
		Rp->LoadInitFile = (Rboolean) FALSE;
		Rp->RestoreAction = SA_NORESTORE;
		Rp->SaveAction = SA_NOSAVE;
		Rp->NoRenviron = TRUE;
		stat = Rf_initialize_R(2, rargv);
		if (stat < 0) {
			return "Rf_initialize failed";
		}
		R_SetParams(Rp);
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

	rapiInitialized++;
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
	char rlibs[BUFSIZ];
	char rapiinclude[BUFSIZ];
	SEXP librisexp;

	// r library folder, create if not exists
	snprintf(rlibs, sizeof(rlibs), "%s%c%s", GDKgetenv("gdk_dbpath"), DIR_SEP,
			 "rapi_packages");

	if (mkdir(rlibs, S_IRWXU) != 0 && errno != EEXIST) {
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

rapi_export str RAPIevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
							InstrPtr pci) {
	return RAPIeval(cntxt, mb, stk, pci, 0);
}
rapi_export str RAPIevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
							 InstrPtr pci) {
	return RAPIeval(cntxt, mb, stk, pci, 1);
}

str RAPIeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bit grouped) {
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

	if (!grouped) {
		sql_subfunc *sqlmorefun = (*(sql_subfunc**) getArgReference(stk, pci, pci->retc));
		if (sqlmorefun) sqlfun = (*(sql_subfunc**) getArgReference(stk, pci, pci->retc))->func;
	} else {
		sqlfun = *(sql_func**) getArgReference(stk, pci, pci->retc);
	}

	args = (str*) GDKzalloc(sizeof(str) * pci->argc);
	if (args == NULL) {
		throw(MAL, "rapi.eval", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		// check for BAT or scalar first, keep code left
		if (!isaBatType(getArgType(mb,pci,i))) {
			b = COLnew(0, getArgType(mb, pci, i), 0, TRANSIENT);
			if (b == NULL) {
				msg = createException(MAL, "rapi.eval", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				goto wrapup;
			}
			if ( getArgType(mb,pci,i) == TYPE_str) {
				if (BUNappend(b, *getArgReference_str(stk, pci, i), FALSE) != GDK_SUCCEED) {
					BBPreclaim(b);
					b = NULL;
					msg = createException(MAL, "rapi.eval", SQLSTATE(HY001) MAL_MALLOC_FAIL);
					goto wrapup;
				}
			} else {
				if (BUNappend(b, getArgReference(stk, pci, i), FALSE) != GDK_SUCCEED) {
					BBPreclaim(b);
					b = NULL;
					msg = createException(MAL, "rapi.eval", SQLSTATE(HY001) MAL_MALLOC_FAIL);
					goto wrapup;
				}
			}
			BATsetcount(b, 1);
			BATsettrivprop(b);
		} else {
			b = BATdescriptor(*getArgReference_bat(stk, pci, i));
			if (b == NULL) {
				msg = createException(MAL, "rapi.eval", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				goto wrapup;
			}
		}

		// check the BAT count, if it is bigger than RAPI_MAX_TUPLES, fail
		if (BATcount(b) > RAPI_MAX_TUPLES) {
			msg = createException(MAL, "rapi.eval",
								  "Got "BUNFMT" rows, but can only handle "LLFMT". Sorry.",
								  BATcount(b), (lng) RAPI_MAX_TUPLES);
			goto wrapup;
		}
		varname = PROTECT(Rf_install(args[i]));
		varvalue = bat_to_sexp(b);
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
		msg = createException(MAL, "rapi.eval", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		msg = createException(MAL, "rapi.eval", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
				msg = createException(MAL, "rapi.eval", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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

void* RAPIloopback(void *query) {
	res_table* output = NULL;
	char* querystr = (char*)CHAR(STRING_ELT(query, 0));
	char* err = SQLstatementIntern(rapiClient, &querystr, "name", 1, 0, &output);

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
				if (b == NULL || !(varvalue = bat_to_sexp(b))) {
					UNPROTECT(i + 3);
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


str RAPIprelude(void *ret) {
#ifdef NEED_MT_LOCK_INIT
	static int initialized = 0;
	/* since we don't destroy the lock, only initialize it once */
	if (!initialized)
		MT_lock_init(&rapiLock, "rapi_lock");
	initialized = 1;
#endif
	(void) ret;

	if (RAPIEnabled()) {
		MT_lock_set(&rapiLock);
		/* startup internal R environment  */
		if (!rapiInitialized) {
			char *initstatus;
			initstatus = RAPIinitialize();
			if (initstatus != 0) {
				throw(MAL, "rapi.eval",
					  "failed to initialise R environment (%s)", initstatus);
			}
			Rf_defineVar(Rf_install("MONETDB_LIBDIR"), ScalarString(RSTR(LIBDIR)), R_GlobalEnv);

		}
		MT_lock_unset(&rapiLock);
		printf("# MonetDB/R   module loaded\n");
	}
	return MAL_SUCCEED;
}

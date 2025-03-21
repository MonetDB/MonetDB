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

/*
 * (author) M. Kersten 2015
*/

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "mal_function.h"		/* for getPC() */
#include "mal_utils.h"
#include "mal_exception.h"
#include "mal_listing.h"

/*
 * Since MAL programs can be created on the fly by linked-in query
 * compilers, or transformed by optimizers, it becomes
 * mandatory to be able to produce textual correct MAL programs
 * from its internal representation for several purposes.
 *
 * Whenever there is an overriding property it is applied.
 *
 * The hiddenInstruction operator assumes a sufficiently large block
 * to leave information on the signature behind.
 *
 * The protection against overflow is not tight.
*/
#define advance(X,B,L)  while(*(X) && B+L>X)(X)++;

/* Copy string in src to *dstp which has *lenp space available and
 * terminate with a NULL byte.  *dstp and *lenp are adjusted for the
 * used space.  If there is not enough space to copy all of src,
 * return false, otherwise return true.  The resulting string is
 * always NULL-terminated. */
static inline bool
copystring(char **dstp, const char *src, size_t *lenp)
{
	size_t len = *lenp;
	char *dst = *dstp;

	if (src == NULL)
		return true;
	if (len > 0) {
		while (*src && len > 1) {
			*dst++ = *src++;
			len--;
		}
		*dst = 0;
		*dstp = dst;
		*lenp = len;
	}
	return *src == 0;
}

static void
renderTerm(MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int idx, int flg, char *buf, size_t max_len)
{
	char *bufend = buf;
	int nameused = 0;
	ValRecord *val = 0;
	char *cv = 0;
	str tpe;
	int showtype = 0, closequote = 0;
	int varid = getArg(p, idx);

	// show the name when required or is used
	if ((flg & LIST_MAL_NAME) && !isVarConstant(mb, varid)
		&& !isVarTypedef(mb, varid)) {
		(void) getVarNameIntoBuffer(mb, varid, bufend);
		bufend += strlen(bufend);
		nameused = 1;
	}
	// show the value when required or being a constant
	if (((flg & LIST_MAL_VALUE) && stk != 0) || isVarConstant(mb, varid)) {
		if (nameused)
			bufend = stpcpy(bufend, "=");
		// locate value record
		if (isVarConstant(mb, varid)) {
			val = &getVarConstant(mb, varid);
			showtype = getVarType(mb, varid) != TYPE_str
					&& getVarType(mb, varid) != TYPE_bit;
		} else if (stk) {
			val = &stk->stk[varid];
		}
		cv = VALformat(val);
		if (cv == NULL) {
			bufend = stpcpy(bufend, "<alloc failed...>");
		} else if (!val->bat && strcmp(cv, "nil") == 0) {
			bufend = stpcpy(bufend, cv);
			showtype = showtype ||
				(getBatType(getVarType(mb, varid)) >= TYPE_date
				 && getBatType(getVarType(mb, varid)) != TYPE_str) ||
				((isVarTypedef(mb, varid)) && isVarConstant(mb, varid)) ||
				isaBatType(getVarType(mb, varid));
		} else {
			if (!isaBatType(getVarType(mb, varid))
				&& getBatType(getVarType(mb, varid)) >= TYPE_date
				&& getBatType(getVarType(mb, varid)) != TYPE_str) {
				closequote = 1;
				bufend = stpcpy(bufend, "\"");
			}
			size_t cv_len = strlen(cv);
			if (cv_len > 100) {
				cv_len = 100;
				if (cv_len > (size_t) ((buf + max_len) - bufend))
					cv_len = (buf + max_len) - bufend - 1;
				strcpy_len(bufend, cv, cv_len + 1); /* 1 for null termination */
				bufend += cv_len;
				cv_len = strconcat_len(bufend, (buf + max_len) - bufend, "\" ..... ", NULL);
				bufend += cv_len;
			} else {
				bufend = stpcpy(bufend, cv);
			}
			if (closequote) {
				bufend = stpcpy(bufend, "\"");
			}
			showtype = showtype || closequote > TYPE_str ||
				((isVarTypedef(mb, varid) ||
				  (flg & (LIST_MAL_REMOTE | LIST_MAL_TYPE))) && isVarConstant(mb, varid)) ||
				(isaBatType(getVarType(mb, varid)) && idx < p->retc);

			if (stk && isaBatType(getVarType(mb, varid))
				&& stk->stk[varid].val.bval) {
				BAT *d = BBPquickdesc(stk->stk[varid].val.bval);
				if (d)
					bufend += snprintf(bufend, (buf + max_len) - bufend, "[" BUNFMT "]", BATcount(d));
			}
		}
		GDKfree(cv);
	}
	*bufend = 0;
	// show the type when required or frozen by the user
	// special care should be taken with constants, they may have been casted
	if ((flg & LIST_MAL_TYPE) || (idx < p->retc) || isVarTypedef(mb, varid)
		|| showtype) {
		tpe = getTypeName(getVarType(mb, varid));
		if (tpe) {
			strconcat_len(bufend, (buf + max_len) - bufend, ":", tpe, NULL);
			GDKfree(tpe);
		}
	}
}

/*
It receives the space to store the definition
The MAL profiler dumps some performance data at the
beginning of each line.
*/

str
cfcnDefinition(Symbol s, str base, size_t len)
{
	unsigned int i;
	str arg, tpe;
	mel_func *f = s->func;
	str t = base;

	if (f->unsafe && !copystring(&t, "unsafe ", &len))
		return base;
	if (!copystring(&t, operatorName(s->kind), &len) ||
		!copystring(&t, " ", &len) ||
		!copystring(&t, f->mod ? f->mod : userRef, &len) ||
		!copystring(&t, ".", &len) ||
		!copystring(&t, f->fcn, &len) || !copystring(&t, "(", &len))
		return base;

	char var[16];
	for (i = f->retc; i < f->argc; i++) {
		if (snprintf(var, 16, "X_%d:", i-f->retc) >= 16 || !copystring(&t, var, &len))
			return base;
		if ((f->args[i].isbat || (f->args[i].opt == 1)) && !copystring(&t, (f->args[i].opt == 1)?"bat?[:":"bat[:", &len))
			return base;
		arg = f->args[i].type;
		if (arg[0] && !copystring(&t, arg, &len))
			return base;
		if (!arg[0]) {
			if (f->args[i].nr) {
				if (snprintf(var, 16, "any_%d", f->args[i].nr ) >= 16 || !copystring(&t, var, &len))
					return base;
			} else if (!copystring(&t, "any", &len))
				return base;
		}
		if ((f->args[i].isbat || f->args[i].opt == 1) && !copystring(&t, "]", &len))
			return base;
		if (i+1 < f->argc && !copystring(&t, ", ", &len))
			return base;
	}

	advance(t, base, len);
	if (f->vargs && !copystring(&t, "...", &len))
		return base;

	if (f->retc == 0) {
		if (!copystring(&t, "):void", &len))
			return base;
	} else if (f->retc == 1) {
		if (!copystring(&t, "):", &len))
			return base;
		if ((f->args[0].isbat || f->args[0].opt == 1) && !copystring(&t, (f->args[0].opt == 1)?"bat?[:":"bat[:", &len))
			return base;
		tpe = f->args[0].type;
		if (tpe[0] && !copystring(&t, tpe, &len))
			return base;
		if (!tpe[0]) {
			if (f->args[0].nr) {
				if (snprintf(var, 16, "any_%d", f->args[0].nr ) >= 16 || !copystring(&t, var, &len))
					return base;
			} else if (!copystring(&t, "any", &len))
				return base;
		}
		if ((f->args[0].isbat || f->args[0].opt == 1) && !copystring(&t, "]", &len))
			return base;
		if (f->vrets && !copystring(&t, "...", &len))
			return base;
	} else {
		if (!copystring(&t, ") (", &len))
			return base;
		for (i = 0; i < f->retc; i++) {
			if (snprintf(var, 16, "X_%d:", i+(f->argc-f->retc)) >= 16 || !copystring(&t, var, &len))
				return base;
			if ((f->args[i].isbat || (f->args[i].opt == 1)) && !copystring(&t, (f->args[i].opt == 1)?"bat?[:":"bat[:", &len))
				return base;
			arg = f->args[i].type;
			if (arg[0] && !copystring(&t, arg, &len))
				return base;
			if (!arg[0]) {
				if (f->args[i].nr) {
					if (snprintf(var, 16, "any_%d", f->args[i].nr ) >= 16 || !copystring(&t, var, &len))
						return base;
				} else if (!copystring(&t, "any", &len))
				return base;
			}
			if ((f->args[i].isbat || f->args[i].opt == 1) && !copystring(&t, "]", &len))
				return base;
			if (i+1 < f->retc && !copystring(&t, ", ", &len))
				return base;
		}
		if (f->vrets && !copystring(&t, "...", &len))
			return base;
		if (!copystring(&t, ")", &len))
			return base;
	}

	return base;
}

str
fcnDefinition(MalBlkPtr mb, InstrPtr p, str t, int flg, str base, size_t len)
{
	int i, j;
	char arg[256];
	str tpe;

	len -= t - base;
	if (!flg && !copystring(&t, "#", &len))
		return base;
	if (mb->inlineProp && !copystring(&t, "inline ", &len))
		return base;
	if (mb->unsafeProp && !copystring(&t, "unsafe ", &len))
		return base;
	if (!copystring(&t, operatorName(p->token), &len) ||
		!copystring(&t, " ", &len) ||
		!copystring(&t, getModuleId(p) ? getModuleId(p) : userRef, &len) ||
		!copystring(&t, ".", &len) ||
		!copystring(&t, getFunctionId(p), &len) || !copystring(&t, "(", &len))
		return base;

	for (i = p->retc; i < p->argc; i++) {
		renderTerm(mb, 0, p, i,
				   (LIST_MAL_NAME | LIST_MAL_TYPE | LIST_MAL_PROPS),
				   arg, sizeof(arg));
		if (!copystring(&t, arg, &len))
			return base;
		if (i < p->argc - 1 && !copystring(&t, ", ", &len))
			return base;
	}

	advance(t, base, len);
	if (p->varargs & VARARGS && !copystring(&t, "...", &len))
		return base;

	if (p->retc == 1) {
		if (!copystring(&t, "):", &len))
			return base;
		tpe = getTypeName(getVarType(mb, getArg(p, 0)));
		if (!copystring(&t, tpe, &len)) {
			GDKfree(tpe);
			return base;
		}
		GDKfree(tpe);
		if (p->varargs & VARRETS && !copystring(&t, "...", &len))
			return base;
	} else {
		if (!copystring(&t, ") (", &len))
			return base;
		for (i = 0; i < p->retc; i++) {
			renderTerm(mb, 0, p, i,
					   (LIST_MAL_NAME | LIST_MAL_TYPE | LIST_MAL_PROPS),
					   arg, sizeof(arg));
			if (!copystring(&t, arg, &len))
				return base;
			if (i < p->retc - 1 && !copystring(&t, ", ", &len))
				return base;
		}
		if (p->varargs & VARRETS && !copystring(&t, "...", &len))
			return base;
		if (!copystring(&t, ")", &len))
			return base;
	}

	if ((flg & LIST_MAL_NOCFUNC) == 0) {
		if (mb->binding[0]) {
			if (!copystring(&t, " address ", &len) ||
				!copystring(&t, mb->binding, &len))
				return base;
		}
		(void) copystring(&t, ";", &len);
	}
	/* add the extra properties for debugging */
	if (flg & LIST_MAL_PROPS) {
		char extra[256];
		if (p->token == REMsymbol) {
		} else {
			snprintf(extra, 256, "\t#[%d] (" BUNFMT ") %s ", getPC(mb, p),
					 getRowCnt(mb, getArg(p, 0)),
					 (p->blk ? p->blk->binding : ""));
			if (!copystring(&t, extra, &len))
				return base;
			for (j = 0; j < p->retc; j++) {
				snprintf(extra, 256, "%d ", getArg(p, j));
				if (!copystring(&t, extra, &len))
					return base;
			}
			if (p->argc - p->retc > 0) {
				if (!copystring(&t, "<- ", &len))
					return base;
			}
			for (; j < p->argc; j++) {
				snprintf(extra, 256, "%d ", getArg(p, j));
				if (!copystring(&t, extra, &len))
					return base;
			}
			if (!p->typeresolved) {
				if (!copystring(&t, " type check needed", &len))
					return base;
			}
		}
	}
	return base;
}

static str
fmtRemark(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, str t, int flg, str base,
		  size_t len)
{
	char aux[128];

	if (!copystring(&t, "# ", &len))
		return base;

	if (pci->argc == 3) {
		if (getFunctionId(pci)) {
			char arg[256];
			renderTerm(mb, stk, pci, 1, flg, arg, sizeof(arg));
			int a1 = atoi(arg);
			renderTerm(mb, stk, pci, 2, flg, arg, sizeof(arg));
			long a2 = atol(arg);
			const char *f = getFunctionId(pci);
			if (strcmp(f, "total") == 0)
				snprintf(aux, 128, "%d optimizers %ld usecs", a1, a2);
			else
				snprintf(aux, 128, "%-36s %d actions %ld usecs", f, a1, a2);
			(void) copystring(&t, aux, &len);
		}
	} else if (pci->argc == 1) {
		if (getFunctionId(pci)) {
			if (!copystring(&t, getFunctionId(pci), &len))
				return base;
		}
	} else if (getVar(mb, getArg(pci, 0))->value.val.sval &&
			   getVar(mb, getArg(pci, 0))->value.len > 0 &&
			   !copystring(&t, getVar(mb, getArg(pci, 0))->value.val.sval,
						   &len))
		return base;

	return base;
}

str
operatorName(int i)
{
	switch (i) {
	case ASSIGNsymbol:
		return ":=";
	case BARRIERsymbol:
		return "barrier";
	case REDOsymbol:
		return "redo";
	case LEAVEsymbol:
		return "leave";
	case EXITsymbol:
		return "exit";
	case RETURNsymbol:
		return "return";
	case CATCHsymbol:
		return "catch";
	case RAISEsymbol:
		return "raise";
	case ENDsymbol:
		return "end";
	case FUNCTIONsymbol:
		return "function";
	case COMMANDsymbol:
		return "command";
	case PATTERNsymbol:
		return "pattern";

		/* internal symbols */
	case FCNcall:
		assert(0);
		return "FCNcall";
	case CMDcall:
		assert(0);
		return "CMDcall";
	case PATcall:
		assert(0);
		return "PATcall";
	}
	return "";
}

str
instruction2str(MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg)
{
	int i, j;
	str base, t;
	size_t len = 512 + (p->argc * 128);	/* max realistic line length estimate */

	t = base = GDKmalloc(len);
	if (base == NULL)
		return NULL;
	if (!flg) {
		*t++ = '#';
		len--;
		if (!p->typeresolved) {
			*t++ = '!';			/* error */
			len--;
		}
	}
	*t = 0;
	if (p->token == REMsymbol
		&& !(getModuleId(p) && strcmp(getModuleId(p), "querylog") == 0
			 && getFunctionId(p) && strcmp(getFunctionId(p), "define") == 0)) {
		/* do nothing */
	} else if (p->barrier) {
		if (p->barrier == LEAVEsymbol ||
			p->barrier == REDOsymbol ||
			p->barrier == RETURNsymbol || p->barrier == RAISEsymbol) {
			if (!copystring(&t, "    ", &len))
				return base;
		}
		if (!copystring(&t, operatorName(p->barrier), &len) || !copystring(&t, " ", &len))
			return base;
	} else if (functionStart(p) && flg != LIST_MAL_CALL) {
		return fcnDefinition(mb, p, t, flg, base, len + (t - base));
	} else if (!functionExit(p) && flg != LIST_MAL_CALL) {
		// beautify with tabs
		if (!copystring(&t, "    ", &len))
			return base;
	}
	switch (p->token < 0 ? -p->token : p->token) {
	case FCNcall:
	case PATcall:
	case CMDcall:
	case ASSIGNsymbol:
		// is any variable explicit or used
		/* this code was meant to make it easy to detect functions whose
		 * result variable was not used anywhere.
		 * It is not essential
		 for (i = 0; i < p->retc; i++)
		 if ( !isTmpVar(mb,getArg(p,i)) || isVarUsed(mb, getArg(p, i)))
		 break;

		 if (i == p->retc)
		 break;
		 */

		/* display multi-assignment list */
		if (p->retc > 1 && !copystring(&t, "(", &len))
			return base;

		for (i = 0; i < p->retc; i++) {
			char arg[256];
			renderTerm(mb, stk, p, i, flg, arg, sizeof(arg));
			if (!copystring(&t, arg, &len))
				return base;
			if (i < p->retc - 1 && !copystring(&t, ", ", &len))
				return base;
		}
		if (p->retc > 1 && !copystring(&t, ")", &len))
			return base;

		if (p->argc > p->retc || getFunctionId(p)) {
			if (!copystring(&t, " := ", &len))
				return base;
		}
		break;
	case ENDsymbol:
		if (!copystring(&t, "end ", &len) ||
			!copystring(&t, getModuleId(getInstrPtr(mb, 0)), &len) ||
			!copystring(&t, ".", &len) ||
			!copystring(&t, getFunctionId(getInstrPtr(mb, 0)), &len))
			return base;
		break;
	case COMMANDsymbol:
	case FUNCTIONsymbol:
	case PATTERNsymbol:
		if (flg & LIST_MAL_VALUE) {
			if (!copystring(&t, operatorName(p->token), &len) ||
				!copystring(&t, " ", &len))
				return base;
		}
		return fcnDefinition(mb, p, t, flg, base, len + (t - base));
	case REMsymbol:
		return fmtRemark(mb, stk, p, t, flg, base, len);
	default:
		i = snprintf(t, len, " unknown symbol ?%d? ", p->token);
		if (i < 0 || (size_t) i >= len)
			return base;
		len -= (size_t) i;
		t += i;
		break;
	}

	if (getModuleId(p)) {
		if (!copystring(&t, getModuleId(p), &len) || !copystring(&t, ".", &len))
			return base;
	}
	if (getFunctionId(p)) {
		if (!copystring(&t, getFunctionId(p), &len) ||
			!copystring(&t, "(", &len))
			return base;
	} else if (p->argc > p->retc + 1) {
		if (!copystring(&t, "(", &len))
			return base;
	}
	for (i = p->retc; i < p->argc; i++) {
		char arg[256];
		renderTerm(mb, stk, p, i, flg, arg, sizeof(arg));
		if (!copystring(&t, arg, &len))
				return base;
		if (i < p->argc - 1 && !copystring(&t, ", ", &len))
			return base;
	}
	if (getFunctionId(p) || p->argc > p->retc + 1) {
		if (!copystring(&t, ")", &len))
			return base;
	}
	if (p->token != REMsymbol) {
		if (!copystring(&t, ";", &len))
			return base;
	}
	/* add the extra properties for debugging */
	if (flg & LIST_MAL_PROPS) {
		char extra[256];
		if (p->token == REMsymbol) {
		} else {
			snprintf(extra, 256, "\t#[%d] (" BUNFMT ") %s ", p->pc,
					 getRowCnt(mb, getArg(p, 0)),
					 (p->blk ? p->blk->binding : ""));
			if (!copystring(&t, extra, &len))
				return base;
			for (j = 0; j < p->retc; j++) {
				snprintf(extra, 256, "%d ", getArg(p, j));
				if (!copystring(&t, extra, &len))
					return base;
			}
			if (p->argc - p->retc > 0) {
				if (!copystring(&t, "<- ", &len))
					return base;
			}
			for (; j < p->argc; j++) {
				snprintf(extra, 256, "%d ", getArg(p, j));
				if (!copystring(&t, extra, &len))
					return base;
			}
			if (!p->typeresolved) {
				if (!copystring(&t, " type check needed", &len))
					return base;
			}
		}
	}
	if (flg & LIST_MAL_ALGO) {
		const char *algo = MT_thread_getalgorithm();
		if (algo) {
			if (!copystring(&t, " # ", &len))
				return base;
			if (!copystring(&t, algo, &len))
				return base;
		}
	}
	return base;
}

/* the MAL beautifier is meant to simplify correlation of MAL variables and
 * the columns in the underlying database.
 * If the status is set, then we consider the instruction DONE and the result variables
 * should be shown as well.
 */

/* Remote execution of MAL calls for more type/property information to be exchanged */
str
mal2str(MalBlkPtr mb, int first, int last)
{
	str ps = NULL, *txt;
	int i, j;
	size_t *len, totlen = 0;

	txt = GDKmalloc(sizeof(str) * mb->stop);
	len = GDKmalloc(sizeof(size_t) * mb->stop);

	if (txt == NULL || len == NULL) {
		addMalException(mb, "mal2str: " MAL_MALLOC_FAIL);
		GDKfree(txt);
		GDKfree(len);
		return NULL;
	}
	for (i = first; i < last; i++) {
		if (i == 0)
			txt[i] = instruction2str(mb, 0, getInstrPtr(mb, i),
									 LIST_MAL_NAME | LIST_MAL_TYPE |
									 LIST_MAL_PROPS);
		else
			txt[i] = instruction2str(mb, 0, getInstrPtr(mb, i),
									 LIST_MAL_CALL | LIST_MAL_PROPS |
									 LIST_MAL_REMOTE);

		if (txt[i])
			totlen += len[i] = strlen(txt[i]);
		else {
			addMalException(mb, "mal2str: " MAL_MALLOC_FAIL);
			GDKfree(len);
			for (j = first; j < i; j++)
				GDKfree(txt[j]);
			GDKfree(txt);
			return NULL;
		}
	}
	ps = GDKmalloc(totlen + mb->stop + 1);
	if (ps == NULL) {
		addMalException(mb, "mal2str: " MAL_MALLOC_FAIL);
		GDKfree(len);
		for (i = first; i < last; i++)
			GDKfree(txt[i]);
		GDKfree(txt);
		return NULL;
	}

	totlen = 0;
	for (i = first; i < last; i++) {
		if (txt[i]) {
			strncpy(ps + totlen, txt[i], len[i]);
			ps[totlen + len[i]] = '\n';
			ps[totlen + len[i] + 1] = 0;
			totlen += len[i] + 1;
			GDKfree(txt[i]);
		}
	}
	GDKfree(len);
	GDKfree(txt);
	return ps;
}

void
printInstruction(stream *fd, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg)
{
	str ps;

	if (fd == 0)
		return;
	ps = instruction2str(mb, stk, p, flg);
	/* ps[strlen(ps)-1] = 0; remove '\n' */
	if (ps) {
		mnstr_printf(fd, "%s%s", (flg & LIST_MAL_MAPI ? "=" : ""), ps);
		GDKfree(ps);
	} else {
		mnstr_printf(fd, "#failed instruction2str()");
	}
	mnstr_printf(fd, "\n");
}

void
traceInstruction(MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg)
{
	str ps;
	TRC_DEBUG_IF(MAL_OPTIMIZER) {
		ps = instruction2str(mb, stk, p, flg);
		/* ps[strlen(ps)-1] = 0; remove '\n' */
		if (ps) {
			TRC_DEBUG_ENDIF(MAL_OPTIMIZER, "%s%s\n",
							(flg & LIST_MAL_MAPI ? "=" : ""), ps);
			GDKfree(ps);
		} else {
			TRC_DEBUG_ENDIF(MAL_OPTIMIZER, "Failed instruction2str()\n");
		}
	}
}

void
printSignature(stream *fd, Symbol s, int flg)
{
	InstrPtr p;
	str txt;

	if (s->def == 0) {
		mnstr_printf(fd, "missing definition of %s\n", s->name);
		return;
	}
	txt = GDKzalloc(MAXLISTING);	/* some slack for large blocks */
	if (txt) {
		p = getSignature(s);
		(void) fcnDefinition(s->def, p, txt, flg, txt, MAXLISTING);
		mnstr_printf(fd, "%s\n", txt);
		GDKfree(txt);
	} else
		mnstr_printf(fd, "printSignature: " MAL_MALLOC_FAIL);
}

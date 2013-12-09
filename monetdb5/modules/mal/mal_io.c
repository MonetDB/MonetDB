/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the MonetDB Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
*/
/*
 * author N.J. Nes, M.L. Kersten
 * 01/07/1996, 31/01/2002
 *
 * Input/Output module
 * The IO module provides simple @sc{ascii-io} rendering options.
 * It is modeled after the tuple formats, but does not
 * attempt to outline the results. Instead, it is geared at speed,
 * which also means that some functionality regarding the built-in
 * types is duplicated from the atoms definitions.
 *
 * A functional limited form of formatted printf is also provided.
 * It accepts at most one variable.
 * A more complete approach is the tablet module.
 *
 * The commands to load and save a BAT from/to an ASCII dump
 * are efficient, but work only for binary tables.
 */

/*
 * Printing
 * The print commands are implemented as single instruction rules,
 * because they need access to the calling context.
 * At a later stage we can look into the issues related to
 * parsing the format string as part of the initialization phase.
 * The old method in V4 essentially causes a lot of overhead
 * because you have to prepare for the worst (e.g. mismatch format
 * identifier and argument value)
 * Beware, the types of the objects to be printed should be
 * obtained from the stack, because the symbol table may actually
 * allow for any type to be assigned.
 */
#include "monetdb_config.h"
#include "mal_io.h"

#define MAXFORMAT 64*1024

str
io_stdin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bstream **ret= (bstream**) getArgReference(stk,pci,0);
	(void) mb;
	*ret = cntxt->fdin;
	return MAL_SUCCEED;
}

str
io_stdout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	stream **ret= (stream**) getArgReference(stk,pci,0);
	(void) mb;
	*ret = cntxt->fdout;
	return MAL_SUCCEED;
}

str
io_stderr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	stream **ret= (stream**) getArgReference(stk,pci,0);
	(void) cntxt;
	(void) mb;
	*ret = GDKerr;
	return MAL_SUCCEED;
}

str
IOprintBoth(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int indx, str hd, str tl, int nobat)
{
	int tpe = getArgType(mb, pci, indx);
	ptr val = (ptr) getArgReference(stk, pci, indx);
	stream *fp = cntxt->fdout;

	(void) mb;

	if (tpe == TYPE_any)
		tpe = stk->stk[pci->argv[indx]].vtype;
	if (val == NULL || tpe == TYPE_void) {
		if (hd)
			mnstr_printf(fp, "%s", hd);
		mnstr_printf(fp, "nil");
		if (tl)
			mnstr_printf(fp, "%s", tl);
		return MAL_SUCCEED;
	}
	if (isaBatType(tpe) ) {
		BAT *b;

		if (*(int *) val == 0) {
			if (hd)
				mnstr_printf(fp, "%s", hd);
			mnstr_printf(fp,"nil");
			if (tl)
				mnstr_printf(fp, "%s", tl);
			return MAL_SUCCEED;
		}
		b = BATdescriptor(*(int *) val);
		if (b == NULL) {
			throw(MAL, "io.print", RUNTIME_OBJECT_MISSING);
		}
		if (nobat) {
			if (hd)
				mnstr_printf(fp, "%s", hd);
			mnstr_printf(fp, "<%s>", BBPname(b->batCacheid));
			if (tl)
				mnstr_printf(fp, "%s", tl);
		} else
			BATmultiprintf(cntxt->fdout, 2, &b, TRUE, 0, TRUE);
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
	if (hd)
		mnstr_printf(fp, "%s", hd);

	if (ATOMvarsized(tpe))
		ATOMprint(tpe, *(str *) val, fp);
	else
		ATOMprint(tpe, val, fp);

	if (tl)
		mnstr_printf(fp, "%s", tl);
	return MAL_SUCCEED;
}

str
IOprint_val(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i;

	(void) cntxt;
	if (p->argc == 2)
		IOprintBoth(cntxt, mb, stk, p, 1, "[ ", " ]\n", 0);
	else {
		IOprintBoth(cntxt, mb, stk, p, 1, "[ ", 0, 1);
		for (i = 2; i < p->argc - 1; i++)
			IOprintBoth(cntxt,mb, stk, p, i, ", ", 0, 1);
		IOprintBoth(cntxt,mb, stk, p, i, ", ", "]\n", 1);
	}
	return MAL_SUCCEED;

}

str
IOprint_tables(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	IOtableAll(cntxt->fdout, cntxt, mb, stk, p, 1, 0, FALSE, TRUE);
	return MAL_SUCCEED;
}

str
IOprompt_val(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return IOprintBoth(cntxt, mb, stk, pci, 1, 0, 0, 1);
}

/*
 * The IOprintf_() gets a format str, and a sequence of (ptr,int) parameters
 * containing values and their type numbers. The printf() proved to be a
 * great risk; people formatting badly their "%s" format strings were crashing
 * the kernel. This function will prevent you from doing so.
 *
 * New implementation that repeatedly invokes sprintf => hacking the va_alist
 * for using vfsprintf proved to be too compiler-dependent (OLD approach).
 */
#define writemem(X1)\
	if (dst+X1 > buf+size) {\
		ptrdiff_t offset = dst - buf;\
		do {\
			size *= 2;\
		} while (dst+X1 > buf+size);\
		buf = GDKrealloc(buf, size);\
		dst = buf + offset;\
	}

#define m5sprintf(X1)\
	if (width > adds) {\
		str newadd;\
		newadd = GDKrealloc(add, width + 10);\
		if (newadd != NULL) {\
			adds = width + 10;\
			add = newadd;\
		}\
	}\
	n = snprintf(add, adds, meta, X1);\
	while (n < 0 || (size_t) n >= adds) {\
		size_t newadds;\
		str newadd;\
\
		if (n >= 0)     /* glibc 2.1 */\
			newadds = n + 1;   /* precisely what is needed */\
		else            /* glibc 2.0 */\
			newadds = n * 2;     /* twice the old size */\
\
		newadd = GDKrealloc(add, newadds);\
		if (newadd == NULL)\
			break;\
\
		adds = newadds;\
		add = newadd;\
		n = snprintf(add, adds, meta, X1);\
	}


static char toofew_error[80] = OPERATION_FAILED " At least %d parameter(s) expected.\n";
static char format_error[80] = OPERATION_FAILED " Error in format before param %d.\n";
static char type_error[80] = OPERATION_FAILED " Illegal type in param %d.\n";

#define return_error(x)\
	GDKfree(buf); GDKfree(add); throw(MAL,"io.printf", x,argc);

static char niltext[4] = "nil";

static str
IOprintf_(str *res, str format, ...)
{
	va_list ap;
	int n;

	int prec = 0, dotseen = 0, escaped = 0, type, size, argc = 1;
	size_t adds = 100, width = 0;
	char *add, *dst, *buf, *cur, *paramseen = NULL;
	char *p;

	if (format == NULL) {
		throw(MAL,"io.printf", ILLEGAL_ARGUMENT " NULL pointer passed as format.\n");
	} else if (strchr(format, '%') == NULL) {
		*res = GDKstrdup(format);
		return MAL_SUCCEED;
	}
	buf = dst = (str) GDKmalloc(size = 80);
	if ( buf == NULL)
		throw(MAL,"io.printf",MAL_MALLOC_FAIL);
	*res = NULL;

	add = GDKmalloc(adds);
	if (add == NULL) {
		GDKfree(buf);
		throw(MAL,"io.printf",MAL_MALLOC_FAIL);
	}

	va_start(ap,format);
	for (cur = format; *cur; cur++) {
		if (paramseen) {
			char meta[100];
			ptrdiff_t extra = 0;
			ptrdiff_t len;

			if (GDKisdigit(*cur)) {
				if (dotseen) {
					prec = 10 * prec + (*cur - '0');
				} else {
					width = 10 * width + (*cur - '0');
				}
				continue;
			} else if (dotseen == 0 && *cur == '.') {
				dotseen = 1;
				continue;
			} else if (cur == paramseen + 1 && (*cur == '+' || *cur == '-' || *cur == ' ')) {
				continue;
			} else if (*cur == 'l') {
				cur++;
				if (*cur == 'l') {
					cur++;
					/* start of ll */
					extra = (cur - paramseen) - 2;
				}
			}
			if ((p = va_arg(ap, char *)) == NULL) {
				va_end(ap);
				return_error(toofew_error);
			}
			type = ATOMstorage(va_arg(ap, int));

			len = 1 + (cur - paramseen);
			memcpy(meta, paramseen, len);
			meta[len] = 0;
			if (ATOMcmp(type, ATOMnilptr(type), p) == 0) {
				/* value is nil; attempt to print formatted 'nil'
				   without generating %ls etc. */
				char *csrc, *ctrg = meta;

				for (csrc = paramseen; csrc < cur; csrc++) {
					if (*csrc == '.')
						break;
					if (GDKisdigit(*csrc) || *csrc == '-')
						*(++ctrg) = *csrc;
				}
				*(++ctrg) = 's';
				*(++ctrg) = 0;
				m5sprintf(niltext);
			} else if (strchr("cdiouxX", *cur) && !extra) {
				int ival;

				if (dotseen) {
					va_end(ap);
					return_error(format_error);
				} else if (type == TYPE_bte) {
					ival = (int) *(bte *) p;
				} else if (type == TYPE_sht) {
					ival = (int) *(sht *) p;
				} else if (type == TYPE_flt) {
					ival = (int) *(flt *) p;
				} else if (type == TYPE_wrd) {
					goto largetypes;
				} else if (type == TYPE_lng) {
					goto largetypes;
				} else if (type == TYPE_int) {
					ival = *(int *) p;
				} else {
					va_end(ap);
					return_error(type_error);
				}
				m5sprintf(ival);
			} else if (strchr("diouxX", *cur)) {
#ifdef NATIVE_WIN32
				ptrdiff_t i;
#endif
				lng lval;

				if (dotseen) {
					va_end(ap);
					return_error(format_error);
				}
			largetypes:
				if (type == TYPE_bte) {
					lval = (lng) *(bte *) p;
				} else if (type == TYPE_sht) {
					lval = (lng) *(sht *) p;
				} else if (type == TYPE_int) {
					lval = (lng) *(int *) p;
				} else if (type == TYPE_wrd) {
					lval = (lng) *(wrd *) p;
				} else if (type == TYPE_flt) {
					lval = (lng) *(flt *) p;
				} else if (type == TYPE_dbl) {
					lval = (lng) *(dbl *) p;
				} else if (type == TYPE_lng) {
					lval = *(lng *) p;
				} else {
					va_end(ap);
					return_error(type_error);
				}
				if (!extra) {
					meta[len + 2] = meta[len];
					meta[len + 1] = meta[len - 1];
					meta[len] = 'l';
					meta[len - 1] = 'l';
					len += 2;
					extra = len - 3;
				}
#ifdef NATIVE_WIN32
				for (i = len; i >= (extra + 2); i--) {
					meta[i + 1] = meta[i];
				}
				meta[extra] = 'I';
				meta[extra + 1] = '6';
				meta[extra + 2] = '4';
#endif
				m5sprintf(lval);
			} else if (strchr("feEgG", *cur)) {
				dbl dval;

				if (type == TYPE_flt) {
					dval = (dbl) *(flt *) p;
				} else if (type == TYPE_dbl) {
					dval = *(dbl *) p;
				} else {
					va_end(ap);
					return_error(type_error);
				}
				width += (1 + prec);
				m5sprintf(dval);
			} else if (*cur == 's') {
				int length;

				if (extra) {
					va_end(ap);
					return_error(format_error);
				} else if (type != TYPE_str) {
					va_end(ap);
					return_error(type_error);
				}
				length = strLen(p);
				width++;
				prec++;	/* account for '\0' */
				if (dotseen && prec < length)
					length = prec;
				if ((size_t) length > width)
					width = (size_t) length;
				m5sprintf(p);
			} else {
				va_end(ap);
				return_error(format_error);
			}
			width = strlen(add);
			writemem(width);
			memcpy(dst, add, width);
			dst += width;
			paramseen = NULL;
			argc++;
		} else if (!escaped) {
			if (*cur == '\\' || (*cur == '%' && cur[1] == '%')) {
				escaped = 1;
			} else if (*cur == '%') {
				paramseen = cur;
				dotseen = prec = 0;
				width = 0;
			} else {
				writemem(1);
				*dst++ = *cur;
			}
		} else {
			escaped = 0;
			writemem(1);
			*dst++ = *cur;
		}
	}

/*
	if ( va_arg(ap, char *) != NULL){
		GDKfree(buf); 
		throw(MAL,"io.printf", "params %d and beyond ignored %s.\n",argc);
	}
*/
	va_end(ap);

	writemem(1);
	*dst = 0;
	*res = buf;
	GDKfree(add);
	return MAL_SUCCEED;
}

static ptr
getArgValue(MalStkPtr stk, InstrPtr pci, int k){
	int j=0;
	ValRecord *v;
	ptr val = NULL;
	int tpe ;

	j = pci->argv[k];
	v= &stk->stk[j];
	tpe = v->vtype;
tstagain:
	switch(tpe){
	/* switch(ATOMstorage(v->vtype)) */
	case TYPE_void: val= (ptr) & v->val.ival; break;
	case TYPE_bit: val= (ptr) & v->val.btval; break;
	case TYPE_sht: val= (ptr) & v->val.shval; break;
	case TYPE_bat: val= (ptr) & v->val.bval; break;
	case TYPE_int: val= (ptr) & v->val.ival; break;
	case TYPE_wrd: val= (ptr) & v->val.wval; break;
	case TYPE_bte: val= (ptr) & v->val.btval; break;
	case TYPE_oid: val= (ptr) & v->val.oval; break;
	case TYPE_ptr: val= (ptr) v->val.pval; break;/*!!*/
	case TYPE_flt: val= (ptr) & v->val.fval; break;
	case TYPE_dbl: val= (ptr) & v->val.dval; break;
	case TYPE_lng: val= (ptr) & v->val.lval; break;
	case TYPE_str: val= (ptr) v->val.sval; break;/*!!*/
	default:
		tpe= ATOMstorage(tpe);
		goto tstagain;
	}
	return val;
} 
#define G(X) , getArgValue(stk,pci,X), getArgType(mb,pci,X)

str
IOprintf(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *fmt = (str*) getArgReference(stk,pci,1);
	str fmt2 = NULL;
	str msg= MAL_SUCCEED;

	(void) cntxt;
	(void) mb;
	switch( pci->argc){
	case 2: msg= IOprintf_(&fmt2,*fmt );
			break;
	case 3: msg= IOprintf_(&fmt2,*fmt G(2));
		break;
	case 4: msg= IOprintf_(&fmt2,*fmt G(2) G(3));
		break;
	case 5: msg= IOprintf_(&fmt2,*fmt G(2) G(3) G(4));
		break;
	case 6: msg= IOprintf_(&fmt2,*fmt G(2) G(3) G(4) G(5));
		break;
	case 7: msg= IOprintf_(&fmt2,*fmt G(2) G(3) G(4) G(5) G(6));
		break;
	case 8: msg= IOprintf_(&fmt2,*fmt G(2) G(3) G(4) G(5) G(6) G(7));
		break;
	case 9: msg= IOprintf_(&fmt2,*fmt G(2) G(3) G(4) G(5) G(6) G(7) G(8));
		break;
	case 10: msg= IOprintf_(&fmt2,*fmt G(2) G(3) G(4) G(5) G(6) G(7) G(8) G(9));
	}
	if (msg== MAL_SUCCEED) {
		mnstr_printf(cntxt->fdout,"%s",fmt2);
		GDKfree(fmt2);
	}
	return msg;
}
str
IOprintfStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	str *fmt = (str*) getArgReference(stk,pci,2);
	str fmt2 = NULL;
	stream *f= (stream*) getArgReference(stk,pci,1);
	str msg= MAL_SUCCEED;

	(void) cntxt;
	(void) mb;
	switch( pci->argc){
	case 3: msg= IOprintf_(&fmt2,*fmt);
		break;
	case 4: msg= IOprintf_(&fmt2,*fmt G(3));
		break;
	case 5: msg= IOprintf_(&fmt2,*fmt G(3) G(4));
		break;
	case 6: msg= IOprintf_(&fmt2,*fmt G(3) G(4) G(5));
		break;
	case 7: msg= IOprintf_(&fmt2,*fmt G(3) G(4) G(5) G(6));
		break;
	case 8: msg= IOprintf_(&fmt2,*fmt G(3) G(4) G(5) G(6) G(7));
		break;
	case 9: msg= IOprintf_(&fmt2,*fmt G(3) G(4) G(5) G(6) G(7) G(8));
		break;
	case 10: msg= IOprintf_(&fmt2,*fmt G(3) G(4) G(5) G(6) G(7) G(8) G(9));
		break;
	case 11: msg= IOprintf_(&fmt2,*fmt G(3) G(4) G(5) G(6) G(7) G(8) G(9) G(10));
	}
	if (msg== MAL_SUCCEED){
		mnstr_printf(f,"%s",fmt2);
		GDKfree(fmt2);
	}
	return msg;
}

/*
 * The table printing routine implementations rely on the multiprintf.
 * They merely differ in destination and order prerequisite
 */
str
IOtableAll(stream *f, Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int i, int order, int printhead, int printorder)
{
	BAT *piv[MAXPARAMS], *b;
	int nbats = 0;
	int tpe, k = i;
	ptr val;

	(void) cntxt;
	for (; i < pci->argc; i++) {
		tpe = getArgType(mb, pci, i);
		val = (ptr) getArgReference(stk, pci, i);
		if (!isaBatType(tpe)) {
			for (k = 0; k < nbats; k++)
				BBPunfix(piv[k]->batCacheid);
			throw(MAL, "io.table", ILLEGAL_ARGUMENT " BAT expected");
		}
		b = BATdescriptor(*(int *) val);
		if (b == NULL) {
			for (k = 0; k < nbats; k++)
				BBPunfix(piv[k]->batCacheid);
			throw(MAL, "io.table", ILLEGAL_ARGUMENT " null BAT encountered");
		}
		piv[nbats++] = b;
	}
	/*if(printhead) */ nbats++;
	BATmultiprintf(f, nbats, piv, printhead, order, printorder);
	for (k = 0; k < nbats - 1; k++)
		BBPunfix(piv[k]->batCacheid);
	return MAL_SUCCEED;
}

str
IOotable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int order;
	order = *(int *) getArgReference(stk, pci, 1);
	return IOtableAll(cntxt->fdout, cntxt, mb, stk, pci, 2, order, TRUE, TRUE);
}

str
IOtable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return IOtableAll(cntxt->fdout, cntxt, mb, stk, pci, 1, 0, TRUE, TRUE);
}

str
IOfotable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	stream *fp;
	int order;

	fp = *(stream **) getArgReference(stk, pci, 1);
	order = *(int *) getArgReference(stk, pci, 2);
	(void) order;		/* fool compiler */
	return IOtableAll(fp, cntxt, mb, stk, pci, 3, 1, TRUE, TRUE);
}

str
IOftable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	stream *fp;

	fp = *(stream **) getArgReference(stk, pci, 1);
	return IOtableAll(fp, cntxt, mb, stk, pci, 2, 0, TRUE, TRUE);
}

str
IOttable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return IOtableAll(cntxt->fdout, cntxt, mb, stk, pci, 1, 0, FALSE, TRUE);
}

str
IOtotable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int order;
	order = *(int *) getArgReference(stk, pci, 1);
	return IOtableAll(cntxt->fdout, cntxt, mb, stk, pci, 2, order, FALSE, TRUE);
}

/*
 * Bulk export/loading
 * To simplify conversion between versions and to interface with other
 * applications, we use a simple import/export operation.
 *
 * The conversion routine assumes space in the buffer for storing the result.
 */
/*
 * A BAT can be saved in Monet format using the export command.
 * It is of particular use in preparing an ASCII version for migration.
 * The exported file is saved in the context of the directory
 * where the server was started unless an absolute file name was
 * presented.
 */
str
IOdatafile(str *ret, str *fnme){
	stream *s = open_rstream(*fnme);
	*ret = 0;
	if (s == NULL )
		throw(MAL, "io.export", RUNTIME_FILE_NOT_FOUND ":%s", *fnme);
	
	if (mnstr_errnr(s)) {
		mnstr_close(s);
		throw(MAL, "io.export", RUNTIME_FILE_NOT_FOUND ":%s", *fnme);
	}
	*ret= GDKstrdup(*fnme);
	mnstr_close(s);
	mnstr_destroy(s);
	return MAL_SUCCEED;
}

str
IOexport(bit *ret, int *bid, str *fnme)
{
	BAT *b;
	stream *s;

	*ret = FALSE;
	if ((b = BATdescriptor(*bid)) == NULL) 
		throw(MAL, "io.export", RUNTIME_OBJECT_MISSING);
	
	s = open_wastream(*fnme);
	if (s == NULL ){
		BBPunfix(b->batCacheid);
		throw(MAL, "io.export", RUNTIME_FILE_NOT_FOUND ":%s", *fnme);
	}
	if (mnstr_errnr(s)) {
		mnstr_close(s);
		BBPunfix(b->batCacheid);
		throw(MAL, "io.export", RUNTIME_FILE_NOT_FOUND ":%s", *fnme);
	}
	BATprintf(s, b);
	mnstr_close(s);
	mnstr_destroy(s);
	*ret = TRUE;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * The import command reads a single BAT from an ASCII file. It assumes
 * a layout compatible with that produced by print or export.
 */
#define COMMA ','
str
IOimport(int *ret, int *bid, str *fnme)
{
	BAT *b;
	int (*hconvert) (const char *, int *, ptr *);
	int (*tconvert) (const char *, int *, ptr *);
	int n;
	size_t bufsize = 2048;	/* NIELS:tmp change used to be 1024 */
	char *base, *cur, *end;
	char *buf;
	ptr h = 0, t = 0;
	int lh = 0, lt = 0;
	FILE *fp = fopen(*fnme, "r");
	char msg[BUFSIZ];

	if ((b = BATdescriptor(*bid)) == NULL) {
		if (fp)
			fclose(fp);
		throw(MAL, "io.import", RUNTIME_OBJECT_MISSING);
	}

	hconvert = BATatoms[BAThtype(b)].atomFromStr;
	tconvert = BATatoms[BATttype(b)].atomFromStr;
	/*
	 * Open the file. Memory map it to minimize buffering problems.
	 */
	if (fp == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "io.import", RUNTIME_FILE_NOT_FOUND ":%s", *fnme);
	} else {
		int fn;
		struct stat st;

		buf = (char *) GDKmalloc(bufsize);
		if ( buf == NULL) {
			BBPunfix(b->batCacheid);
			fclose(fp);
			throw(MAL,"io.import",MAL_MALLOC_FAIL);
		}

		if ((fn = fileno(fp)) <= 0) {
			BBPunfix(b->batCacheid);
			fclose(fp);
			throw(MAL, "io.import", OPERATION_FAILED " fileno()");
		}
		if (fstat(fn, &st) != 0) {
			BBPunfix(b->batCacheid);
			fclose(fp);
			throw(MAL, "io.imports", OPERATION_FAILED "fstat()");
		}

		(void) fclose(fp);
		if (st.st_size <= 0) {
			BBPunfix(b->batCacheid);
			throw(MAL, "io.imports", OPERATION_FAILED "Empty file or fstat broken");
		}
#if SIZEOF_SIZE_T == SIZEOF_INT
		if (st.st_size > ~ (size_t) 0) {
			BBPunfix(b->batCacheid);
			throw(MAL, "io.imports", OPERATION_FAILED "File too large");
		}
#endif
		base = cur = (char *) MT_mmap(*fnme, MMAP_SEQUENTIAL, (size_t) st.st_size);
		if (cur == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "io.mport", OPERATION_FAILED "MT_mmap()");
		}
		end = cur + st.st_size;

	}
	/* Parse a line. Copy it into a buffer. Concat broken lines with a slash.  */
	while (cur < end) {
		str dst = buf, src = cur, p;
		size_t l;

		/* like p = strchr(cur, '\n') but with extra bounds check */
		for (p = cur; p < end && *p != '\n'; p++)
			;
		l = p - cur;

		if (p < end) {
			while (src[l - 1] == '\\') {
				if (buf+bufsize < dst+l) {
					size_t len = dst - buf;
					size_t inc = (size_t) ((dst+l) - buf);
					buf = (char*) GDKrealloc((void*) buf, bufsize = MAX(inc,bufsize)*2);
					dst = buf + len;
				}
				memcpy(dst, src, l-1);
				dst += l - 1;
				src += l + 1;
				for (p = src; p < end && *p != '\n'; p++)
					;
				if (p == end)
					break;
				l = p - src;
			}
		}

		if (buf+bufsize < dst+l) {
			size_t len = dst - buf;
			size_t inc = (size_t) ((dst+l) - buf);
			buf = (char*) GDKrealloc((void*) buf, bufsize = MAX(inc,bufsize)*2);
			dst = buf + len;
		}
		memcpy(dst, src, l);
		dst[l] = 0;
		cur = p + 1;
		/* Parse the line, and insert a BUN.  */
		for (p = buf; *p && GDKisspace(*p); p++)
			;
		if (*p == '#')
			continue;

		for (;*p && *p != '['; p++)
			;
		if (*p)
			for (p++; *p && GDKisspace(*p); p++)
				;
		if (*p == 0) {
			BBPunfix(b->batCacheid);
			snprintf(msg,sizeof(msg),"error in input %s",buf);
			throw(MAL, "io.import", "%s", msg);
		}
		n = hconvert(p, &lh, (ptr*)&h);
		if (n <= 0) {
			BBPunfix(b->batCacheid);
			snprintf(msg,sizeof(msg),"error in input %s",buf);
			throw(MAL, "io.import", "%s", msg);
		}
		p += n;

		for (;*p && *p != COMMA; p++)
			;
		if (*p)
			for (p++; *p && GDKisspace(*p); p++)
				;
		if (*p == 0) {
			BBPunfix(b->batCacheid);
			snprintf(msg,sizeof(msg),"error in input %s",buf);
			throw(MAL, "io.import", "%s", msg);
		}
		n = tconvert(p, &lt, (ptr*)&t);
		if (n <= 0) {
			BBPunfix(b->batCacheid);
			snprintf(msg,sizeof(msg),"error in input %s",buf);
			throw(MAL, "io.import", "%s", msg);
		}
		p += n;
		if (BUNins(b, h, t, FALSE) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "io.import", "insert failed");
		}

/*
 * Unmap already parsed memory, to keep the memory usage low.
 */
#ifndef WIN32
#define MAXBUF 40*MT_pagesize()
		if ((unsigned) (cur - base) > MAXBUF) {
			MT_munmap(base, MAXBUF);
			base += MAXBUF;
		}
#endif
	}
	/* Cleanup and exit. Return the filled BAT.  */
	if (h)
		GDKfree(h);
	if (t)
		GDKfree(t);
	GDKfree(buf);
	MT_munmap(base, end - base);
	BBPkeepref(*ret= b->batCacheid);
	return MAL_SUCCEED;
}


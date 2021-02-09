/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "mal.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "mutils.h"

#define MAXFORMAT 64*1024

static str
io_stdin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bstream **ret= (bstream**) getArgReference(stk,pci,0);
	(void) mb;
	if( cntxt->fdin == NULL)
		throw(MAL, "io.print", SQLSTATE(HY002) "Input channel missing");
	*ret = cntxt->fdin;
	return MAL_SUCCEED;
}

static str
io_stdout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	stream **ret= (stream**) getArgReference(stk,pci,0);
	(void) mb;
	if( cntxt->fdout == NULL)
		throw(MAL, "io.print", SQLSTATE(HY002) "Output channel missing");
	*ret = cntxt->fdout;
	return MAL_SUCCEED;
}

static str
IOprintBoth(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int indx, str hd, str tl, int nobat)
{
	int tpe = getArgType(mb, pci, indx);
	ptr val = getArgReference(stk, pci, indx);
	stream *fp = cntxt->fdout;

	(void) mb;
	if( cntxt->fdout == NULL)
		throw(MAL, "io.print", SQLSTATE(HY002) "Output channel missing");

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

		if (is_bat_nil(*(bat *) val)) {
			if (hd)
				mnstr_printf(fp, "%s", hd);
			mnstr_printf(fp,"nil");
			if (tl)
				mnstr_printf(fp, "%s", tl);
			return MAL_SUCCEED;
		}
		b = BATdescriptor(*(bat *) val);
		if (b == NULL) {
			throw(MAL, "io.print", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (nobat) {
			if (hd)
				mnstr_printf(fp, "%s", hd);
			mnstr_printf(fp, "<%s>", BBPname(b->batCacheid));
			if (tl)
				mnstr_printf(fp, "%s", tl);
		} else {
			BATprint(cntxt->fdout, b);
		}
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

static str
IOprint_val(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i;
	str msg;

	(void) cntxt;
	if (p->argc == 2)
		msg = IOprintBoth(cntxt, mb, stk, p, 1, "[ ", " ]\n", 0);
	else {
		msg = IOprintBoth(cntxt, mb, stk, p, 1, "[ ", 0, 1);
		if (msg)
			return msg;
		for (i = 2; i < p->argc - 1; i++)
			if ((msg = IOprintBoth(cntxt,mb, stk, p, i, ", ", 0, 1)) != NULL)
				return msg;
		msg = IOprintBoth(cntxt,mb, stk, p, i, ", ", "]\n", 1);
	}
	return msg;

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
#define writemem(X1)													\
	do {																\
		if (dst+X1 > buf+size) {										\
			ptrdiff_t offset = dst - buf;								\
			char *tmp;													\
			do {														\
				size *= 2;												\
			} while (dst+X1 > buf+size);								\
			tmp = GDKrealloc(buf, size);								\
			if (tmp == NULL) {											\
				va_end(ap);												\
				GDKfree(buf);											\
				GDKfree(add);											\
				throw(MAL, "io.printf", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			}															\
			buf = tmp;													\
			dst = buf + offset;											\
		}																\
	} while (0)

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

#define return_error(x)							\
	do {										\
		GDKfree(buf);							\
		GDKfree(add);							\
		throw(MAL,"io.printf", x,argc);			\
	} while (0)

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
		if (*res == NULL)
			throw(MAL,"io.printf", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	buf = dst = (str) GDKmalloc(size = 80);
	if ( buf == NULL)
		throw(MAL,"io.printf", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*res = NULL;

	add = GDKmalloc(adds);
	if (add == NULL) {
		GDKfree(buf);
		throw(MAL,"io.printf", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
			type = va_arg(ap, int);
			type = ATOMbasetype(type);

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
				} else if (type == TYPE_lng) {
					goto largetypes;
#ifdef HAVE_HGE
				} else if (type == TYPE_hge) {
					/* Does this happen?
					 * If so, what do we have TODO ? */
					va_end(ap);
					return_error(type_error);
#endif
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
				} else if (type == TYPE_flt) {
					lval = (lng) *(flt *) p;
				} else if (type == TYPE_dbl) {
					lval = (lng) *(dbl *) p;
				} else if (type == TYPE_lng) {
					lval = *(lng *) p;
#ifdef HAVE_HGE
				} else if (type == TYPE_hge) {
					/* Does this happen?
					 * If so, what do we have TODO ? */
					va_end(ap);
					return_error(type_error);
#endif
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
				size_t length;

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
				if (dotseen && (size_t) prec < length)
					length = (size_t) prec;
				if (length > width)
					width = length;
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

	writemem(1);
	va_end(ap);
	*dst = 0;
	*res = buf;
	GDKfree(add);
	return MAL_SUCCEED;
}

#define getArgValue(s,p,k) VALptr(&(s)->stk[(p)->argv[k]])

#define G(X) getArgValue(stk,pci,X), getArgType(mb,pci,X)

static str
IOprintf(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *fmt = getArgReference_str(stk,pci,1);
	str fmt2 = NULL;
	str msg= MAL_SUCCEED;

	(void) cntxt;
	(void) mb;
	switch( pci->argc){
	case 2: msg= IOprintf_(&fmt2,*fmt);
			break;
	case 3: msg= IOprintf_(&fmt2,*fmt,G(2));
		break;
	case 4: msg= IOprintf_(&fmt2,*fmt,G(2),G(3));
		break;
	case 5: msg= IOprintf_(&fmt2,*fmt,G(2),G(3),G(4));
		break;
	case 6: msg= IOprintf_(&fmt2,*fmt,G(2),G(3),G(4),G(5));
		break;
	case 7: msg= IOprintf_(&fmt2,*fmt,G(2),G(3),G(4),G(5),G(6));
		break;
	case 8: msg= IOprintf_(&fmt2,*fmt,G(2),G(3),G(4),G(5),G(6),G(7));
		break;
	case 9: msg= IOprintf_(&fmt2,*fmt,G(2),G(3),G(4),G(5),G(6),G(7),G(8));
		break;
	case 10: msg= IOprintf_(&fmt2,*fmt,G(2),G(3),G(4),G(5),G(6),G(7),G(8),G(9));
	}
	if (msg== MAL_SUCCEED) {
		mnstr_printf(cntxt->fdout,"%s",fmt2);
		GDKfree(fmt2);
	}
	return msg;
}
static str
IOprintfStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	str *fmt = getArgReference_str(stk,pci,2);
	str fmt2 = NULL;
	stream *f= (stream *) getArgReference(stk,pci,1);
	str msg= MAL_SUCCEED;

	(void) cntxt;
	(void) mb;
	switch( pci->argc){
	case 3: msg= IOprintf_(&fmt2,*fmt);
		break;
	case 4: msg= IOprintf_(&fmt2,*fmt,G(3));
		break;
	case 5: msg= IOprintf_(&fmt2,*fmt,G(3),G(4));
		break;
	case 6: msg= IOprintf_(&fmt2,*fmt,G(3),G(4),G(5));
		break;
	case 7: msg= IOprintf_(&fmt2,*fmt,G(3),G(4),G(5),G(6));
		break;
	case 8: msg= IOprintf_(&fmt2,*fmt,G(3),G(4),G(5),G(6),G(7));
		break;
	case 9: msg= IOprintf_(&fmt2,*fmt,G(3),G(4),G(5),G(6),G(7),G(8));
		break;
	case 10: msg= IOprintf_(&fmt2,*fmt,G(3),G(4),G(5),G(6),G(7),G(8),G(9));
		break;
	case 11: msg= IOprintf_(&fmt2,*fmt,G(3),G(4),G(5),G(6),G(7),G(8),G(9),G(10));
	}
	if (msg== MAL_SUCCEED){
		mnstr_printf(f,"%s",fmt2);
		GDKfree(fmt2);
	}
	return msg;
}

/*
 * The table printing routine implementations.
 * They merely differ in destination and order prerequisite
 */
static str
IOtable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *piv[MAXPARAMS];
	int i;
	int tpe;
	ptr val;

	(void) cntxt;
	if ( pci->retc != 1 || pci->argc < 2 || pci->argc >= MAXPARAMS)
		throw(MAL, "io.table", "INTERNAL ERROR" " assertion error  retc %d  argc %d", pci->retc, pci->argc);

	memset(piv, 0, sizeof(BAT*) * MAXPARAMS);
	for (i = 1; i < pci->argc; i++) {
		tpe = getArgType(mb, pci, i);
		val = getArgReference(stk, pci, i);
		if (!isaBatType(tpe)) {
			while (--i >= 1)
				if (piv[i] != NULL)
					BBPunfix(piv[i]->batCacheid);
			throw(MAL, "io.table", ILLEGAL_ARGUMENT " BAT expected");
		}
		if ((piv[i] = BATdescriptor(*(bat *) val)) == NULL) {
			while (--i >= 1)
				BBPunfix(piv[i]->batCacheid);
			throw(MAL, "io.table", ILLEGAL_ARGUMENT " null BAT encountered");
		}
	}
	/* add materialized void column */
	piv[0] = BATdense(piv[1]->hseqbase, 0, BATcount(piv[1]));
	if (piv[0] == NULL) {
		for (i = 1; i < pci->argc; i++)
			BBPunfix(piv[i]->batCacheid);
		throw(MAL, "io.table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATprintcolumns(cntxt->fdout, pci->argc, piv);
	for (i = 0; i < pci->argc; i++)
		BBPunfix(piv[i]->batCacheid);
	return MAL_SUCCEED;
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

static str
IOexport(void *ret, bat *bid, str *fnme)
{
	BAT *b;
	stream *s;

	(void) ret;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "io.export", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	s = open_wastream(*fnme);
	if (s == NULL ){
		BBPunfix(b->batCacheid);
		throw(MAL, "io.export", "%s", mnstr_peek_error(NULL));
	}
	if (mnstr_errnr(s)) {
		mnstr_close(s);
		BBPunfix(b->batCacheid);
		throw(MAL, "io.export", "%s", mnstr_peek_error(NULL));
	}
    BATprintcolumns(s, 1, &b);
	close_stream(s);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * The import command reads a single BAT from an ASCII file produced by export.
 */
static str
IOimport(void *ret, bat *bid, str *fnme)
{
	BAT *b;
	ssize_t (*tconvert) (const char *, size_t *, ptr *, bool);
	ssize_t n;
	size_t bufsize = 2048;	/* NIELS:tmp change used to be 1024 */
	char *base, *cur, *end;
	char *buf;
	ptr t = 0;
	size_t lt = 0;
	FILE *fp = MT_fopen(*fnme, "r");
	char msg[BUFSIZ];

	(void) ret;
	if ((b = BATdescriptor(*bid)) == NULL) {
		if (fp)
			fclose(fp);
		throw(MAL, "io.import", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

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
			throw(MAL,"io.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		if ((fn = fileno(fp)) <= 0) {
			BBPunfix(b->batCacheid);
			fclose(fp);
			GDKfree(buf);
			throw(MAL, "io.import", OPERATION_FAILED ": fileno()");
		}
		if (fstat(fn, &st) != 0) {
			BBPunfix(b->batCacheid);
			fclose(fp);
			GDKfree(buf);
			throw(MAL, "io.import", OPERATION_FAILED ": fstat()");
		}

		(void) fclose(fp);
		if (st.st_size <= 0) {
			BBPunfix(b->batCacheid);
			GDKfree(buf);
			throw(MAL, "io.import", OPERATION_FAILED ": empty file");
		}
#if SIZEOF_SIZE_T == SIZEOF_INT
		if (st.st_size > 0x7FFFFFFF) {
			BBPunfix(b->batCacheid);
			GDKfree(buf);
			throw(MAL, "io.import", OPERATION_FAILED ": file too large");
		}
#endif
		base = cur = (char *) GDKmmap(*fnme, MMAP_SEQUENTIAL, (size_t) st.st_size);
		if (cur == NULL) {
			BBPunfix(b->batCacheid);
			GDKfree(buf);
			throw(MAL, "io.import", OPERATION_FAILED "GDKmmap()");
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
					char *tmp = GDKrealloc(buf, bufsize = MAX(inc,bufsize)*2);
					if (tmp == NULL) {
						BBPunfix(b->batCacheid);
						GDKfree(buf);
						GDKfree(t);
						GDKmunmap(base, end - base);
						throw(MAL, "io.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					}
					buf = tmp;
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
			char *tmp = GDKrealloc(buf, bufsize = MAX(inc,bufsize)*2);
			if (tmp == NULL) {
				BBPunfix(b->batCacheid);
				GDKfree(buf);
				GDKfree(t);
				GDKmunmap(base, end - base);
				throw(MAL, "io.import", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			buf = tmp;
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
			GDKfree(buf);
			GDKmunmap(base, end - base);
			GDKfree(t);
			throw(MAL, "io.import", "%s", msg);
		}
		n = tconvert(p, &lt, (ptr*)&t, true);
		if (n < 0) {
			BBPunfix(b->batCacheid);
			snprintf(msg,sizeof(msg),"error in input %s",buf);
			GDKfree(buf);
			GDKmunmap(base, end - base);
			GDKfree(t);
			throw(MAL, "io.import", "%s", msg);
		}
		p += n;
		if (BUNappend(b, t, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			GDKfree(buf);
			GDKfree(t);
			GDKmunmap(base, end - base);
			throw(MAL, "io.import", "insert failed");
		}

#if 0							/* why do this? any measured effects? */
/*
 * Unmap already parsed memory, to keep the memory usage low.
 */
#ifndef WIN32
#define MAXBUF 40*MT_pagesize()
		if ((unsigned) (cur - base) > MAXBUF) {
			GDKmunmap(base, MAXBUF);
			base += MAXBUF;
		}
#endif
#endif
	}
	/* Cleanup and exit. Return the filled BAT.  */
	if (t)
		GDKfree(t);
	GDKfree(buf);
	GDKmunmap(base, end - base);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}



static str
IOsetmallocsuccesscount(void *res, lng *count) {
	(void) res;
	GDKsetmallocsuccesscount(*count);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func mal_io_init_funcs[] = {
 pattern("io", "stdin", io_stdin, false, "return the input stream to the database client", args(1,1, arg("",bstream))),
 pattern("io", "stdout", io_stdout, false, "return the output stream for the database client", args(1,1, arg("",streams))),
 pattern("io", "print", IOprint_val, false, "Print a MAL value tuple .", args(1,3, arg("",void),argany("val",1),varargany("lst",0))),
 pattern("io", "print", IOtable, false, "BATs are printed with '#' for legend \nlines, and the BUNs on seperate lines \nbetween brackets, containing each to \ncomma separated values (head and tail). \nIf multiple BATs are passed for printing, \nprint() performs an implicit natural \njoin on the void head, producing a multi attribute table.", args(1,2, arg("",void),batvarargany("b1",0))),
 pattern("io", "print", IOprint_val, false, "Print a MAL value.", args(1,2, arg("",void),argany("val",1))),
 pattern("io", "print", IOprint_val, false, "Print a MAL value column .", args(1,2, arg("",void),batargany("val",1))),
 pattern("io", "printf", IOprintf, false, "Select default format ", args(1,3, arg("",void),arg("fmt",str),varargany("val",0))),
 pattern("io", "printf", IOprintf, false, "Select default format ", args(1,2, arg("",void),arg("fmt",str))),
 pattern("io", "printf", IOprintfStream, false, "Select default format ", args(1,4, arg("",void),arg("filep",streams),arg("fmt",str),varargany("val",0))),
 pattern("io", "printf", IOprintfStream, false, "Select default format ", args(1,3, arg("",void),arg("filep",streams),arg("fmt",str))),
 command("io", "export", IOexport, false, "Export a BAT as ASCII to a file. If the 'filepath' is not absolute, it\nis put into the $DBPATH directory. Success of failure is indicated.", args(0,2, batargany("b",2),arg("filepath",str))),
 command("io", "import", IOimport, false, "Import a BAT from an ASCII dump. The tuples are appended to the\nfirst argument. Its signature must match the dump,\nelse parsing errors will occur as an exception.", args(0,2, batargany("b",2),arg("filepath",str))),
 command("io", "setmallocsuccesscount", IOsetmallocsuccesscount, false, "Set number of mallocs that are allowed to succeed.", args(1,2, arg("",void),arg("count",lng))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_mal_io_mal)
{ mal_module("mal_io", NULL, mal_io_init_funcs); }

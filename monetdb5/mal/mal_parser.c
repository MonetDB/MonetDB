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

/* (c): M. L. Kersten
*/

#include "monetdb_config.h"
#include "mal_parser.h"
#include "mal_resolve.h"
#include "mal_linker.h"
#include "mal_atom.h"			/* for malAtomDefinition(), malAtomProperty() */
#include "mal_interpreter.h"	/* for showErrors() */
#include "mal_instruction.h"	/* for pushEndInstruction(), findVariableLength() */
#include "mal_namespace.h"
#include "mal_utils.h"
#include "mal_builder.h"
#include "mal_type.h"
#include "mal_session.h"
#include "mal_private.h"

#define FATALINPUT (MAXERRORS+1)
#define NL(X) ((X)=='\n' || (X)=='\r')

static str idCopy(allocator *ma, Client ctx, int len);
static str strCopy(allocator *va, Client ctx, int len);

/*
 * For error reporting we may have to find the start of the previous line,
 * which, ofcourse, is easy given the client buffer.
 * The remaining functions are self-explanatory.
*/
static str
lastline(Client ctx)
{
	str s = CURRENT(ctx);
	if (NL(*s))
		s++;
	while (s > ctx->fdin->buf && !NL(*s))
		s--;
	if (NL(*s))
		s++;
	return s;
}

static ssize_t
position(Client ctx)
{
	str s = lastline(ctx);
	return (ssize_t) (CURRENT(ctx) - s);
}

/*
 * Upon encountering an error we skip to the nearest semicolon,
 * or comment terminated by a new line
 */
static inline void
skipToEnd(Client ctx)
{
	char c;
	while ((c = *CURRENT(ctx)) != ';' && c && c != '\n')
		nextChar(ctx);
	if (c && c != '\n')
		nextChar(ctx);
}

/*
 * Keep on syntax error for reflection and correction.
 */
static void
parseError(allocator *ma, Client ctx, str msg)
{
	MalBlkPtr mb;
	char *old, *new;
	char buf[1028] = { 0 };
	char *s = buf, *t, *line = "", *marker = "";
	char *l = lastline(ctx);
	ssize_t i;

	if (ctx->backup) {
		freeSymbol(ctx->curprg);
		ctx->curprg = ctx->backup;
		ctx->backup = 0;
	}

	mb = ctx->curprg->def;
	s = buf;
	for (t = l; *t && *t != '\n' && s < buf + sizeof(buf) - 4; t++) {
		*s++ = *t;
	}
	*s++ = '\n';
	*s = 0;
	line = createException(SYNTAX, "parseError", "%s", buf);

	/* produce the position marker */
	s = buf;
	i = position(ctx);
	for (; i > 0 && s < buf + sizeof(buf) - 4; i--) {
		*s++ = ((l && *(l + 1) && *l++ != '\t')) ? ' ' : '\t';
	}
	*s++ = '^';
	*s = 0;
	marker = createException(SYNTAX, "parseError", "%s%s", buf, msg);

	old = mb->errors;
	new = ma_alloc(ma, (old ? strlen(old) : 0) + strlen(line) + strlen(marker) +
					64);
	if (new == NULL) {
		freeException(line);
		freeException(marker);
		skipToEnd(ctx);
		return;					// just stick to old error message
	}
	mb->errors = new;
	if (old) {
		new = stpcpy(new, old);
		//GDKfree(old);
	}
	new = stpcpy(new, line);
	new = stpcpy(new, marker);

	freeException(line);
	freeException(marker);
	skipToEnd(ctx);
}

/* Before a line is parsed we check for a request to echo it.
 * This command should be executed at the beginning of a parse
 * request and each time we encounter EOL.
*/
static void
echoInput(Client ctx)
{
	char *c = CURRENT(ctx);
	if (ctx->listing == 1 && *c && !NL(*c)) {
		mnstr_printf(ctx->fdout, "#");
		while (*c && !NL(*c)) {
			mnstr_printf(ctx->fdout, "%c", *c++);
		}
		mnstr_printf(ctx->fdout, "\n");
	}
}

static inline void
skipSpace(Client ctx)
{
	char *s = &currChar(ctx);
	for (;;) {
		switch (*s++) {
		case ' ':
		case '\t':
		case '\n':
		case '\r':
			nextChar(ctx);
			break;
		default:
			return;
		}
	}
}

static inline void
advance(Client ctx, size_t length)
{
	ctx->yycur += length;
	skipSpace(ctx);
}

/*
 * The most recurring situation is to recognize identifiers.
 * This process is split into a few steps to simplify subsequent
 * construction and comparison.
 * IdLength searches the end of an identifier without changing
 * the cursor into the input pool.
 * IdCopy subsequently prepares a GDK string for inclusion in the
 * instruction datastructures.
*/

static const bool opCharacter[256] = {
	['$'] = true,
	['!'] = true,
	['%'] = true,
	['&'] = true,
	['*'] = true,
	['+'] = true,
	['-'] = true,
	['/'] = true,
	[':'] = true,
	['<'] = true,
	['='] = true,
	['>'] = true,
	['\\'] = true,
	['^'] = true,
	['|'] = true,
	['~'] = true,
};

static const bool idCharacter[256] = {
	['a'] = true,
	['b'] = true,
	['c'] = true,
	['d'] = true,
	['e'] = true,
	['f'] = true,
	['g'] = true,
	['h'] = true,
	['i'] = true,
	['j'] = true,
	['k'] = true,
	['l'] = true,
	['m'] = true,
	['n'] = true,
	['o'] = true,
	['p'] = true,
	['q'] = true,
	['r'] = true,
	['s'] = true,
	['t'] = true,
	['u'] = true,
	['v'] = true,
	['w'] = true,
	['x'] = true,
	['y'] = true,
	['z'] = true,
	['A'] = true,
	['B'] = true,
	['C'] = true,
	['D'] = true,
	['E'] = true,
	['F'] = true,
	['G'] = true,
	['H'] = true,
	['I'] = true,
	['J'] = true,
	['K'] = true,
	['L'] = true,
	['M'] = true,
	['N'] = true,
	['O'] = true,
	['P'] = true,
	['Q'] = true,
	['R'] = true,
	['S'] = true,
	['T'] = true,
	['U'] = true,
	['V'] = true,
	['W'] = true,
	['X'] = true,
	['Y'] = true,
	['Z'] = true,
	[TMPMARKER] = true,
};

static const bool idCharacter2[256] = {
	['a'] = true,
	['b'] = true,
	['c'] = true,
	['d'] = true,
	['e'] = true,
	['f'] = true,
	['g'] = true,
	['h'] = true,
	['i'] = true,
	['j'] = true,
	['k'] = true,
	['l'] = true,
	['m'] = true,
	['n'] = true,
	['o'] = true,
	['p'] = true,
	['q'] = true,
	['r'] = true,
	['s'] = true,
	['t'] = true,
	['u'] = true,
	['v'] = true,
	['w'] = true,
	['x'] = true,
	['y'] = true,
	['z'] = true,
	['A'] = true,
	['B'] = true,
	['C'] = true,
	['D'] = true,
	['E'] = true,
	['F'] = true,
	['G'] = true,
	['H'] = true,
	['I'] = true,
	['J'] = true,
	['K'] = true,
	['L'] = true,
	['M'] = true,
	['N'] = true,
	['O'] = true,
	['P'] = true,
	['Q'] = true,
	['R'] = true,
	['S'] = true,
	['T'] = true,
	['U'] = true,
	['V'] = true,
	['W'] = true,
	['X'] = true,
	['Y'] = true,
	['Z'] = true,
	['0'] = true,
	['1'] = true,
	['2'] = true,
	['3'] = true,
	['4'] = true,
	['5'] = true,
	['6'] = true,
	['7'] = true,
	['8'] = true,
	['9'] = true,
	[TMPMARKER] = true,
	['@'] = true,
};

static int
idLength(Client ctx)
{
	str s, t;
	int len = 0;

	skipSpace(ctx);
	s = CURRENT(ctx);
	t = s;

	if (!idCharacter[(unsigned char) (*s)])
		return 0;
	/* avoid a clash with old temporaries */
	if (s[0] == TMPMARKER)
		s[0] = REFMARKER;
	/* prepare escape of temporary names */
	s++;
	while (len < IDLENGTH && idCharacter2[(unsigned char) (*s)]) {
		s++;
		len++;
	}
	if (len == IDLENGTH)
		// skip remainder
		while (idCharacter2[(unsigned char) (*s)])
			s++;
	return (int) (s - t);
}

/* Simple type identifiers can not be marked with a type variable. */
static size_t
typeidLength(Client ctx)
{
	size_t l;
	char id[IDLENGTH], *t = id;
	str s;
	skipSpace(ctx);
	s = CURRENT(ctx);

	if (!idCharacter[(unsigned char) (*s)])
		return 0;
	l = 1;
	*t++ = *s++;
	while (l < IDLENGTH
		   && (idCharacter[(unsigned char) (*s)]
			   || isdigit((unsigned char) *s))) {
		*t++ = *s++;
		l++;
	}
	/* recognize the special type variables {any, any_<nr>} */
	if (strncmp(id, "any", 3) == 0)
		return 3;
	if (strncmp(id, "any_", 4) == 0)
		return 4;
	return l;
}

static str
idCopy(allocator *ma, Client ctx, int length)
{
	str s = ma_alloc(ma, length + 1);
	if (s == NULL)
		return NULL;
	memcpy(s, CURRENT(ctx), (size_t) length);
	s[length] = 0;
	/* avoid a clash with old temporaries */
	advance(ctx, length);
	return s;
}

static int
MALlookahead(Client ctx, str kw, int length)
{
	int i;

	/* avoid double test or use lowercase only. */
	if (currChar(ctx) == *kw &&
		strncmp(CURRENT(ctx), kw, length) == 0 &&
		!idCharacter[(unsigned char) (CURRENT(ctx)[length])] &&
		!isdigit((unsigned char) (CURRENT(ctx)[length]))) {
		return 1;
	}
	/* check for capitalized versions */
	for (i = 0; i < length; i++)
		if (tolower(CURRENT(ctx)[i]) != kw[i])
			return 0;
	if (!idCharacter[(unsigned char) (CURRENT(ctx)[length])] &&
		!isdigit((unsigned char) (CURRENT(ctx)[length]))) {
		return 1;
	}
	return 0;
}

static inline int
MALkeyword(Client ctx, str kw, int length)
{
	skipSpace(ctx);
	if (MALlookahead(ctx, kw, length)) {
		advance(ctx, length);
		return 1;
	}
	return 0;
}

/*
 * Keyphrase testing is limited to a few characters only
 * (check manually). To speed this up we use a pipelined and inline macros.
*/

static inline int
keyphrase1(Client ctx, str kw)
{
	skipSpace(ctx);
	if (currChar(ctx) == *kw) {
		advance(ctx, 1);
		return 1;
	}
	return 0;
}

static inline int
keyphrase2(Client ctx, str kw)
{
	skipSpace(ctx);
	if (CURRENT(ctx)[0] == kw[0] && CURRENT(ctx)[1] == kw[1]) {
		advance(ctx, 2);
		return 1;
	}
	return 0;
}

/*
 * A similar approach is used for string literals.
 * Beware, string lengths returned include the
 * brackets and escapes. They are eaten away in strCopy.
 * We should provide the C-method to split strings and
 * concatenate them upon retrieval[todo]
*/
static int
stringLength(Client ctx)
{
	int l = 0;
	int quote = 0;
	str s;
	skipSpace(ctx);
	s = CURRENT(ctx);

	if (*s != '"')
		return 0;
	for (s++; *s; l++, s++) {
		if (quote) {
			quote = 0;
		} else {
			if (*s == '"')
				break;
			quote = *s == '\\';
		}
	}
	return l + 2;
}

/*Beware, the idcmp routine uses a short cast to compare multiple bytes
 * at once. This may cause problems when the net string length is zero.
*/

static str
strCopy(allocator *va, Client ctx, int length)
{
	str s;
	int i;

	i = length < 4 ? 4 : length;
	s = va?ma_alloc(va, i) : GDKmalloc(i);
	if (s == 0)
		return NULL;
	memcpy(s, CURRENT(ctx) + 1, (size_t) (length - 2));
	s[length - 2] = 0;
	mal_unquote(s);
	return s;
}

/*
 * And a similar approach is used for operator names.
 * A lookup table is considered, because it generally is
 * faster then a non-dense switch.
*/
static int
operatorLength(Client ctx)
{
	int l = 0;
	str s;

	skipSpace(ctx);
	for (s = CURRENT(ctx); *s; s++) {
		if (opCharacter[(unsigned char) (*s)])
			l++;
		else
			return l;
	}
	return l;
}

/*
 * The lexical analyser for constants is a little more complex.
 * Aside from getting its length, we need an indication of its type.
 * The constant structure is initialized for later use.
 */
static int
cstToken(allocator *ma, Client ctx, MalBlkPtr mb, ValPtr cst)
{
	int i = 0;
	str s = CURRENT(ctx);

	*cst = (ValRecord) {
		.vtype = TYPE_int,
		.val.lval = 0,
		.bat = false,
	};
	switch (*s) {
	case '{':
	case '[':
		/* JSON Literal */
		break;
	case '"':
		i = stringLength(ctx);
		VALset(cst, TYPE_str, strCopy(mb->ma, ctx, i));
		return i;
	case '-':
		i++;
		s++;
		/* fall through */
	case '0':
		if (s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
			/* deal with hex */
			i += 2;
			s += 2;
			while (isxdigit((unsigned char) *s)) {
				i++;
				s++;
			}
			goto handleInts;
		}
		/* fall through */
	case '1':
	case '2':
	case '3':
	case '4':
	case '5':
	case '6':
	case '7':
	case '8':
	case '9':
		while (isdigit((unsigned char) *s)) {
			i++;
			s++;
		}

		/* fall through */
	case '.':
		if (*s == '.' && isdigit((unsigned char) *(s + 1))) {
			i++;
			s++;
			while (isdigit((unsigned char) *s)) {
				i++;
				s++;
			}
			cst->vtype = TYPE_dbl;
		}
		if (*s == 'e' || *s == 'E') {
			i++;
			s++;
			if (*s == '-' || *s == '+') {
				i++;
				s++;
			}
			cst->vtype = TYPE_dbl;
			while (isdigit((unsigned char) *s)) {
				i++;
				s++;
			}
		}
		if (cst->vtype == TYPE_flt) {
			size_t len = sizeof(flt);
			float *pval = &cst->val.fval;
			if (fltFromStr(CURRENT(ctx), &len, &pval, false) < 0) {
				parseError(ma, ctx, GDKerrbuf);
				return i;
			}
		}
		if (cst->vtype == TYPE_dbl) {
			size_t len = sizeof(dbl);
			double *pval = &cst->val.dval;
			if (dblFromStr(CURRENT(ctx), &len, &pval, false) < 0) {
				parseError(ma, ctx, GDKerrbuf);
				return i;
			}
		}
		if (*s == '@') {
			size_t len = sizeof(lng);
			lng l, *pval = &l;
			if (lngFromStr(CURRENT(ctx), &len, &pval, false) < 0) {
				parseError(ma, ctx, GDKerrbuf);
				return i;
			}
			if (is_lng_nil(l) || l < 0
#if SIZEOF_OID < SIZEOF_LNG
				|| l > GDK_oid_max
#endif
					)
				cst->val.oval = oid_nil;
			else
				cst->val.oval = (oid) l;
			cst->vtype = TYPE_oid;
			i++;
			s++;
			while (isdigit((unsigned char) *s)) {
				i++;
				s++;
			}
			return i;
		}
		if (*s == 'L') {
			if (cst->vtype == TYPE_int)
				cst->vtype = TYPE_lng;
			if (cst->vtype == TYPE_flt)
				cst->vtype = TYPE_dbl;
			i++;
			s++;
			if (*s == 'L') {
				i++;
				s++;
			}
			if (cst->vtype == TYPE_dbl) {
				size_t len = sizeof(dbl);
				dbl *pval = &cst->val.dval;
				if (dblFromStr(CURRENT(ctx), &len, &pval, false) < 0) {
					parseError(ma, ctx, GDKerrbuf);
					return i;
				}
			} else {
				size_t len = sizeof(lng);
				lng *pval = &cst->val.lval;
				if (lngFromStr(CURRENT(ctx), &len, &pval, false) < 0) {
					parseError(ma, ctx, GDKerrbuf);
					return i;
				}
			}
			return i;
		}
#ifdef HAVE_HGE
		if (*s == 'H' && cst->vtype == TYPE_int) {
			size_t len = sizeof(hge);
			hge *pval = &cst->val.hval;
			cst->vtype = TYPE_hge;
			i++;
			s++;
			if (*s == 'H') {
				i++;
				s++;
			}
			if (hgeFromStr(CURRENT(ctx), &len, &pval, false) < 0) {
				parseError(ma, ctx, GDKerrbuf);
				return i;
			}
			return i;
		}
#endif
  handleInts:
		assert(cst->vtype != TYPE_lng);
#ifdef HAVE_HGE
		assert(cst->vtype != TYPE_hge);
#endif
		if (cst->vtype == TYPE_int) {
#ifdef HAVE_HGE
			size_t len = sizeof(hge);
			hge l, *pval = &l;
			if (hgeFromStr(CURRENT(ctx), &len, &pval, false) < 0)
				l = hge_nil;

			if ((hge) GDK_int_min <= l && l <= (hge) GDK_int_max) {
				cst->vtype = TYPE_int;
				cst->val.ival = (int) l;
			} else if ((hge) GDK_lng_min <= l && l <= (hge) GDK_lng_max) {
				cst->vtype = TYPE_lng;
				cst->val.lval = (lng) l;
			} else {
				cst->vtype = TYPE_hge;
				cst->val.hval = l;
			}
#else
			size_t len = sizeof(lng);
			lng l, *pval = &l;
			if (lngFromStr(CURRENT(ctx), &len, &pval, false) < 0)
				l = lng_nil;

			if ((lng) GDK_int_min <= l && l <= (lng) GDK_int_max) {
				cst->vtype = TYPE_int;
				cst->val.ival = (int) l;
			} else {
				cst->vtype = TYPE_lng;
				cst->val.lval = l;
			}
#endif
		}
		return i;

	case 'f':
		if (strncmp(s, "false", 5) == 0 && !isalnum((unsigned char) *(s + 5)) &&
			*(s + 5) != '_') {
			cst->vtype = TYPE_bit;
			cst->val.btval = 0;
			cst->len = 1;
			return 5;
		}
		return 0;
	case 't':
		if (strncmp(s, "true", 4) == 0 && !isalnum((unsigned char) *(s + 4)) &&
			*(s + 4) != '_') {
			cst->vtype = TYPE_bit;
			cst->val.btval = 1;
			cst->len = 1;
			return 4;
		}
		return 0;
	case 'n':
		if (strncmp(s, "nil", 3) == 0 && !isalnum((unsigned char) *(s + 3)) &&
			*(s + 3) != '_') {
			cst->vtype = TYPE_void;
			cst->len = 0;
			cst->val.oval = oid_nil;
			return 3;
		}
	}
	return 0;
}

#define cstCopy(C,I)  idCopy(C,I)

/* Type qualifier
 * Types are recognized as identifiers preceded by a colon.
 *
 * The type ANY matches any type specifier.
 * Appending it with an alias turns it into a type variable.
 * The type alias is \$DIGIT (1-3) and can be used to relate types
 * by type equality.
 * The type variable are defined within the context of a function
 * scope.
 * Additional information, such as a repetition factor,
 * encoding tables, or type dependency should be modeled as properties.
 */
static int
typeAlias(allocator *ma, Client ctx, int tpe)
{
	int t;

	if (tpe != TYPE_any)
		return 0;
	if (currChar(ctx) == TMPMARKER) {
		nextChar(ctx);
		t = currChar(ctx) - '0';
		if (t <= 0 || t > 3) {
			parseError(ma, ctx, "[1-3] expected\n");
			return -1;
		} else
			nextChar(ctx);
		return t;
	}
	return 0;
}

/*
 * The simple type analysis currently assumes a proper type identifier.
 * We should change getMALtype to return a failure instead.
 */
static int
simpleTypeId(allocator *ma, Client ctx)
{
	int tpe;
	size_t l;

	nextChar(ctx);
	l = typeidLength(ctx);
	if (l == 0) {
		parseError(ma, ctx, "Type identifier expected\n");
		ctx->yycur--;			/* keep it */
		return -1;
	}
	if (l == 3 && CURRENT(ctx)[0] == 'b' && CURRENT(ctx)[1] == 'a' && CURRENT(ctx)[2] == 't')
		tpe = newBatType(TYPE_any);
	else
		tpe = getAtomIndex(CURRENT(ctx), l, -1);
	if (tpe < 0) {
		parseError(ma, ctx, "Type identifier expected\n");
		ctx->yycur -= l;		/* keep it */
		return TYPE_void;
	}
	advance(ctx, l);
	return tpe;
}

static int
parseTypeId(allocator *ma, Client ctx)
{
	int i = TYPE_any, kt = 0;
	char *s = CURRENT(ctx);
	int tt;

	if (strncmp(s, ":bat", 4) == 0 || strncmp(s, ":BAT", 4) == 0) {
		int opt = 0;
		/* parse :bat[:type] */
		advance(ctx, 4);
		if (currChar(ctx) == '?') {
			opt = 1;
			advance(ctx, 1);
		}
		if (currChar(ctx) != '[') {
			if (opt)
				setOptBat(i);
			else
				i = newBatType(TYPE_any);
			return i;
			if (!opt)
				return newBatType(TYPE_any);

			parseError(ma, ctx, "':bat[:type]' expected\n");
			return -1;
		}
		advance(ctx, 1);
		if (currChar(ctx) == ':') {
			tt = simpleTypeId(ma, ctx);
			kt = typeAlias(ma, ctx, tt);
			if (kt < 0)
				return kt;
		} else {
			parseError(ma, ctx, "':bat[:any]' expected\n");
			return -1;
		}

		if (!opt)
			i = newBatType(tt);
		if (kt > 0)
			setTypeIndex(i, kt);
		if (opt)
			setOptBat(i);

		if (currChar(ctx) != ']')
			parseError(ma, ctx, "']' expected\n");
		nextChar(ctx);		// skip ']'
		skipSpace(ctx);
		return i;
	}
	if (currChar(ctx) == ':') {
		tt = simpleTypeId(ma, ctx);
		kt = typeAlias(ma, ctx, tt);
		if (kt < 0)
			return kt;
		if (kt > 0)
			setTypeIndex(tt, kt);
		return tt;
	}
	parseError(ma, ctx, "<type identifier> expected\n");
	return -1;
}

static inline int
typeElm(allocator *ma, Client ctx, int def)
{
	if (currChar(ctx) != ':')
		return def;				/* no type qualifier */
	return parseTypeId(ma, ctx);
}

 /*
  * The Parser
  * The client is responsible to collect the
  * input for parsing in a single string before calling the parser.
  * Once the input is available parsing runs in a critical section for
  * a single client thread.
  *
  * The parser uses the rigid structure of the language to speedup
  * analysis. In particular, each input line is translated into
  * a MAL instruction record as quickly as possible. Its context is
  * manipulated during the parsing process, by keeping the  curPrg,
  * curBlk, and curInstr variables.
  *
  * The language statements of the parser are gradually introduced, with
  * the overall integration framework last.
  * The convention is to return a zero when an error has been
  * reported or when the structure can not be recognized.
  * Furthermore, we assume that blancs have been skipped before entering
  * recognition of a new token.
  *
  * Module statement.
  * The module and import commands have immediate effect.
  * The module statement switches the location for symbol table update
  * to a specific named area. The effect is that all definitions may become
  * globally known (?) and symbol table should be temporarily locked
  * for updates by concurrent users.
  *
  * @multitable @columnfractions 0.15 0.8
  * @item moduleStmt
  * @tab :  @sc{atom} ident [':'ident]
  * @item
  * @tab | @sc{module} ident
  * @end multitable
  *
  * An atom statement does not introduce a new module.
  */
static void
helpInfo(allocator *ma, Client ctx, allocator *va, str *help)
{
	int l = 0;
	char c, *e, *s;

	if (MALkeyword(ctx, "comment", 7)) {
		skipSpace(ctx);
		// The comment is either a quoted string or all characters up to the next semicolon
		c = currChar(ctx);
		if (c != '"') {
			e = s = CURRENT(ctx);
			for (; *e; l++, e++)
				if (*e == ';')
					break;
			*help = strCopy(va, ctx, l);
			skipToEnd(ctx);
		} else {
			if ((l = stringLength(ctx))) {
				GDKfree(*help);
				*help = strCopy(va, ctx, l);
				if (*help)
					advance(ctx, l - 1);
				skipToEnd(ctx);
			} else {
				parseError(ma, ctx, "<string> expected\n");
			}
		}
	} else if (currChar(ctx) != ';')
		parseError(ma, ctx, "';' expected\n");
}

static InstrPtr
binding(allocator *ma, Client ctx, MalBlkPtr curBlk, InstrPtr curInstr, int flag)
{
	int l, varid = -1;
	malType type;

	l = idLength(ctx);
	if (l > 0) {
		varid = findVariableLength(curBlk, CURRENT(ctx), l);
		if (varid < 0) {
			varid = newVariable(curBlk, CURRENT(ctx), l, TYPE_any);
			advance(ctx, l);
			if (varid < 0)
				return curInstr;
			type = typeElm(ma, ctx, TYPE_any);
			if (type < 0)
				return curInstr;
			if (isPolymorphic(type))
				setPolymorphic(curInstr, type, TRUE);
			setVarType(curBlk, varid, type);
		} else if (flag) {
			parseError(ma, ctx, "Argument defined twice\n");
			typeElm(ma, ctx, getVarType(curBlk, varid));
		} else {
			advance(ctx, l);
			type = typeElm(ma, ctx, getVarType(curBlk, varid));
			if (type != getVarType(curBlk, varid))
				parseError(ma, ctx, "Incompatible argument type\n");
			if (isPolymorphic(type))
				setPolymorphic(curInstr, type, TRUE);
			setVarType(curBlk, varid, type);
		}
	} else if (currChar(ctx) == ':') {
		type = typeElm(ma, ctx, TYPE_any);
		varid = newTmpVariable(curBlk, type);
		if (varid < 0)
			return curInstr;
		if (isPolymorphic(type))
			setPolymorphic(curInstr, type, TRUE);
		setVarType(curBlk, varid, type);
	} else {
		parseError(ma, ctx, "argument expected\n");
		return curInstr;
	}
	if (varid >= 0)
		curInstr = pushArgument(curBlk, curInstr, varid);
	return curInstr;
}

/*
 * At this stage the LHS part has been parsed and the destination
 * variables have been set. Next step is to parse the expression,
 * which starts with an operand.
 * This code is used in both positions of the expression
 */
static int
term(allocator *ma, Client ctx, MalBlkPtr curBlk, InstrPtr *curInstr, int ret)
{
	int i, idx, free = 1;
	ValRecord cst;
	int cstidx = -1;
	malType tpe = TYPE_any;

	if ((i = cstToken(ma, ctx, curBlk, &cst))) {
		advance(ctx, i);
		if (currChar(ctx) != ':' && cst.vtype == TYPE_dbl
			&& cst.val.dval > FLT_MIN && cst.val.dval <= FLT_MAX) {
			float dummy = (flt) cst.val.dval;
			cst.vtype = TYPE_flt;
			cst.val.fval = dummy;
		}
		cstidx = fndConstant(curBlk, &cst, MAL_VAR_WINDOW);
		if (cstidx >= 0) {

			if (currChar(ctx) == ':') {
				tpe = typeElm(ma, ctx, getVarType(curBlk, cstidx));
				if (tpe < 0)
					return 3;
				cst.bat = isaBatType(tpe);
				if (tpe != getVarType(curBlk, cstidx)) {
					cstidx = defConstant(curBlk, tpe, &cst);
					if (cstidx < 0)
						return 3;
					setPolymorphic(*curInstr, tpe, FALSE);
					free = 0;
				}
			} else if (cst.vtype != getVarType(curBlk, cstidx)) {
				cstidx = defConstant(curBlk, cst.vtype, &cst);
				if (cstidx < 0)
					return 3;
				setPolymorphic(*curInstr, cst.vtype, FALSE);
				free = 0;
			}
			/* protect against leaks coming from constant reuse */
			if (free && ATOMextern(cst.vtype) && cst.val.pval)
				VALclear(&cst);
			*curInstr = pushArgument(curBlk, *curInstr, cstidx);
			return ret;
		} else {
			/* add a new constant literal, the :type could be erroneously be a coltype */
			tpe = typeElm(ma, ctx, cst.vtype);
			if (tpe < 0)
				return 3;
			cst.bat = isaBatType(tpe);
			cstidx = defConstant(curBlk, tpe, &cst);
			if (cstidx < 0)
				return 3;
			setPolymorphic(*curInstr, tpe, FALSE);
			*curInstr = pushArgument(curBlk, *curInstr, cstidx);
			return ret;
		}
	} else if ((i = idLength(ctx))) {
		if ((idx = findVariableLength(curBlk, CURRENT(ctx), i)) == -1) {
			idx = newVariable(curBlk, CURRENT(ctx), i, TYPE_any);
			advance(ctx, i);
			if (idx < 0)
				return 0;
		} else {
			advance(ctx, i);
		}
		if (currChar(ctx) == ':') {
			/* skip the type description */
			tpe = typeElm(ma, ctx, TYPE_any);
			if (getVarType(curBlk, idx) == TYPE_any)
				setVarType(curBlk, idx, tpe);
			else if (getVarType(curBlk, idx) != tpe) {
				/* non-matching types */
				return 4;
			}
		}
		*curInstr = pushArgument(curBlk, *curInstr, idx);
	} else if (currChar(ctx) == ':') {
		tpe = typeElm(ma, ctx, TYPE_any);
		if (tpe < 0)
			return 3;
		setPolymorphic(*curInstr, tpe, FALSE);
		idx = newTypeVariable(curBlk, tpe);
		*curInstr = pushArgument(curBlk, *curInstr, idx);
		return ret;
	}
	return 0;
}

static int
parseAtom(allocator *ma, Client ctx)
{
	const char *modnme = 0;
	int l, tpe;
	char *nxt = CURRENT(ctx);

	if ((l = idLength(ctx)) <= 0) {
		parseError(ma, ctx, "atom name expected\n");
		return -1;
	}

	/* parse: ATOM id:type */
	modnme = putNameLen(nxt, l);
	if (modnme == NULL) {
		parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	advance(ctx, l);
	if (currChar(ctx) != ':')
		tpe = TYPE_void;		/* no type qualifier */
	else
		tpe = parseTypeId(ma, ctx);
	if (ATOMindex(modnme) < 0) {
		if (ctx->curprg->def->errors)
			freeException(ctx->curprg->def->errors);
		ctx->curprg->def->errors = malAtomDefinition(modnme, tpe);
	}
	if (modnme != userRef)
		ctx->curmodule = fixModule(modnme);
	else
		ctx->curmodule = ctx->usermodule;
	ctx->usermodule->isAtomModule = TRUE;
	skipSpace(ctx);
	helpInfo(ma, ctx, NULL, &ctx->usermodule->help);
	return 0;
}

/*
 * All modules, except 'user', should be global
 */
static int
parseModule(allocator *ma, Client ctx)
{
	const char *modnme = 0;
	int l;
	char *nxt;

	nxt = CURRENT(ctx);
	if ((l = idLength(ctx)) <= 0) {
		parseError(ma, ctx, "<module path> expected\n");
		return -1;
	}
	modnme = putNameLen(nxt, l);
	if (modnme == NULL) {
		parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	advance(ctx, l);
	if (strcmp(modnme, ctx->usermodule->name) == 0) {
		// ignore this module definition
	} else if (getModule(modnme) == NULL) {
		if (globalModule(modnme) == NULL)
			parseError(ma, ctx, "<module> could not be created");
	}
	if (modnme != userRef)
		ctx->curmodule = fixModule(modnme);
	else
		ctx->curmodule = ctx->usermodule;
	skipSpace(ctx);
	helpInfo(ma, ctx, NULL, &ctx->usermodule->help);
	return 0;
}

/*
 * Include files should be handled in line with parsing. This way we
 * are ensured that any possible signature definition will be known
 * afterwards. The effect is that errors in the include sequence are
 * marked as warnings.
 */
static int
parseInclude(allocator *ma, Client ctx)
{
	const char *modnme = 0;
	char *s;
	int x;
	char *nxt;

	nxt = CURRENT(ctx);

	if ((x = idLength(ctx)) > 0) {
		modnme = putNameLen(nxt, x);
		advance(ctx, x);
	} else if ((x = stringLength(ctx)) > 0) {
		modnme = putNameLen(nxt + 1, x - 1);
		advance(ctx, x);
	} else {
		parseError(ma, ctx, "<module name> expected\n");
		return -1;
	}
	if (modnme == NULL) {
		parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}

	if (currChar(ctx) != ';') {
		parseError(ma, ctx, "';' expected\n");
		return 0;
	}
	skipToEnd(ctx);

	if (!malLibraryEnabled(modnme)) {
		return 0;
	}

	if (getModule(modnme) == NULL) {
		s = loadLibrary(modnme, FALSE);
		if (s) {
			parseError(ma, ctx, s);
			freeException(s);
			return 0;
		}
	}
	if ((s = malInclude(ctx, modnme, 0))) {
		parseError(ma, ctx, s);
		freeException(s);
		return 0;
	}
	return 0;
}

/* return the combined count of the number of arguments and the number
 * of return values so that we can allocate enough space in the
 * instruction; returns -1 on error (missing closing parenthesis) */
static int
cntArgsReturns(allocator *ma, Client ctx, int *retc)
{
	size_t yycur = ctx->yycur;
	int cnt = 0;
	char ch;

	ch = currChar(ctx);
	if (ch != ')') {
		cnt++;
		while (ch != ')' && ch && !NL(ch)) {
			if (ch == ',')
				cnt++;
			nextChar(ctx);
			ch = currChar(ctx);
		}
	}
	if (ch != ')') {
		parseError(ma, ctx, "')' expected\n");
		ctx->yycur = yycur;
		return -1;
	}
	advance(ctx, 1);
	ch = currChar(ctx);
	if (ch == '(') {
		advance(ctx, 1);
		ch = currChar(ctx);
		cnt++;
		(*retc)++;
		while (ch != ')' && ch && !NL(ch)) {
			if (ch == ',') {
				cnt++;
				(*retc)++;
			}
			nextChar(ctx);
			ch = currChar(ctx);
		}
		if (ch != ')') {
			parseError(ma, ctx, "')' expected\n");
			ctx->yycur = yycur;
			return -1;
		}
	} else {
		cnt++;
		(*retc)++;
	}
	ctx->yycur = yycur;
	return cnt;
}

static void
mf_destroy(mel_func *f)
{
	if (f) {
		if (f->args)
			GDKfree(f->args);
		GDKfree(f);
	}
}

static int
argument(allocator *ma, Client ctx, mel_func *curFunc, mel_arg *curArg)
{
	malType type;

	int l = idLength(ctx);
	*curArg = (mel_arg){ .isbat = 0 };
	if (l > 0) {
		char *varname = CURRENT(ctx);
		(void)varname; /* not used */

		advance(ctx, l);
		type = typeElm(ma, ctx, TYPE_any);
		if (type < 0)
			return -1;
		int tt = getBatType(type);
		if (tt != TYPE_any)
            strcpy(curArg->type, BATatoms[tt].name);
		if (isaBatType(type))
			curArg->isbat = true;
		if (isPolymorphic(type)) {
			curArg->nr = getTypeIndex(type);
			setPoly(curFunc, type);
			tt = TYPE_any;
		}
		curArg->typeid = tt;
	} else if (currChar(ctx) == ':') {
		type = typeElm(ma, ctx, TYPE_any);
		int tt = getBatType(type);
		if (tt != TYPE_any)
            strcpy(curArg->type, BATatoms[tt].name);
		if (isaBatType(type))
			curArg->isbat = true;
		if (isPolymorphic(type)) {
			curArg->nr = getTypeIndex(type);
			setPoly(curFunc, type);
			tt = TYPE_any;
		}
		curArg->typeid = tt;
	} else {
		parseError(ma, ctx, "argument expected\n");
		return -1;
	}
	return 0;
}

static mel_func *
fcnCommandPatternHeader(allocator *ma, Client ctx, int kind)
{
	int l;
	malType tpe;
	const char *fnme;
	const char *modnme = NULL;
	char ch;

	l = operatorLength(ctx);
	if (l == 0)
		l = idLength(ctx);
	if (l == 0) {
		parseError(ma, ctx, "<identifier> | <operator> expected\n");
		return NULL;
	}

	fnme = putNameLen(((char *) CURRENT(ctx)), l);
	if (fnme == NULL) {
		parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return NULL;
	}
	advance(ctx, l);

	if (currChar(ctx) == '.') {
		nextChar(ctx);		/* skip '.' */
		modnme = fnme;
		if (modnme != userRef && getModule(modnme) == NULL) {
			if (globalModule(modnme) == NULL) {
				parseError(ma, ctx, "<module> name not defined\n");
				return NULL;
			}
		}
		l = operatorLength(ctx);
		if (l == 0)
			l = idLength(ctx);
		if (l == 0) {
			parseError(ma, ctx, "<identifier> | <operator> expected\n");
			return NULL;
		}
		fnme = putNameLen(((char *) CURRENT(ctx)), l);
		if (fnme == NULL) {
			parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return NULL;
		}
		advance(ctx, l);
	} else
		modnme = ctx->curmodule->name;

	if (currChar(ctx) != '(') {
		parseError(ma, ctx, "function header '(' expected\n");
		return NULL;
	}
	advance(ctx, 1);

	/* keep current prg also active ! */
	int retc = 0, nargs = cntArgsReturns(ma, ctx, &retc);
	if (nargs < 0)
		return 0;

	/* one extra for argument/return manipulation */
	assert(kind == COMMANDsymbol || kind == PATTERNsymbol);

	mel_func *curFunc = (mel_func*)GDKmalloc(sizeof(mel_func));
	if (curFunc)
		curFunc->args = NULL;
	if (curFunc && nargs)
		curFunc->args = (mel_arg*)GDKmalloc(sizeof(mel_arg)*nargs);

	if (ctx->curprg == NULL || ctx->curprg->def->errors || curFunc == NULL || (nargs && curFunc->args == NULL)) {
		mf_destroy(curFunc);
		parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return NULL;
	}

	curFunc->fcn = fnme;
	curFunc->mod = modnme;
	curFunc->cname = NULL;
	curFunc->command = false;
	if (kind == COMMANDsymbol)
		curFunc->command = true;
	curFunc->unsafe = 0;
	curFunc->vargs = 0;
	curFunc->vrets = 0;
	curFunc->poly = 0;
	curFunc->retc = retc;
	curFunc->argc = nargs;
	curFunc->comment = NULL;

	/* get calling parameters */
	ch = currChar(ctx);
	int i = retc;
	while (ch != ')' && ch && !NL(ch)) {
		if (argument(ma, ctx, curFunc, curFunc->args+i) < 0) {
			mf_destroy(curFunc);
			return NULL;
		}
		/* the last argument may be variable length */
		if (MALkeyword(ctx, "...", 3)) {
			curFunc->vargs = true;
			setPoly(curFunc, TYPE_any);
			break;
		}
		if ((ch = currChar(ctx)) != ',') {
			if (ch == ')')
				break;
			mf_destroy(curFunc);
			parseError(ma, ctx, "',' expected\n");
			return NULL;
		} else {
			nextChar(ctx);	/* skip ',' */
			i++;
		}
		skipSpace(ctx);
		ch = currChar(ctx);
	}
	if (currChar(ctx) != ')') {
		mf_destroy(curFunc);
		parseError(ma, ctx, "')' expected\n");
		return NULL;
	}
	advance(ctx, 1);			/* skip ')' */
/*
   The return type is either a single type or multiple return type structure.
   We simply keep track of the number of arguments added and
   during the final phase reshuffle the return values to the beginning (?)
 */
	if (currChar(ctx) == ':') {
		tpe = typeElm(ma, ctx, TYPE_void);
		curFunc->args[0].vargs = 0;
		curFunc->args[0].nr = 0;
		if (isPolymorphic(tpe)) {
			curFunc->args[0].nr = getTypeIndex(tpe);
			setPoly(curFunc, tpe);
		}
		if (isaBatType(tpe))
			curFunc->args[0].isbat = true;
		else
			curFunc->args[0].isbat = false;
		int tt = getBatType(tpe);
		curFunc->args[0].typeid = tt;
		curFunc->args[0].opt = 0;
		/* we may be confronted by a variable target type list */
		if (MALkeyword(ctx, "...", 3)) {
			curFunc->args[0].vargs = true;
			curFunc->vrets = true;
			setPoly(curFunc, TYPE_any);
		}
	} else if (keyphrase1(ctx, "(")) {	/* deal with compound return */
		int i = 0;
		/* parse multi-target result */
		/* skipSpace(ctx); */
		ch = currChar(ctx);
		while (ch != ')' && ch && !NL(ch)) {
			if (argument(ma, ctx, curFunc, curFunc->args+i) < 0) {
				mf_destroy(curFunc);
				return NULL;
			}
			/* we may be confronted by a variable target type list */
			if (MALkeyword(ctx, "...", 3)) {
				curFunc->args[i].vargs = true;
				curFunc->vrets = true;
				setPoly(curFunc, TYPE_any);
			}
			if ((ch = currChar(ctx)) != ',') {
				if (ch == ')')
					break;
				parseError(ma, ctx, "',' expected\n");
				return curFunc;
			} else {
				nextChar(ctx);	/* skip ',' */
				i++;
			}
			skipSpace(ctx);
			ch = currChar(ctx);
		}
		if (currChar(ctx) != ')') {
			mf_destroy(curFunc);
			parseError(ma, ctx, "')' expected\n");
			return NULL;
		}
		nextChar(ctx);		/* skip ')' */
	}
	return curFunc;
}

static Symbol
parseCommandPattern(allocator *ma, Client ctx, int kind, MALfcn address)
{
	mel_func *curFunc = fcnCommandPatternHeader(ma, ctx, kind);
	if (curFunc == NULL) {
		ctx->blkmode = 0;
		return NULL;
	}
	const char *modnme = curFunc->mod;
	if (modnme && (getModule(modnme) == FALSE && modnme != userRef)) {
		// introduce the module
		if (globalModule(modnme) == NULL) {
			mf_destroy(curFunc);
			parseError(ma, ctx, "<module> could not be defined\n");
			return NULL;
		}
	}
	modnme = modnme ? modnme : ctx->usermodule->name;

	size_t l = strlen(modnme);
	modnme = putNameLen(modnme, l);
	if (modnme == NULL) {
		parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return NULL;
	}

	Symbol curPrg = newFunctionArgs(modnme, curFunc->fcn, kind, -1);
	if (!curPrg) {
		mf_destroy(curFunc);
		parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return NULL;
	}
	curPrg->func = curFunc;
	curPrg->def = NULL;
	curPrg->allocated = true;

	skipSpace(ctx);
	if (MALkeyword(ctx, "address", 7)) {
		int i;
		i = idLength(ctx);
		if (i == 0) {
			parseError(ma, ctx, "address <identifier> expected\n");
			return NULL;
		}
		ctx->blkmode = 0;

		size_t sz = (size_t) (i < IDLENGTH ? i : IDLENGTH - 1);
		curFunc->cname = GDKmalloc(sz+1);
		if (!curFunc->cname) {
			parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			freeSymbol(curPrg);
			return NULL;
		}
		memcpy((char*)curFunc->cname, CURRENT(ctx), sz);
		((char*)curFunc->cname)[sz] = 0;
		/* avoid a clash with old temporaries */
		advance(ctx, i);
		curFunc->imp = getAddress(curFunc->mod, curFunc->cname);

		if (ctx->usermodule->isAtomModule) {
			if (curFunc->imp == NULL) {
				parseError(ma, ctx, "<address> not found\n");
				freeSymbol(curPrg);
				return NULL;
			}
			malAtomProperty(curFunc);
		}
		skipSpace(ctx);
	} else if (address) {
		curFunc->mod = modnme;
		curFunc->imp = address;
	}
	if (modnme == userRef) {
		insertSymbol(ctx->usermodule, curPrg);
	} else if (getModule(modnme)) {
		insertSymbol(getModule(modnme), curPrg);
	} else {
		freeSymbol(curPrg);
		parseError(ma, ctx, "<module> not found\n");
		return NULL;
	}

	char *comment = NULL;
	helpInfo(ma, ctx, NULL, &comment);
	curFunc->comment = comment;
	return curPrg;
}

static MalBlkPtr
fcnHeader(allocator *ma, Client ctx, int kind)
{
	int l;
	malType tpe;
	const char *fnme;
	const char *modnme = NULL;
	char ch;
	Symbol curPrg;
	MalBlkPtr curBlk = 0;
	InstrPtr curInstr;

	l = operatorLength(ctx);
	if (l == 0)
		l = idLength(ctx);
	if (l == 0) {
		parseError(ma, ctx, "<identifier> | <operator> expected\n");
		return 0;
	}

	fnme = putNameLen(((char *) CURRENT(ctx)), l);
	if (fnme == NULL) {
		parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return NULL;
	}
	advance(ctx, l);

	if (currChar(ctx) == '.') {
		nextChar(ctx);		/* skip '.' */
		modnme = fnme;
		if (modnme != userRef && getModule(modnme) == NULL) {
			if (globalModule(modnme) == NULL) {
				parseError(ma, ctx, "<module> name not defined\n");
				return 0;
			}
		}
		l = operatorLength(ctx);
		if (l == 0)
			l = idLength(ctx);
		if (l == 0) {
			parseError(ma, ctx, "<identifier> | <operator> expected\n");
			return 0;
		}
		fnme = putNameLen(((char *) CURRENT(ctx)), l);
		if (fnme == NULL) {
			parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return NULL;
		}
		advance(ctx, l);
	} else
		modnme = ctx->curmodule->name;

	/* temporary suspend capturing statements in main block */
	if (ctx->backup) {
		parseError(ma, ctx, "mal_parser: unexpected recursion\n");
		return 0;
	}
	if (currChar(ctx) != '(') {
		parseError(ma, ctx, "function header '(' expected\n");
		return curBlk;
	}
	advance(ctx, 1);

	assert(!ctx->backup);
	ctx->backup = ctx->curprg;
	int retc = 0, nargs = cntArgsReturns(ma, ctx, &retc);
	(void)retc;
	if (nargs < 0)
		return 0;
	/* one extra for argument/return manipulation */
	ctx->curprg = newFunctionArgs(modnme, fnme, kind, nargs + 1);
	if (ctx->curprg == NULL) {
		/* reinstate curprg to have a place for the error */
		ctx->curprg = ctx->backup;
		ctx->backup = NULL;
		parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return 0;
	}
	ctx->curprg->def->errors = ctx->backup->def->errors;
	ctx->backup->def->errors = 0;
	curPrg = ctx->curprg;
	curBlk = curPrg->def;
	curInstr = getInstrPtr(curBlk, 0);

	/* get calling parameters */
	ch = currChar(ctx);
	while (ch != ')' && ch && !NL(ch)) {
		curInstr = binding(ma, ctx, curBlk, curInstr, 1);
		/* the last argument may be variable length */
		if (MALkeyword(ctx, "...", 3)) {
			curInstr->varargs |= VARARGS;
			setPolymorphic(curInstr, TYPE_any, TRUE);
			break;
		}
		if ((ch = currChar(ctx)) != ',') {
			if (ch == ')')
				break;
			if (ctx->backup)
				curBlk = NULL;
			parseError(ma, ctx, "',' expected\n");
			return curBlk;
		} else
			nextChar(ctx);	/* skip ',' */
		skipSpace(ctx);
		ch = currChar(ctx);
	}
	if (currChar(ctx) != ')') {
		freeInstruction(curBlk, curInstr);
		if (ctx->backup)
			curBlk = NULL;
		parseError(ma, ctx, "')' expected\n");
		return curBlk;
	}
	advance(ctx, 1);			/* skip ')' */
/*
   The return type is either a single type or multiple return type structure.
   We simply keep track of the number of arguments added and
   during the final phase reshuffle the return values to the beginning (?)
 */
	if (currChar(ctx) == ':') {
		tpe = typeElm(ma, ctx, TYPE_void);
		setPolymorphic(curInstr, tpe, TRUE);
		setVarType(curBlk, curInstr->argv[0], tpe);
		/* we may be confronted by a variable target type list */
		if (MALkeyword(ctx, "...", 3)) {
			curInstr->varargs |= VARRETS;
			setPolymorphic(curInstr, TYPE_any, TRUE);
		}

	} else if (keyphrase1(ctx, "(")) {	/* deal with compound return */
		int retc = curInstr->argc, i1, i2 = 0;
		int max;
		short *newarg;
		/* parse multi-target result */
		/* skipSpace(ctx); */
		ch = currChar(ctx);
		while (ch != ')' && ch && !NL(ch)) {
			curInstr = binding(ma, ctx, curBlk, curInstr, 0);
			/* we may be confronted by a variable target type list */
			if (MALkeyword(ctx, "...", 3)) {
				curInstr->varargs |= VARRETS;
				setPolymorphic(curInstr, TYPE_any, TRUE);
			}
			if ((ch = currChar(ctx)) != ',') {
				if (ch == ')')
					break;
				if (ctx->backup)
					curBlk = NULL;
				parseError(ma, ctx, "',' expected\n");
				return curBlk;
			} else {
				nextChar(ctx);	/* skip ',' */
			}
			skipSpace(ctx);
			ch = currChar(ctx);
		}
		/* re-arrange the parameters, results first */
		max = curInstr->maxarg;
		newarg = (short *) GDKmalloc(max * sizeof(curInstr->argv[0]));
		if (newarg == NULL) {
			parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			if (ctx->backup)
				curBlk = NULL;
			return curBlk;
		}
		for (i1 = retc; i1 < curInstr->argc; i1++)
			newarg[i2++] = curInstr->argv[i1];
		curInstr->retc = curInstr->argc - retc;
		for (i1 = 1; i1 < retc; i1++)
			newarg[i2++] = curInstr->argv[i1];
		curInstr->argc = i2;
		for (; i2 < max; i2++)
			newarg[i2] = 0;
		for (i1 = 0; i1 < max; i1++)
			curInstr->argv[i1] = newarg[i1];
		GDKfree(newarg);
		if (currChar(ctx) != ')') {
			freeInstruction(curBlk, curInstr);
			if (ctx->backup)
				curBlk = NULL;
			parseError(ma, ctx, "')' expected\n");
			return curBlk;
		}
		nextChar(ctx);		/* skip ')' */
	} else {					/* default */
		setVarType(curBlk, 0, TYPE_void);
	}
	if (curInstr != getInstrPtr(curBlk, 0)) {
		freeInstruction(curBlk, getInstrPtr(curBlk, 0));
		putInstrPtr(curBlk, 0, curInstr);
	}
	return curBlk;
}

static MalBlkPtr
parseFunction(allocator *ma, Client ctx, int kind)
{
	MalBlkPtr curBlk = 0;

	curBlk = fcnHeader(ma, ctx, kind);
	if (curBlk == NULL)
		return curBlk;
	if (MALkeyword(ctx, "address", 7)) {
		/* TO BE DEPRECATED */
		str nme;
		int i;
		InstrPtr curInstr = getInstrPtr(curBlk, 0);
		i = idLength(ctx);
		if (i == 0) {
			parseError(ma, ctx, "<identifier> expected\n");
			return 0;
		}
		nme = idCopy(ma, ctx, i);
		if (nme == NULL) {
			parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return 0;
		}
		curInstr->fcn = getAddress(getModuleId(curInstr), nme);
		//GDKfree(nme);
		if (curInstr->fcn == NULL) {
			parseError(ma, ctx, "<address> not found\n");
			return 0;
		}
		skipSpace(ctx);
	}
	/* block is terminated at the END statement */
	helpInfo(ma, ctx, curBlk->ma, &curBlk->help);
	return curBlk;
}

/*
 * Functions and  factories end with a labeled end-statement.
 * The routine below checks for misalignment of the closing statements.
 * Any instruction parsed after the function block is considered an error.
 */
static int
parseEnd(allocator *ma, Client ctx)
{
	Symbol curPrg = 0;
	size_t l;
	InstrPtr sig;
	str errors = MAL_SUCCEED, msg = MAL_SUCCEED;

	if (MALkeyword(ctx, "end", 3)) {
		curPrg = ctx->curprg;
		l = idLength(ctx);
		if (l == 0)
			l = operatorLength(ctx);
		sig = getInstrPtr(ctx->curprg->def, 0);
		if (strncmp(CURRENT(ctx), getModuleId(sig), l) == 0) {
			advance(ctx, l);
			skipSpace(ctx);
			if (currChar(ctx) == '.')
				nextChar(ctx);
			skipSpace(ctx);
			l = idLength(ctx);
			if (l == 0)
				l = operatorLength(ctx);
		}
		/* parse fcn */
		if ((l == strlen(curPrg->name) &&
			 strncmp(CURRENT(ctx), curPrg->name, l) == 0) || l == 0)
			advance(ctx, l);
		else
			parseError(ma, ctx, "non matching end label\n");
		pushEndInstruction(ctx->curprg->def);
		ctx->blkmode = 0;
		if (getModuleId(sig) == userRef)
			insertSymbol(ctx->usermodule, ctx->curprg);
		else
			insertSymbol(getModule(getModuleId(sig)), ctx->curprg);

		if (ctx->curprg->def->errors) {
			errors = ctx->curprg->def->errors;
			ctx->curprg->def->errors = 0;
		}
		// check for newly identified errors
		msg = chkProgram(ctx->usermodule, ctx->curprg->def);
		if (errors == NULL)
			errors = msg;
		else
			freeException(msg);
		if (errors == NULL) {
			errors = ctx->curprg->def->errors;
			ctx->curprg->def->errors = 0;
		} else if (ctx->curprg->def->errors) {
			//collect all errors for reporting
			str new = GDKmalloc(strlen(errors) +
								strlen(ctx->curprg->def->errors) + 16);
			if (new) {
				char *p = stpcpy(new, errors);
				if (p[-1] != '\n')
					*p++ = '\n';
				*p++ = '!';
				strcpy(p, ctx->curprg->def->errors);

				freeException(errors);
				freeException(ctx->curprg->def->errors);

				ctx->curprg->def->errors = 0;
				errors = new;
			}
		}

		if (ctx->backup) {
			ctx->curprg = ctx->backup;
			ctx->backup = 0;
		} else {
			str msg;
			if ((msg = MSinitClientPrg(ctx, ctx->curmodule->name,
									   mainRef)) != MAL_SUCCEED) {
				if (errors) {
					str new = GDKmalloc(strlen(errors) + strlen(msg) + 3);
					if (new) {
						char *p = stpcpy(new, msg);
						if (p[-1] != '\n')
							*p++ = '\n';
						strcpy(p, errors);
						freeException(errors);
						ctx->curprg->def->errors = new;
					} else {
						ctx->curprg->def->errors = errors;
					}
					freeException(msg);
				} else {
					ctx->curprg->def->errors = msg;
				}
				return 1;
			}
		}
		// pass collected errors to context
		assert(ctx->curprg->def->errors == NULL);
		ctx->curprg->def->errors = errors;
		return 1;
	}
	return 0;
}

/*
 * Most instructions are simple assignments, possibly
 * modified with a barrier/catch tag.
 *
 * The basic types are also predefined as a variable.
 * This makes it easier to communicate types to MAL patterns.
 */

#define GETvariable(FREE)												\
	if ((varid = findVariableLength(curBlk, CURRENT(ctx), l)) == -1) { \
		varid = newVariable(curBlk, CURRENT(ctx), l, TYPE_any);		\
		advance(ctx, l);												\
		if (varid <  0) { FREE; return; }								\
	} else																\
		advance(ctx, l);

/* The parameter of parseArguments is the return value of the enclosing function. */
static int
parseArguments(allocator *ma, Client ctx, MalBlkPtr curBlk, InstrPtr *curInstr)
{
	while (currChar(ctx) != ')') {
		switch (term(ma, ctx, curBlk, curInstr, 0)) {
		case 0:
			break;
		case 2:
			return 2;
		case 3:
			return 3;
		case 4:
			parseError(ma, ctx, "Argument type overwrites previous definition\n");
			return 0;
		default:
			parseError(ma, ctx, "<factor> expected\n");
			return 1;
		}
		if (currChar(ctx) == ',')
			advance(ctx, 1);
		else if (currChar(ctx) != ')') {
			parseError(ma, ctx, "',' expected\n");
			ctx->yycur--;		/* keep it */
			break;
		}
	}
	if (currChar(ctx) == ')')
		advance(ctx, 1);
	return 0;
}

static void
parseAssign(allocator *ma, Client ctx, int cntrl)
{
	InstrPtr curInstr;
	MalBlkPtr curBlk;
	Symbol curPrg;
	int i = 0, l, type = TYPE_any, varid = -1;
	const char *arg = 0;
	ValRecord cst;

	curPrg = ctx->curprg;
	curBlk = curPrg->def;
	if ((curInstr = newInstruction(curBlk, NULL, NULL)) == NULL) {
		parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return;
	}

	if (cntrl) {
		curInstr->token = ASSIGNsymbol;
		curInstr->barrier = cntrl;
	}

	/* start the parsing by recognition of the lhs of an assignment */
	if (currChar(ctx) == '(') {
		/* parsing multi-assignment */
		advance(ctx, 1);
		curInstr->argc = 0;		/*reset to handle pushArg correctly !! */
		curInstr->retc = 0;
		while (currChar(ctx) != ')' && currChar(ctx)) {
			l = idLength(ctx);
			i = cstToken(ma, ctx, curBlk, &cst);
			if (l == 0 || i) {
				freeInstruction(curBlk, curInstr);
				parseError(ma, ctx, "<identifier> or <literal> expected\n");
				return;
			}
			GETvariable(freeInstruction(curBlk, curInstr));
			if (currChar(ctx) == ':') {
				type = typeElm(ma, ctx, getVarType(curBlk, varid));
				if (type < 0)
					goto part3;
				setPolymorphic(curInstr, type, FALSE);
				setVarType(curBlk, varid, type);
			}
			curInstr = pushArgument(curBlk, curInstr, varid);
			curInstr->retc++;
			if (currChar(ctx) == ')')
				break;
			if (currChar(ctx) == ',')
				keyphrase1(ctx, ",");
		}
		advance(ctx, 1);		/* skip ')' */
		if (curInstr->retc == 0) {
			/* add dummy variable */
			curInstr = pushArgument(curBlk, curInstr,
									newTmpVariable(curBlk, TYPE_any));
			curInstr->retc++;
		}
	} else {
		/* are we dealing with a simple assignment? */
		l = idLength(ctx);
		i = cstToken(ma, ctx, curBlk, &cst);
		if (l == 0 || i) {
			/* we haven't seen a target variable */
			/* flow of control statements may end here. */
			/* shouldn't allow for nameless controls todo */
			if (i && cst.vtype == TYPE_str)
				GDKfree(cst.val.sval);
			if (cntrl == LEAVEsymbol || cntrl == REDOsymbol ||
				cntrl == RETURNsymbol || cntrl == EXITsymbol) {
				curInstr->argv[0] = getBarrierEnvelop(curBlk);
				if (currChar(ctx) != ';') {
					freeInstruction(curBlk, curInstr);
					parseError(ma, ctx,
							   "<identifier> or <literal> expected in control statement\n");
					return;
				}
				pushInstruction(curBlk, curInstr);
				return;
			}
			getArg(curInstr, 0) = newTmpVariable(curBlk, TYPE_any);
			freeInstruction(curBlk, curInstr);
			parseError(ma, ctx, "<identifier> or <literal> expected\n");
			return;
		}
		/* Check if we are dealing with module.fcn call */
		if (CURRENT(ctx)[l] == '.' || CURRENT(ctx)[l] == '(') {
			curInstr->argv[0] = newTmpVariable(curBlk, TYPE_any);
			goto FCNcallparse;
		}

		/* Get target variable details */
		GETvariable(freeInstruction(curBlk, curInstr));
		if (!(currChar(ctx) == ':' && CURRENT(ctx)[1] == '=')) {
			curInstr->argv[0] = varid;
			if (currChar(ctx) == ':') {
				type = typeElm(ma, ctx, getVarType(curBlk, varid));
				if (type < 0)
					goto part3;
				setPolymorphic(curInstr, type, FALSE);
				setVarType(curBlk, varid, type);
			}
		}
		curInstr->argv[0] = varid;
	}
	/* look for assignment operator */
	if (!keyphrase2(ctx, ":=")) {
		/* no assignment !! a control variable is allowed */
		/* for the case RETURN X, we normalize it to include the function arguments */
		if (cntrl == RETURNsymbol) {
			int e;
			InstrPtr sig = getInstrPtr(curBlk, 0);
			curInstr->retc = 0;
			for (e = 0; e < sig->retc; e++)
				curInstr = pushReturn(curBlk, curInstr, getArg(sig, e));
		}

		goto part3;
	}
	if (currChar(ctx) == '(') {
		/* parse multi assignment */
		advance(ctx, 1);
		switch (parseArguments(ma, ctx, curBlk, &curInstr)) {
		case 2:
			goto part2;
		default:
		case 3:
			goto part3;
		}
		/* unreachable */
	}
/*
 * We have so far the LHS part of an assignment. The remainder is
 * either a simple term expression, a multi assignent, or the start
 * of a function call.
 */
  FCNcallparse:
	if ((l = idLength(ctx)) && CURRENT(ctx)[l] == '(') {
		/*  parseError(ma, ctx,"<module> expected\n"); */
		setModuleId(curInstr, ctx->curmodule->name);
		i = l;
		goto FCNcallparse2;
	} else if ((l = idLength(ctx)) && CURRENT(ctx)[l] == '.') {
		/* continue with parsing a function/operator call */
		arg = putNameLen(CURRENT(ctx), l);
		if (arg == NULL) {
			freeInstruction(curBlk, curInstr);
			parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return;
		}
		advance(ctx, l + 1);	/* skip '.' too */
		setModuleId(curInstr, arg);
		i = idLength(ctx);
		if (i == 0)
			i = operatorLength(ctx);
  FCNcallparse2:
		if (i) {
			setFunctionId(curInstr, putNameLen(((char *) CURRENT(ctx)), i));
			if (getFunctionId(curInstr) == NULL) {
				freeInstruction(curBlk, curInstr);
				parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				return;
			}
			advance(ctx, i);
		} else {
			freeInstruction(curBlk, curInstr);
			parseError(ma, ctx, "<functionname> expected\n");
			return;
		}
		skipSpace(ctx);
		if (currChar(ctx) != '(') {
			freeInstruction(curBlk, curInstr);
			parseError(ma, ctx, "'(' expected\n");
			return;
		}
		advance(ctx, 1);
		switch (parseArguments(ma, ctx, curBlk, &curInstr)) {
		case 2:
			goto part2;
		default:
		case 3:
			goto part3;
		}
		/* unreachable */
	}
	/* Handle the ordinary assignments and expressions */
	switch (term(ma, ctx, curBlk, &curInstr, 2)) {
	case 2:
		goto part2;
	case 3:
		goto part3;
	}
  part2:						/* consume <operator><term> part of expression */
	if ((i = operatorLength(ctx))) {
		/* simple arithmetic operator expression */
		setFunctionId(curInstr, putNameLen(((char *) CURRENT(ctx)), i));
		if (getFunctionId(curInstr) == NULL) {
			freeInstruction(curBlk, curInstr);
			parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return;
		}
		advance(ctx, i);
		curInstr->modname = calcRef;
		if (curInstr->modname == NULL) {
			freeInstruction(curBlk, curInstr);
			parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return;
		}
		if ((l = idLength(ctx))
			&& !(l == 3 && strncmp(CURRENT(ctx), "nil", 3) == 0)) {
			GETvariable(freeInstruction(curBlk, curInstr));
			curInstr = pushArgument(curBlk, curInstr, varid);
			goto part3;
		}
		switch (term(ma, ctx, curBlk, &curInstr, 3)) {
		case 2:
			goto part2;
		case 3:
			goto part3;
		}
		freeInstruction(curBlk, curInstr);
		parseError(ma, ctx, "<term> expected\n");
		return;
	} else {
		skipSpace(ctx);
		if (currChar(ctx) == '(') {
			freeInstruction(curBlk, curInstr);
			parseError(ma, ctx, "module name missing\n");
			return;
		} else if (currChar(ctx) != ';' && currChar(ctx) != '#') {
			freeInstruction(curBlk, curInstr);
			parseError(ma, ctx, "operator expected\n");
			return;
		}
		pushInstruction(curBlk, curInstr);
		return;
	}
  part3:
	skipSpace(ctx);
	if (currChar(ctx) != ';') {
		freeInstruction(curBlk, curInstr);
		parseError(ma, ctx, "';' expected\n");
		skipToEnd(ctx);
		return;
	}
	skipToEnd(ctx);
	if (cntrl == RETURNsymbol
		&& !(curInstr->token == ASSIGNsymbol || getModuleId(curInstr) != 0)) {
		freeInstruction(curBlk, curInstr);
		parseError(ma, ctx, "return assignment expected\n");
		return;
	}
	pushInstruction(curBlk, curInstr);
}

void
parseMAL(Client ctx, Symbol curPrg, int skipcomments, int lines,
		 MALfcn address)
{
	int cntrl = 0;
	/*Symbol curPrg= ctx->curprg; */
	char c;
	int inlineProp = 0, unsafeProp = 0;

	(void) curPrg;
	echoInput(ctx);
	// can we use mal block allocator always?
	allocator *ma = ctx->backup? ctx->backup->def->ma:curPrg->def->ma;
	/* here the work takes place */
	while ((c = currChar(ctx)) && lines > 0) {
		switch (c) {
		case '\n':
		case '\r':
		case '\f':
			lines -= c == '\n';
			nextChar(ctx);
			echoInput(ctx);
			continue;
		case ';':
		case '\t':
		case ' ':
			nextChar(ctx);
			continue;
		case '#':
		{						/* keep the full line comments */
			char start[256], *e = start, c;
			MalBlkPtr curBlk = ctx->curprg->def;
			InstrPtr curInstr;

			*e = 0;
			nextChar(ctx);
			while ((c = currChar(ctx))) {
				if (e < start + 256 - 1)
					*e++ = c;
				nextChar(ctx);
				if (c == '\n' || c == '\r') {
					*e = 0;
					if (e > start)
						e--;
					/* prevChar(ctx); */
					break;
				}
			}
			if (e > start)
				*e = 0;
			if (!skipcomments && e > start && curBlk->stop > 0) {
				ValRecord cst;
				if ((curInstr = newInstruction(curBlk, NULL, NULL)) == NULL) {
					parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					continue;
				}
				curInstr->token = REMsymbol;
				curInstr->barrier = 0;
				if (VALinit(curBlk->ma, &cst, TYPE_str, start) == NULL) {
					parseError(ma, ctx, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					freeInstruction(curBlk, curInstr);
					continue;
				}
				int cstidx = defConstant(curBlk, TYPE_str, &cst);
				if (cstidx < 0) {
					freeInstruction(curBlk, curInstr);
					continue;
				}
				getArg(curInstr, 0) = cstidx;
				setVarDisabled(curBlk, getArg(curInstr, 0));
				pushInstruction(curBlk, curInstr);
			}
			echoInput(ctx);
		}
			continue;
		case 'A':
		case 'a':
			if (MALkeyword(ctx, "atom", 4) && parseAtom(ma, ctx) == 0)
				break;
			goto allLeft;
		case 'b':
		case 'B':
			if (MALkeyword(ctx, "barrier", 7)) {
				ctx->blkmode++;
				cntrl = BARRIERsymbol;
			}
			goto allLeft;
		case 'C':
		case 'c':
			if (MALkeyword(ctx, "command", 7)) {
				Symbol p = parseCommandPattern(ma, ctx, COMMANDsymbol, address);
				if (p) {
					p->func->unsafe = unsafeProp;
				}
				if (inlineProp)
					parseError(ma, ctx, "<identifier> expected\n");
				inlineProp = 0;
				unsafeProp = 0;
				continue;
			}
			if (MALkeyword(ctx, "catch", 5)) {
				ctx->blkmode++;
				cntrl = CATCHsymbol;
				goto allLeft;
			}
			goto allLeft;
		case 'E':
		case 'e':
			if (MALkeyword(ctx, "exit", 4)) {
				if (ctx->blkmode > 0)
					ctx->blkmode--;
				cntrl = EXITsymbol;
			} else if (parseEnd(ma, ctx)) {
				break;
			}
			goto allLeft;
		case 'F':
		case 'f':
			if (MALkeyword(ctx, "function", 8)) {
				MalBlkPtr p;
				ctx->blkmode++;
				if ((p = parseFunction(ma, ctx, FUNCTIONsymbol))) {
					p->unsafeProp = unsafeProp;
					ctx->curprg->def->inlineProp = inlineProp;
					ctx->curprg->def->unsafeProp = unsafeProp;
					inlineProp = 0;
					unsafeProp = 0;
					break;
				}
			}
			goto allLeft;
		case 'I':
		case 'i':
			if (MALkeyword(ctx, "inline", 6)) {
				inlineProp = 1;
				skipSpace(ctx);
				continue;
			} else if (MALkeyword(ctx, "include", 7)) {
				parseInclude(ma, ctx);
				break;
			}
			goto allLeft;
		case 'L':
		case 'l':
			if (MALkeyword(ctx, "leave", 5))
				cntrl = LEAVEsymbol;
			goto allLeft;
		case 'M':
		case 'm':
			if (MALkeyword(ctx, "module", 6) && parseModule(ma, ctx) == 0)
				break;
			goto allLeft;
		case 'P':
		case 'p':
			if (MALkeyword(ctx, "pattern", 7)) {
				if (inlineProp)
					parseError(ma, ctx, "parseError:INLINE ignored\n");
				Symbol p = parseCommandPattern(ma, ctx, PATTERNsymbol, address);
				if (p) {
					p->func->unsafe = unsafeProp;
				}
				inlineProp = 0;
				unsafeProp = 0;
				continue;
			}
			goto allLeft;
		case 'R':
		case 'r':
			if (MALkeyword(ctx, "redo", 4)) {
				cntrl = REDOsymbol;
				goto allLeft;
			}
			if (MALkeyword(ctx, "raise", 5)) {
				cntrl = RAISEsymbol;
				goto allLeft;
			}
			if (MALkeyword(ctx, "return", 6)) {
				cntrl = RETURNsymbol;
			}
			goto allLeft;
		case 'U':
		case 'u':
			if (MALkeyword(ctx, "unsafe", 6)) {
				unsafeProp = 1;
				skipSpace(ctx);
				continue;
			}
			/* fall through */
		default:
  allLeft:
			parseAssign(ma, ctx, cntrl);
			cntrl = 0;
		}
	}
	skipSpace(ctx);
}

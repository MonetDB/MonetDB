/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/* (c): M. L. Kersten
*/

#include "monetdb_config.h"
#include "mal_parser.h"
#include "mal_resolve.h"
#include "mal_linker.h"
#include "mal_atom.h"       /* for malAtomDefinition(), malAtomProperty() */
#include "mal_interpreter.h"    /* for showErrors() */
#include "mal_instruction.h"    /* for pushEndInstruction(), findVariableLength() */
#include "mal_namespace.h"
#include "mal_utils.h"
#include "mal_builder.h"
#include "mal_type.h"
#include "mal_private.h"

#define FATALINPUT MAXERRORS+1
#define NL(X) ((X)=='\n' || (X)=='\r')

static str idCopy(Client cntxt, int len);
static str strCopy(Client cntxt, int len);

static str parseError(Client cntxt, str msg);

/* Before a line is parsed we check for a request to echo it.
 * This command should be executed at the beginning of a parse
 * request and each time we encounter EOL.
*/
static void 
echoInput(Client cntxt)
{
	char *c = CURRENT(cntxt);
	if (cntxt->listing == 1 && *c && !NL(*c)) {
		mnstr_printf(cntxt->fdout,"#");
		while (*c && !NL(*c)) {
			mnstr_printf(cntxt->fdout, "%c", *c++);
		}
		mnstr_printf(cntxt->fdout, "\n");
	}
}

static inline void
skipSpace(Client cntxt)
{
	char *s= &currChar(cntxt);
	for (;;) {
		switch (*s++) {
		case ' ':
		case '\t':
		case '\n':
		case '\r':
			nextChar(cntxt);
			break;
		default:
			return;
		}
	}
}

static inline void
advance(Client cntxt, int length)
{
	cntxt->yycur += length;
	skipSpace(cntxt);
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

short opCharacter[256];
short idCharacter[256];
short idCharacter2[256];

void
initParser(void)
{
	int i;

	for (i = 0; i < 256; i++) {
		idCharacter2[i] = isalpha(i) || isdigit(i);
		idCharacter[i] = isalpha(i);
	}
	for (i = 0; i < 256; i++)
		switch (i) {
		case '-': case '!': case '\\': case '$': case '%':
		case '^': case '*': case '~': case '+': case '&':
		case '|': case '<': case '>': case '=': case '/':
		case ':':
			opCharacter[i] = 1;
		}

	idCharacter[TMPMARKER] = 1;
	idCharacter2[TMPMARKER] = 1;
	idCharacter2['@'] = 1;
}

#undef isdigit
#define isdigit(X)  ((X) >= '0' && (X) <= '9')

static int
idLength(Client cntxt)
{
	str s,t;
	int len = 0;
	
	skipSpace(cntxt);
	s = CURRENT(cntxt);
	t = s;

	if (!idCharacter[(int) (*s)])
		return 0;
	/* avoid a clash with old temporaries */
	if (s[0] == TMPMARKER)
		s[0] = REFMARKER;
	/* prepare escape of temporary names */
	s++;
	while (len < IDLENGTH && idCharacter2[(int) (*s)]){
		s++;
		len++;
	}
	if( len == IDLENGTH)
		// skip remainder
		while (idCharacter2[(int) (*s)])
			s++;
	return (int) (s-t);;
}

/* Simple type identifiers can not be marked with a type variable. */
static int
typeidLength(Client cntxt)
{
	int l;
	char id[IDLENGTH], *t= id;
	str s;
	skipSpace(cntxt);
	s = CURRENT(cntxt);

	if (!idCharacter[(int) (*s)])
		return 0;
	l = 1;
	*t++ = *s++;
	while (l < IDLENGTH && (idCharacter[(int) (*s)] || isdigit(*s)) ) {
		*t++ = *s++;
		l++;
	}
	/* recognize the special type variables {any, any_<nr>} */
	if( strncmp(id, "any",3) == 0)
		return 3;
	if( strncmp(id, "any_",4) == 0)
		return 4;
	return l;
}

static str
idCopy(Client cntxt, int length)
{
	str s = GDKmalloc(length + 1);
	if (s == NULL)
		return NULL;
	memcpy(s, CURRENT(cntxt), (size_t) length);
	s[length] = 0;
	/* avoid a clash with old temporaries */
	advance(cntxt, length);
	return s;
}

static int
MALlookahead(Client cntxt, str kw, int length)
{
	int i;

	/* avoid double test or use lowercase only. */
	if (currChar(cntxt) == *kw &&
		strncmp(CURRENT(cntxt), kw, length) == 0 &&
		!idCharacter[(int) (CURRENT(cntxt)[length])] &&
		!isdigit((int) (CURRENT(cntxt)[length]))) {
		return 1;
	}
	/* check for captialized versions */
	for (i = 0; i < length; i++)
		if (tolower(CURRENT(cntxt)[i]) != kw[i])
			return 0;
	if (!idCharacter[(int) (CURRENT(cntxt)[length])] &&
		!isdigit((int) (CURRENT(cntxt)[length]))) {
		return 1;
	}
	return 0;
}

static inline int
MALkeyword(Client cntxt, str kw, int length)
{
	skipSpace(cntxt);
	if (MALlookahead(cntxt, kw, length)) {
		advance(cntxt, length);
		return 1;
	}
	return 0;
}

/*
 * Keyphrase testing is limited to a few characters only
 * (check manually). To speed this up we use a pipelined and inline macros.
*/

static inline int
keyphrase1(Client cntxt, str kw)
{
	skipSpace(cntxt);
	if (currChar(cntxt) == *kw) {
		advance(cntxt, 1);
		return 1;
	}
	return 0;
}

static inline int
keyphrase2(Client cntxt, str kw)
{
	skipSpace(cntxt);
	if (CURRENT(cntxt)[0] == kw[0] && CURRENT(cntxt)[1] == kw[1]) {
		advance(cntxt, 2);
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
stringLength(Client cntxt)
{
	int l = 0;
	int quote = 0;
	str s;
	skipSpace(cntxt);
	s = CURRENT(cntxt);

	if (*s != '"')
		return 0;
	for (s++;*s; l++, s++) {
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

str
strCopy(Client cntxt, int length)
{
	str s;
	int i;

	i = length < 4 ? 4 : length;
	s = GDKmalloc(i);
	if (s == 0)
		return NULL;
	memcpy(s, CURRENT(cntxt) + 1, (size_t) (length - 2));
	s[length-2] = 0;
	mal_unquote(s);
	return s;
}

/*
 * And a similar approach is used for operator names.
 * A lookup table is considered, because it generally is
 * faster then a non-dense switch.
*/
static int
operatorLength(Client cntxt)
{
	int l = 0;
	str s;

	skipSpace(cntxt);
	for (s = CURRENT(cntxt); *s; s++) {
		if (opCharacter[(int) (*s)])
			l++;
		else
			return l;
	}
	return l;
}

/*
 * For error reporting we may have to find the start of the previous line,
 * which, ofcourse, is easy given the client buffer.
 * The remaining functions are self-explanatory.
*/
static str
lastline(Client cntxt)
{
	str s = CURRENT(cntxt);
	if (NL(*s))
		s++;
	while (s && s > cntxt->fdin->buf && !NL(*s))
		s--;
	if (NL(*s))
		s++;
	return s;
}

static ssize_t
position(Client cntxt)
{
	str s = lastline(cntxt);
	return (ssize_t) (CURRENT(cntxt) - s);
}

/*
 * Upon encountering an error we skip to the nearest semicolon,
 * or comment terminated by a new line
 */
static inline void
skipToEnd(Client cntxt)
{
	char c;
	while ((c = *CURRENT(cntxt)) != ';' && c && c != '\n')
		nextChar(cntxt);
	if (c && c != '\n')
		nextChar(cntxt);
}

/*
 * The lexical analyser for constants is a little more complex.
 * Aside from getting its length, we need an indication of its type.
 * The constant structure is initialized for later use.
 */
static int
cstToken(Client cntxt, ValPtr cst)
{
	int i = 0;
	int hex = 0;
	str s = CURRENT(cntxt);

	cst->vtype = TYPE_int;
	cst->val.lval = 0;
	switch (*s) {
	case '{': case '[':
		/* JSON Literal */
		break;
	case '"':
		cst->vtype = TYPE_str;
		i = stringLength(cntxt);
		cst->val.sval = strCopy(cntxt, i);
		if (cst->val.sval)
			cst->len = (int) strlen(cst->val.sval);
		else
			cst->len = 0;
		return i;
	case '-':
		i++;
		s++;
		/* fall through */
	case '0':
		if ((s[1] == 'x' || s[1] == 'X')) {
			/* deal with hex */
			hex = TRUE;
			i += 2;
			s += 2;
		}
		/* fall through */
	case '1': case '2': case '3': case '4': case '5':
	case '6': case '7': case '8': case '9':
		if (hex)
			while (isdigit((int) *s) || isalpha((int) *s)) {
				if (!((tolower(*s) >= 'a' && tolower(*s) <= 'f')
					  || isdigit((int) *s)))
					break;
				i++;
				s++;
			}
		else
			while (isdigit((int) *s)) {
				i++;
				s++;
			}

		if (hex)
			goto handleInts;
		/* fall through */
	case '.':
		if (*s == '.' && isdigit(*(s + 1))) {
			i++;
			s++;
			while (isdigit(*s)) {
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
			while (isdigit(*s)) {
				i++;
				s++;
			}
		}
		if (cst->vtype == TYPE_flt) {
			int len = i;
			float *pval = 0;
			fltFromStr(CURRENT(cntxt), &len, &pval);
			if (pval) {
				cst->val.fval = *pval;
				GDKfree(pval);
			} else
				cst->val.fval = 0;
		}
		if (cst->vtype == TYPE_dbl) {
			int len = i;
			double *pval = 0;
			dblFromStr(CURRENT(cntxt), &len, &pval);
			if (pval) {
				cst->val.dval = *pval;
				GDKfree(pval);
			} else
				cst->val.dval = 0;
		}
		if (*s == '@') {
			int len = (int) sizeof(lng);
			lng l, *pval = &l;
			lngFromStr(CURRENT(cntxt), &len, &pval);
			if (l == lng_nil || l < 0
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
			while (isdigit(*s)) {
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
				int len = i;
				double *pval = 0;
				dblFromStr(CURRENT(cntxt), &len, &pval);
				if (pval) {
					cst->val.dval = *pval;
					GDKfree(pval);
				} else
					cst->val.dval = 0;
			} else {
				int len = i;
				lng *pval = 0;
				lngFromStr(CURRENT(cntxt), &len, &pval);
				if (pval) {
					cst->val.lval = *pval;
					GDKfree(pval);
				} else
					cst->val.lval = 0;
			}
			return i;
		}
#ifdef HAVE_HGE
		if (*s == 'H' && cst->vtype == TYPE_int) {
			int len = i;
			hge *pval = 0;
			cst->vtype = TYPE_hge;
			i++;
			s++;
			if (*s == 'H') {
				i++;
				s++;
			}
			hgeFromStr(CURRENT(cntxt), &len, &pval);
			if (pval) {
				cst->val.hval = *pval;
				GDKfree(pval);
			} else
				cst->val.hval = 0;
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
			int len = (int) sizeof(hge);
			hge l, *pval = &l;
			if (hgeFromStr(CURRENT(cntxt), &len, &pval) <= 0 || l == hge_nil)
				l = hge_nil;

			if ((hge) GDK_int_min < l && l <= (hge) GDK_int_max) {
				cst->vtype = TYPE_int;
				cst->val.ival = (int) l;
			} else
			if ((hge) GDK_lng_min < l && l <= (hge) GDK_lng_max) {
				cst->vtype = TYPE_lng;
				cst->val.lval = (lng) l;
			} else {
				cst->vtype = TYPE_hge;
				cst->val.hval = l;
				if (l == hge_nil)
					showException(cntxt->fdout, SYNTAX, "convertConstant", "integer parse error");
			}
#else
			int len = (int) sizeof(lng);
			lng l, *pval = &l;
			if (lngFromStr(CURRENT(cntxt), &len, &pval) <= 0 || l == lng_nil)
				l = lng_nil;

			if ((lng) GDK_int_min < l && l <= (lng) GDK_int_max) {
				cst->vtype = TYPE_int;
				cst->val.ival = (int) l;
			} else {
				cst->vtype = TYPE_lng;
				cst->val.lval = l;
				if (l == lng_nil)
					showException(cntxt->fdout, SYNTAX, "convertConstant", "integer parse error");
			}
#endif
		}
		return i;

	case 'f':
		if (strncmp(s, "false", 5) == 0 && !isalnum((int) *(s + 5)) &&
			*(s + 5) != '_') {
			cst->vtype = TYPE_bit;
			cst->val.btval = 0;
			cst->len = 1;
			return 5;
		}
		return 0;
	case 't':
		if (strncmp(s, "true", 4) == 0 && !isalnum((int) *(s + 4)) &&
			*(s + 4) != '_') {
			cst->vtype = TYPE_bit;
			cst->val.btval = 1;
			cst->len = 1;
			return 4;
		}
		return 0;
	case 'n':
		if (strncmp(s, "nil", 3) == 0 && !isalnum((int) *(s + 3)) &&
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
 * They may be extended with a property list
 * and 'any' types can be marked with an alias.
 * The type qualifier parser returns the encoded type
 * as a short 32-bit integer.
 * The syntax structure is
 *
 * @multitable @columnfractions 0.15 0.8
 * @item typeQualifier
 * @tab : typeName propQualifier
 * @item typeName
 * @tab : scalarType | collectionType | anyType
 * @item scalarType
 * @tab :  ':' @sc{ identifier}
 * @item collectionType
 * @tab :  ':' @sc{ bat} ['['  col ']']
 * @item anyType
 * @tab :  ':' @sc{ any} [typeAlias]
 * @item col
 * @tab :  scalarType | anyType
 * @end multitable
 *
 * The type ANY matches any type specifier.
 * Appending it with an alias turns it into a type variable.
 * The type alias is \$DIGIT (1-9) and can be used to relate types
 * by type equality.
 * The type variable are defined within the context of a function
 * scope.
 * Additional information, such as a repetition factor,
 * encoding tables, or type dependency should be modeled as properties.
 */
static int
typeAlias(Client cntxt, int tpe)
{
	int t;

	if (tpe != TYPE_any)
		return -1;
	if (currChar(cntxt) == TMPMARKER) {
		nextChar(cntxt);
		t = currChar(cntxt) - '0';
		if (t <= 0 || t > 9)
			parseError(cntxt, "[1-9] expected\n");
		else
			nextChar(cntxt);
		return t;
	}
	return -1;
}

/*
 * The simple type analysis currently assumes a proper type identifier.
 * We should change getMALtype to return a failure instead.
 */
static int
simpleTypeId(Client cntxt)
{
	int l, tpe;

	nextChar(cntxt);
	l = typeidLength(cntxt);
	if (l == 0) {
		parseError(cntxt, "Type identifier expected\n");
		cntxt->yycur--; /* keep it */
		return -1;
	}
	tpe = getAtomIndex(CURRENT(cntxt), l, -1);
	if (tpe < 0) {
		parseError(cntxt, "Type identifier expected\n");
		cntxt->yycur -= l; /* keep it */
		return TYPE_void;
	}
	advance(cntxt, l);
	return tpe;
}

static int
parseTypeId(Client cntxt, int defaultType)
{
	int i = TYPE_any, tt, kt = 0;
	char *s = CURRENT(cntxt);

	if (s[0] == ':' && s[1] == 'b' && s[2] == 'a' && s[3] == 't' && s[4] == '[') {
		/* parse :bat[:oid,:type] */
		advance(cntxt, 5);
		if (currChar(cntxt) == ':') {
			tt = simpleTypeId(cntxt);
			kt = typeAlias(cntxt, tt);
		} else{
			parseError(cntxt, "':bat[:any]' expected\n");
			return TYPE_bat;
		}

		i = newBatType(tt);
		if (kt > 0)
			setTypeIndex(i, kt);

		if (currChar(cntxt) != ']')
			parseError(cntxt, "']' expected\n");
		nextChar(cntxt); // skip ']' 
		skipSpace(cntxt);
		return i;
	}
	if (currChar(cntxt) == ':') {
		tt = simpleTypeId(cntxt);
		kt = typeAlias(cntxt, tt);
		if (kt > 0)
			setTypeIndex(tt, kt);
		return tt;
	}
	parseError(cntxt, "<type identifier> expected\n");
	return defaultType;
}

static inline int
typeElm(Client cntxt, int def)
{
	if (currChar(cntxt) != ':')
		return def;  /* no type qualifier */
	return parseTypeId(cntxt, def);
}

 /*
 * The Parser
 * The client is responsible to collect the
 * input for parsing in a single string before calling the parser.
 * Once the input is available parsing runs in a critial section for
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
 * globally known (?) and symbol table should be temporarilly locked
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
helpInfo(Client cntxt, str *help)
{
	int l;

	if (MALkeyword(cntxt, "comment", 7)) {
		skipSpace(cntxt);
		if ((l = stringLength(cntxt))) {
			GDKfree(*help);
			*help = strCopy(cntxt, l);
			if (*help)
				advance(cntxt, l - 1);
		} else {
			parseError(cntxt, "<string> expected\n");
		}
	} else if (currChar(cntxt) != ';')
		parseError(cntxt, "';' expected\n");
	skipToEnd(cntxt);
}

static InstrPtr
binding(Client cntxt, MalBlkPtr curBlk, InstrPtr curInstr, int flag)
{
	int l, varid = -1;
	malType type;

	l = idLength(cntxt);
	if (l > 0) {
		varid = findVariableLength(curBlk, CURRENT(cntxt), l);
		if (varid < 0) {
			varid = newVariable(curBlk, CURRENT(cntxt), l, TYPE_any);
			advance(cntxt, l);
			if ( varid < 0)
				return curInstr;
			type = typeElm(cntxt, TYPE_any);
			if (isPolymorphic(type))
				setPolymorphic(curInstr, type, TRUE);
			setVarType(curBlk, varid, type);
		} else if (flag) {
			parseError(cntxt, "Argument defined twice\n");
			typeElm(cntxt, getVarType(curBlk, varid));
		} else {
			advance(cntxt, l);
			type = typeElm(cntxt, getVarType(curBlk, varid));
			if( type != getVarType(curBlk,varid))
				parseError(cntxt, "Incompatible argument type\n");
			if (isPolymorphic(type))
				setPolymorphic(curInstr, type, TRUE);
			setVarType(curBlk, varid, type);
		}
	} else if (currChar(cntxt) == ':') {
		type = typeElm(cntxt, TYPE_any);
		varid = newTmpVariable(curBlk, type);
		if ( varid < 0)
			return curInstr;
		if ( isPolymorphic(type))
			setPolymorphic(curInstr, type, TRUE);
		setVarType(curBlk, varid, type);
	} else {
		parseError(cntxt, "argument expected\n");
		return curInstr;
	}
	if( varid >=0)
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
term(Client cntxt, MalBlkPtr curBlk, InstrPtr *curInstr, int ret)
{
	int i, idx, flag, free = 1;
	ValRecord cst;
	int cstidx = -1;
	malType tpe = TYPE_any;

	if ((i = cstToken(cntxt, &cst))) {
		advance(cntxt, i);
		if (currChar(cntxt) != ':' && cst.vtype == TYPE_dbl && cst.val.dval > FLT_MIN && cst.val.dval <= FLT_MAX) {
			cst.vtype = TYPE_flt;
			cst.val.fval = (flt) cst.val.dval;
		}
		cstidx = fndConstant(curBlk, &cst, MAL_VAR_WINDOW);
		if (cstidx >= 0) {

			if (currChar(cntxt) == ':') {
				tpe = typeElm(cntxt, getVarType(curBlk, cstidx));
				if (tpe < 0)
					return 3;
				if(tpe == getVarType(curBlk,cstidx) ){
					setVarUDFtype(curBlk, cstidx);
				} else {
					cstidx = defConstant(curBlk, tpe, &cst);
					setPolymorphic(*curInstr, tpe, FALSE);
					setVarUDFtype(curBlk, cstidx);
					free = 0;
				}
			} else if (cst.vtype != getVarType(curBlk, cstidx)) {
				cstidx = defConstant(curBlk, cst.vtype, &cst);
				setPolymorphic(*curInstr, cst.vtype, FALSE);
				free = 0;
			}
			/* protect against leaks coming from constant reuse */
			if (free && ATOMextern(cst.vtype) && cst.val.pval)
				VALclear(&cst);
			*curInstr = pushArgument(curBlk, *curInstr, cstidx);
			return ret;
		} else {
			/* add a new constant */
			flag = currChar(cntxt) == ':';
			tpe = typeElm(cntxt, cst.vtype);
			if (tpe < 0)
				return 3;
			cstidx = defConstant(curBlk, tpe, &cst);
			setPolymorphic(*curInstr, tpe, FALSE);
			if (flag)
				setVarUDFtype(curBlk, cstidx);
			*curInstr = pushArgument(curBlk, *curInstr, cstidx);
			return ret;
		}
	} else if ((i = idLength(cntxt))) {
		if ((idx = findVariableLength(curBlk, CURRENT(cntxt), i)) == -1) {
			idx = newVariable(curBlk, CURRENT(cntxt), i, TYPE_any);
			advance(cntxt, i);
			if( idx <0)
				return 0;
		} else {
			advance(cntxt, i);
		}
		*curInstr = pushArgument(curBlk, *curInstr, idx);
	} else if (currChar(cntxt) == ':') {
		tpe = typeElm(cntxt, TYPE_any);
		if (tpe < 0)
			return 3;
		setPolymorphic(*curInstr, tpe, FALSE);
		idx = newTypeVariable(curBlk, tpe);
		*curInstr = pushArgument(curBlk, *curInstr, idx);
		return ret;
	}
	return 0;
}

static str
parseAtom(Client cntxt)
{
	str modnme = 0;
	int l, tpe;
	char *nxt = CURRENT(cntxt);

	if ((l = idLength(cntxt)) <= 0)
		return parseError(cntxt, "atom name expected\n");

	/* parse: ATOM id:type */
	modnme = putNameLen(nxt, l);
	advance(cntxt, l);
	if (currChar(cntxt) != ':')
		tpe = TYPE_void;  /* no type qualifier */
	else
		tpe = parseTypeId(cntxt, TYPE_int);
	if( malAtomDefinition(cntxt->fdout, modnme, tpe) < 0){
		skipToEnd(cntxt);
		return 0;
	}
	cntxt->nspace = fixModule(cntxt->nspace, modnme);
	cntxt->nspace->isAtomModule = TRUE;
	skipSpace(cntxt);
	helpInfo(cntxt, &cntxt->nspace->help);
	return "";
}

/*
 * All modules, except 'user', should be global
 */
static str parseModule(Client cntxt)
{
	str modnme = 0;
	int l;
	char *nxt;

	nxt = CURRENT(cntxt);
	if ((l = idLength(cntxt)) <= 0)
		return parseError(cntxt, "<module path> expected\n");
	modnme = putNameLen(nxt, l);
	advance(cntxt, l);
	if( ! isModuleDefined(cntxt->nspace,modnme))
		newModule(NULL,modnme);
	cntxt->nspace = fixModule(cntxt->nspace, modnme);
	skipSpace(cntxt);
	helpInfo(cntxt, &cntxt->nspace->help);
	return "";
}

/*
 * Include statement
 * An include statement is immediately taken into effect. This
 * calls for temporary switching the input for a particular client.
 * The administration for this is handled by malInclude.
 * No listing is produced, because module sources are assumed to
 * be debugged upfront already.
 * @multitable @columnfractions 0.15 0.8
 * @item includeStmt
 * @tab : @sc{include} identifier
 * @item
 * @tab | @sc{include} string_literal
 * @end multitable
 *
 * Include files should be handled in line with parsing. This way we
 * are ensured that any possible signature definition will be known
 * afterwards. The effect is that errors in the include sequence are
 * marked as warnings.
 */
static str
parseInclude(Client cntxt)
{
	str modnme = 0, s;
	int x;
	char *nxt;

	if (!MALkeyword(cntxt, "include", 7))
		return 0;
	nxt = CURRENT(cntxt);

	if ((x = idLength(cntxt)) > 0) {
		modnme = putNameLen(nxt, x);
		advance(cntxt, x);
	} else if ((x = stringLength(cntxt)) > 0) {
		modnme = putNameLen(nxt + 1, x - 1);
		advance(cntxt, x);
	} else
		return parseError(cntxt, "<module name> expected\n");

	if (currChar(cntxt) != ';') {
		parseError(cntxt, "';' expected\n");
		skipToEnd(cntxt);
		return 0;
	}
	skipToEnd(cntxt);

	if (!malLibraryEnabled(modnme)) {
		return "";
	}

	s = loadLibrary(modnme, FALSE);
	if (s) {
		parseError(cntxt, s);
		GDKfree(s);
		return 0;
	}
	if ((s = malInclude(cntxt, modnme, 0))) {
		parseError(cntxt, s);
		GDKfree(s);
		return 0;
	}
	return "";
}

/*
 * Definition
 * The definition statements share a lot in common, which calls for factoring
 * out the code in a few text macros. Upon encountering a definition, we
 * initialize a MAL instruction container. We should also check for
 * non-terminated definitions.
 *
 * @multitable @columnfractions 0.15 0.8
 * @item program
 * @tab : ( definition [helpinfo] | statement ) *
 *
 * @item definition
 * @tab : moduleStmt | includeStmt
 * @item
 * @tab  |  commandStmt | patternStmt
 * @item
 * @tab  | functionStmt | factoryStmt
 * @item
 * @tab  | includeStmt
 * @end multitable
 *
 * Beware, a function signature f(a1..an):(b1..bn) is parsed in such a way that
 * the symbol table and stackframe contains the sequence
 * f,a1..an,b1..bn. This slightly complicates the implementation
 * of the return statement.
 *
 * Note, the function name could be mod.fcn, which calls for storing
 * the function definition in a particular module instead of the current one.
 */
static MalBlkPtr
fcnHeader(Client cntxt, int kind)
{
	int l;
	malType tpe;
	str fnme, modnme = NULL;
	char ch;
	Symbol curPrg;
	MalBlkPtr curBlk = 0;
	InstrPtr curInstr;

	l = operatorLength(cntxt);
	if (l == 0)
		l = idLength(cntxt);
	if (l == 0) {
		parseError(cntxt, "<identifier> | <operator> expected\n");
		skipToEnd(cntxt);
		return 0;
	}

	fnme = putNameLen(((char *) CURRENT(cntxt)), l);
	advance(cntxt, l);

	if (currChar(cntxt) == '.') {
		nextChar(cntxt); /* skip '.' */
		modnme = fnme;
		if (isModuleDefined(cntxt->nspace, modnme) == FALSE) {
			parseError(cntxt, "<module> name not defined\n");
			skipToEnd(cntxt);
			return curBlk;
		}
		l = operatorLength(cntxt);
		if (l == 0)
			l = idLength(cntxt);
		if (l == 0){
			parseError(cntxt, "<identifier> | <operator> expected\n");
			skipToEnd(cntxt);
			return 0;
		}
		fnme = putNameLen(((char *) CURRENT(cntxt)), l);
		advance(cntxt, l);
	} else 
		modnme= cntxt->nspace->name;

	/* temporary suspend capturing statements in main block */
	if (cntxt->backup){
		parseError(cntxt, "mal_parser: unexpected recursion\n");
		skipToEnd(cntxt);
		return 0;
	}
	if (currChar(cntxt) != '('){
		parseError(cntxt, "function header '(' expected\n");
		skipToEnd(cntxt);
		return curBlk;
	}
	advance(cntxt, 1);

	assert(!cntxt->backup);
	cntxt->backup = cntxt->curprg;
	cntxt->curprg = newFunction( modnme, fnme, kind);
	curPrg = cntxt->curprg;
	curBlk = curPrg->def;
	curBlk->flowfixed = 0;
	curBlk->typefixed = 0;
	curInstr = getInstrPtr(curBlk, 0);

	/* get calling parameters */
	ch = currChar(cntxt);
	while (ch != ')' && ch && !NL(ch)) {
		curInstr = binding(cntxt, curBlk, curInstr, 1);
		/* the last argument may be variable length */
		if (MALkeyword(cntxt, "...", 3)) {
			curInstr->varargs |= VARARGS;
			setPolymorphic(curInstr, TYPE_any, TRUE);
			break;
		}
		if ((ch = currChar(cntxt)) != ',') {
			if (ch == ')')
				break;
			if (cntxt->backup) {
				freeSymbol(cntxt->curprg);
				cntxt->curprg = cntxt->backup;
				cntxt->backup = 0;
				curBlk = NULL;
			}
			parseError(cntxt, "',' expected\n");
			skipToEnd(cntxt);
			return curBlk;
		} else
			nextChar(cntxt);  /* skip ',' */
		skipSpace(cntxt);
		ch = currChar(cntxt);
	}
	if (currChar(cntxt) != ')') {
		freeInstruction(curInstr);
		if (cntxt->backup) {
			freeSymbol(cntxt->curprg);
			cntxt->curprg = cntxt->backup;
			cntxt->backup = 0;
			curBlk = NULL;
		}
		parseError(cntxt, "')' expected\n");
		skipToEnd(cntxt);
		return curBlk;
	}
	advance(cntxt, 1); /* skip ')' */
/*
   The return type is either a single type or multiple return type structure.
   We simply keep track of the number of arguments added and
   during the final phase reshuffle the return values to the beginning (?)
 */
	if (currChar(cntxt) == ':') {
		tpe = typeElm(cntxt, TYPE_void);
		setPolymorphic(curInstr, tpe, TRUE);
		setVarType(curBlk, curInstr->argv[0], tpe);
		/* we may be confronted by a variable target type list */
		if (MALkeyword(cntxt, "...", 3)) {
			curInstr->varargs |= VARRETS;
			setPolymorphic(curInstr, TYPE_any, TRUE);
		}

	} else if (keyphrase1(cntxt, "(")) { /* deal with compound return */
		int retc = curInstr->argc, i1, i2 = 0;
		int max;
		short *newarg;
		/* parse multi-target result */
		/* skipSpace(cntxt);*/
		ch = currChar(cntxt);
		while (ch != ')' && ch && !NL(ch)) {
			curInstr = binding(cntxt, curBlk, curInstr, 0);
			/* we may be confronted by a variable target type list */
			if (MALkeyword(cntxt, "...", 3)) {
				curInstr->varargs |= VARRETS;
				setPolymorphic(curInstr, TYPE_any, TRUE);
			}
			if ((ch = currChar(cntxt)) != ',') {
				if (ch == ')')
					break;
				if (cntxt->backup) {
					freeSymbol(cntxt->curprg);
					cntxt->curprg = cntxt->backup;
					cntxt->backup = 0;
					curBlk = NULL;
				}
				parseError(cntxt, "',' expected\n");
				skipToEnd(cntxt);
				return curBlk;
			} else {
				nextChar(cntxt); /* skip ',' */
			}
			skipSpace(cntxt);
			ch = currChar(cntxt);
		}
		/* re-arrange the parameters, results first*/
		max = curInstr->maxarg;
		newarg = (short *) GDKmalloc(max * sizeof(curInstr->argv[0]));
		if (newarg == NULL){
			parseError(cntxt, MAL_MALLOC_FAIL);
			if (cntxt->backup) {
				freeSymbol(cntxt->curprg);
				cntxt->curprg = cntxt->backup;
				cntxt->backup = 0;
				curBlk = NULL;
			}
			skipToEnd(cntxt);
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
		if (currChar(cntxt) != ')') {
			freeInstruction(curInstr);
			if (cntxt->backup) {
				freeSymbol(cntxt->curprg);
				cntxt->curprg = cntxt->backup;
				cntxt->backup = 0;
				curBlk = NULL;
			}
			parseError(cntxt, "')' expected\n");
			skipToEnd(cntxt);
			return curBlk;
		}
		nextChar(cntxt); /* skip ')' */
	} else { /* default */
		setVarType(curBlk, 0, TYPE_void);
	}
	if (curInstr != getInstrPtr(curBlk, 0)) {
		freeInstruction(getInstrPtr(curBlk, 0));
		getInstrPtr(curBlk, 0) = curInstr;
	}
	return curBlk;
}

/*
 * The common theme in definitions is to parse the argument list.
 * @multitable @columnfractions .15 .8
 * @item header
 * @tab :  hdrName '(' params ')' result
 * @item result
 * @tab :  paramType | '(' params ')'
 * @item params
 * @tab :  binding [',' binding]*
 * @item binding
 * @tab :  identifier typeName [propQualifier]
 * @end multitable
*/
/*
 * MAL variables are statically/dynamically typed.
 * Function and procedure arguments should always be typed.
 * We do not permit polymorphism at this interpretation level.
 *
 * The type information maintained simplifies analysis of
 * column results. If the underlying type is not known, then it
 * may be replaced once during execution of a MAL instruction
 * typically as a side-effect of calling a bat-returning function.
 *
 * We should also allow for variable argument lists. However, they
 * may only appear in patterns, because the calling context is necessary
 * to resolve the actual argument list. Furthermore, we can not
 * assume much about its type structure.
 * Variables are extended with a property list to enable
 * optimizers to make decisions. (See the section on properties).
*/
/*
 * @-
 */
/*
 * Each procedure definition opens a structure in which the
 * information is gathered. The enclosing module is statically
 * determined.
 *
 * A proc-header translates into a single MAL instruction.
 * Since no recursive rules are included, we can stick to
 * using a single global variable to accummulate the
 * properties.
 *
 * The external commands and rules come with a short
 * help information.
*/

static MalBlkPtr
parseCommandPattern(Client cntxt, int kind)
{
	MalBlkPtr curBlk = 0;
	Symbol curPrg = 0;
	InstrPtr curInstr = 0;
	str modnme = NULL;
	size_t l = 0;

	curBlk = fcnHeader(cntxt, kind);
	if (curBlk == NULL) 
		return curBlk;
	getInstrPtr(curBlk, 0)->token = kind;
	curPrg = cntxt->curprg;
	curPrg->kind = kind;
	curInstr = getInstrPtr(curBlk, 0);

	modnme = getModuleId(getInstrPtr(curBlk, 0));
	if (modnme && isModuleDefined(cntxt->nspace, modnme) == FALSE)
		return (MalBlkPtr) parseError(cntxt, "<module> not defined\n");
	modnme = modnme ? modnme : cntxt->nspace->name;

	l = strlen(modnme);
	modnme = putNameLen(modnme, l);
	if (isModuleDefined(cntxt->nspace, modnme))
		insertSymbol(findModule(cntxt->nspace, modnme), curPrg);
	else {
		freeSymbol(curPrg);
		cntxt->curprg = cntxt->backup;
		cntxt->backup = 0;
		return (MalBlkPtr) parseError(cntxt, "<module> not found\n");
	}
	chkProgram(cntxt->fdout, cntxt->nspace, curBlk);
	if (cntxt->backup) {
		cntxt->curprg = cntxt->backup;
		cntxt->backup = 0;
	}
/*
 * Short-cut function calls
 * Most functions are (dynamically) linked with the kernel as
 * commands or pattern definitions.  This enables for fast execution.
 *
 * In addition we allow functions to be bound to both
 * a linked C-function and a MAL specification block.
 * It the function address is not available, the interpreter
 * will use the MAL block instead.
 * This scheme is intended for just-in-time compilation.
 *
 * [note, command and patterns do not have a MAL block]
 */
	if (MALkeyword(cntxt, "address", 7)) {
		int i;
		i = idLength(cntxt);
		if (i == 0) {
			parseError(cntxt, "<identifier> expected\n");
			return 0;
		}
		cntxt->blkmode = 0;
		if (getModuleId(curInstr))
			setModuleId(curInstr, NULL);
		setModuleScope(curInstr,
				findModule(cntxt->nspace, modnme));

		memcpy(curBlk->binding, CURRENT(cntxt), (size_t)(i < IDLENGTH? i:IDLENGTH-1));
		curBlk->binding[(i< IDLENGTH? i:IDLENGTH-1)] = 0;
		/* avoid a clash with old temporaries */
		advance(cntxt, i);
		curInstr->fcn = getAddress(cntxt->fdout, cntxt->srcFile, curBlk->binding, 0);

		if (cntxt->nspace->isAtomModule) {
			if (curInstr->fcn == NULL) {
				parseError(cntxt, "<address> not found\n");
				return 0;
			}
			malAtomProperty(curBlk, curInstr);
		}
		skipSpace(cntxt);
	} else {
		parseError(cntxt, "'address' expected\n");
		return 0;
	}
	helpInfo(cntxt, &curBlk->help);
	showErrors(cntxt);
	if (curBlk && cntxt->listing > 1)
		printFunction(cntxt->fdout, curBlk, 0, cntxt->listing);
#ifdef HAVE_HGE
	if (!have_hge)
		have_hge = strcmp(modnme, "calc") == 0 && strcmp(getFunctionId(curInstr), "hge") == 0;
#endif
	return curBlk;
}

static MalBlkPtr
parseFunction(Client cntxt, int kind)
{
	MalBlkPtr curBlk = 0;

	curBlk = fcnHeader(cntxt, kind);
	if (curBlk == NULL) 
		return curBlk;
	if (MALkeyword(cntxt, "address", 7)) {
		str nme;
		int i;
		InstrPtr curInstr = getInstrPtr(curBlk, 0);
		i = idLength(cntxt);
		if (i == 0) {
			parseError(cntxt, "<identifier> expected\n");
			return 0;
		}
		nme = idCopy(cntxt, i);
		curInstr->fcn = getAddress(cntxt->fdout, cntxt->srcFile, nme, 0);
		GDKfree(nme);
		if (curInstr->fcn == NULL) {
			parseError(cntxt, "<address> not found\n");
			return 0;
		}
		skipSpace(cntxt);
	}
	/* block is terminated at the END statement */
	helpInfo(cntxt, &curBlk->help);
	return curBlk;
}

/*
 * Functions and  factories end with a labeled end-statement.
 * The routine below checks for misalignment of the closing statements.
 * Any instruction parsed after the function block is considered an error.
 */
static int
parseEnd(Client cntxt)
{
	MalBlkPtr curBlk = 0;
	Symbol curPrg = 0;
	int l, showit = 0;

	if (MALkeyword(cntxt, "end", 3)) {
		curPrg = cntxt->curprg;
		curBlk = curPrg->def;
		l = idLength(cntxt);
		if (l == 0)
			l = operatorLength(cntxt);

		if (strncmp(CURRENT(cntxt), getModuleId(getSignature(curPrg)), l) == 0) {
			advance(cntxt, l);
			skipSpace(cntxt);
			if (currChar(cntxt) == '.')
				nextChar(cntxt);
			skipSpace(cntxt);
			l = idLength(cntxt);
			if (l == 0)
				l = operatorLength(cntxt);
		}
		/* parse fcn */
		if ((l == (int) strlen(curPrg->name) &&
			 strncmp(CURRENT(cntxt), curPrg->name, l) == 0) || l == 0) {} else {
			parseError(cntxt, "non matching end label\n");
		}
		pushEndInstruction(curBlk);
		insertSymbol(cntxt->nspace, cntxt->curprg);
		cntxt->blkmode = 0;
		curBlk->typefixed = 0;
		chkProgram(cntxt->fdout, cntxt->nspace, cntxt->curprg->def);
		if (cntxt->backup) {
			cntxt->curprg = cntxt->backup;
			cntxt->backup = 0;
		}
		showit = TRUE;
		skipToEnd(cntxt);
		if (showit && cntxt->listing > 1)
			printFunction(cntxt->fdout, curBlk, 0, cntxt->listing);
		showErrors(cntxt);
		return 1;
	}
	return 0;
}
/*
 * Most instructions are simple assignments, possibly
 * modified with a barrier/catch tag.
 * @multitable @columnfractions .15 .8
 * @item statement
 * @tab :  tag varlist [':=' expr ] propQualifier
 * @item tag
 * @tab :  @sc{ return} | @sc{ barrier} | @sc{ catch}
 * @item
 * @tab |  @sc{ leave} | @sc{ redo} |
 * @item expr
 * @tab :  fcncall
 * @item
 * @tab : [factor  operator] factor
 * @item varlist
 * @tab :  variable
 * @item
 * @tab |  @verb{'{' variable {',' variable}* ')' }
 * @item variable
 * @tab :  identifier propQualifier
 * @item factor
 * @tab :  constant | var
 * @end multitable
 *
 * The basic types are also predefined as a variable.
 * This makes it easier to communicate types to MAL patterns.
 */

#define GETvariable	\
	if ((varid = findVariableLength(curBlk, CURRENT(cntxt), l)) == -1) { \
		varid = newVariable(curBlk, CURRENT(cntxt),l, TYPE_any);	\
		advance(cntxt, l);\
		if(varid <  0) return;\
	} else \
		advance(cntxt, l);

/* The parameter of parseArguments is the return value of the enclosing function. */
static int
parseArguments(Client cntxt, MalBlkPtr curBlk, InstrPtr *curInstr)
{
	while (currChar(cntxt) != ')') {
		switch (term(cntxt, curBlk, curInstr, 0)) {
		case 0:
			break;
		case 2: return 2;
		case 3: return 3;
		default:
			parseError(cntxt, "<factor> expected\n");
			pushInstruction(curBlk, *curInstr);
			skipToEnd(cntxt);
			return 1;
		}
		if (currChar(cntxt) == ',')
			advance(cntxt, 1);
		else if (currChar(cntxt) != ')') {
			parseError(cntxt, "',' expected\n");
			cntxt->yycur--; /* keep it */
			break;
		}
	}
	if (currChar(cntxt) == ')')
		advance(cntxt, 1);
	return 0;
}

static void
parseAssign(Client cntxt, int cntrl)
{
	InstrPtr curInstr;
	MalBlkPtr curBlk;
	Symbol curPrg;
	int i = 0, l, type = TYPE_any, varid = -1;
	str arg = 0;
	ValRecord cst;

	curPrg = cntxt->curprg;
	curBlk = curPrg->def;
	curInstr = newInstruction(curBlk, NULL, NULL);
	
	if( cntrl){
		curInstr->token = ASSIGNsymbol;
		curInstr->barrier = cntrl;
	}

	/* start the parsing by recognition of the lhs of an assignment */
	if (currChar(cntxt) == '(') {
		/* parsing multi-assignment */
		advance(cntxt, 1);
		curInstr->argc = 0; /*reset to handle pushArg correctly !! */
		curInstr->retc = 0;
		while (currChar(cntxt) != ')' && currChar(cntxt)) {
			l = idLength(cntxt);
			i = cstToken(cntxt, &cst);
			if (l == 0 || i) {
				parseError(cntxt, "<identifier> expected\n");
				skipToEnd(cntxt);
				freeInstruction(curInstr);
				return;
			}
			GETvariable;
			if (currChar(cntxt) == ':') {
				setVarUDFtype(curBlk, varid);
				type = typeElm(cntxt, getVarType(curBlk, varid));
				if (type < 0)
					goto part3;
				setPolymorphic(curInstr, type, FALSE);
				setVarType(curBlk, varid, type);
			}
			curInstr = pushArgument(curBlk, curInstr, varid);
			curInstr->retc++;
			if (currChar(cntxt) == ')')
				break;
			if (currChar(cntxt) == ',')
				keyphrase1(cntxt, ",");
		}
		advance(cntxt, 1); /* skip ')' */
		if (curInstr->retc == 0) {
			/* add dummy variable */
			curInstr = pushArgument(curBlk, curInstr, newTmpVariable(curBlk, TYPE_any));
			curInstr->retc++;
		}
	} else {
		/* are we dealing with a simple assignment? */
		l = idLength(cntxt);
		i = cstToken(cntxt, &cst);
		if (l == 0 || i) {
			/* we haven't seen a target variable */
			/* flow of control statements may end here. */
			/* shouldn't allow for nameless controls todo*/
			if (i && cst.vtype == TYPE_str)
				GDKfree(cst.val.sval);
			if (cntrl == LEAVEsymbol || cntrl == REDOsymbol ||
				cntrl == RETURNsymbol || cntrl == EXITsymbol) {
				curInstr->argv[0] = getBarrierEnvelop(curBlk);
				pushInstruction(curBlk, curInstr);
				if (currChar(cntxt) != ';')
					parseError(cntxt, "<identifier> expected\n");
				skipToEnd(cntxt);
				return;
			}
			getArg(curInstr, 0) = newTmpVariable(curBlk, TYPE_any);
			pushInstruction(curBlk, curInstr);
			parseError(cntxt, "<identifier> expected\n");
			skipToEnd(cntxt);
			return;
		}
		/* Check if we are dealing with module.fcn call*/
		if (CURRENT(cntxt)[l] == '.' || CURRENT(cntxt)[l] == '(') {
			curInstr->argv[0] = newTmpVariable(curBlk, TYPE_any);
			goto FCNcallparse;
		}

		/* Get target variable details*/
		GETvariable;
		if (!(currChar(cntxt) == ':' && CURRENT(cntxt)[1] == '=')) {
			curInstr->argv[0] = varid;
			if (currChar(cntxt) == ':') {
				setVarUDFtype(curBlk, varid);
				type = typeElm(cntxt, getVarType(curBlk, varid));
				if (type < 0)
					goto part3;
				setPolymorphic(curInstr, type, FALSE);
				setVarType(curBlk, varid, type);
			}
		}
		curInstr->argv[0] = varid;
	}
	/* look for assignment operator */
	if (!keyphrase2(cntxt, ":=")) {
		/* no assignment !! a control variable is allowed */
		/* for the case RETURN X, we normalize it to include the function arguments */
		if (cntrl == RETURNsymbol || cntrl == YIELDsymbol) {
			int e;
			InstrPtr sig = getInstrPtr(curBlk,0);
			curInstr->retc = 0;
			for (e = 0; e < sig->retc; e++) 
				curInstr = pushReturn(curBlk, curInstr, getArg(sig, e));
		}

		goto part3;
	}
	if (currChar(cntxt) == '(') {
		/* parse multi assignment */
		advance(cntxt, 1);
		switch (parseArguments(cntxt, curBlk, &curInstr)) {
		case 2: goto part2;
		default:
		case 3: goto part3;
		}
		/* unreachable */
	}
/*
 * We have so far the LHS part of an assignment. The remainder is
 * either a simple term expression, a multi assignent, or the start
 * of a function call.
 */
FCNcallparse:
	if ((l = idLength(cntxt)) && CURRENT(cntxt)[l] == '(') {
		/*  parseError(cntxt,"<module> expected\n");*/
		setModuleId(curInstr, getModuleId(getInstrPtr(curBlk, 0)));
		i = l;
		goto FCNcallparse2;
	} else if ((l = idLength(cntxt)) && CURRENT(cntxt)[l] == '.') {
		/* continue with parseing a function/operator call */
		arg = putNameLen(CURRENT(cntxt), l);
		advance(cntxt, l + 1); /* skip '.' too */
		setModuleId(curInstr, arg);
		i = idLength(cntxt);
		if (i == 0)
			i = operatorLength(cntxt);
FCNcallparse2:
		if (i) {
			setFunctionId(curInstr, putNameLen(((char *) CURRENT(cntxt)), i));
			advance(cntxt, i);
		} else {
			parseError(cntxt, "<functionname> expected\n");
			skipToEnd(cntxt);
			pushInstruction(curBlk, curInstr);
			return;
		}
		skipSpace(cntxt);
		if (currChar(cntxt) != '(') {
			parseError(cntxt, "'(' expected\n");
			skipToEnd(cntxt);
			pushInstruction(curBlk, curInstr);
			return;
		}
		advance(cntxt, 1);
		switch (parseArguments(cntxt, curBlk, &curInstr)) {
		case 2: goto part2;
		default:
		case 3: goto part3;
		}
		/* unreachable */
	}
	/* Handle the ordinary assignments and expressions */
	switch (term(cntxt, curBlk, &curInstr, 2)) {
	case 2: goto part2;
	case 3: goto part3;
	}
part2:  /* consume <operator><term> part of expression */
	if ((i = operatorLength(cntxt))) {
		/* simple arithmetic operator expression */
		setFunctionId(curInstr, putNameLen(((char *) CURRENT(cntxt)), i));
		advance(cntxt, i);
		curInstr->modname = putName("calc");
		if ((l = idLength(cntxt)) && !(l == 3 && strncmp(CURRENT(cntxt), "nil", 3) == 0)) {
			GETvariable;
			curInstr = pushArgument(curBlk, curInstr, varid);
			goto part3;
		}
		switch (term(cntxt, curBlk, &curInstr, 3)) {
		case 2: goto part2;
		case 3: goto part3;
		}
		parseError(cntxt, "<term> expected\n");
		skipToEnd(cntxt);
		pushInstruction(curBlk, curInstr);
		return;
	} else {
		skipSpace(cntxt);
		if (currChar(cntxt) == '(')
			parseError(cntxt, "module name missing\n");
		else if (currChar(cntxt) != ';' && currChar(cntxt) != '#') {
			parseError(cntxt, "operator expected\n");
			skipToEnd(cntxt);
		}
		pushInstruction(curBlk, curInstr);
		return;
	}
part3:
	skipSpace(cntxt);
	if (currChar(cntxt) != ';')
		parseError(cntxt, "';' expected\n");
	skipToEnd(cntxt);
	pushInstruction(curBlk, curInstr);
	if (cntrl == RETURNsymbol && !(curInstr->token == ASSIGNsymbol || getModuleId(curInstr) != 0))
		parseError(cntxt, "return assignment expected\n");
}

int
parseMAL(Client cntxt, Symbol curPrg, int skipcomments, int lines)
{
	int cntrl = 0;
	/*Symbol curPrg= cntxt->curprg;*/
	char c;
	int inlineProp =0, unsafeProp = 0, sealedProp = 0;

	echoInput(cntxt);
	/* here the work takes place */
	while ((c = currChar(cntxt)) && lines > 0) {
		switch (c) {
		case '\n': case '\r': case '\f':
			lines -= c =='\n';
			nextChar(cntxt);
			echoInput(cntxt);
			continue;
		case ';': case '\t': case ' ':
			nextChar(cntxt);
			continue;
		case '#':
		{ /* keep the full line comments unless it is a MX #line */
			char start[256], *e = start, c;
			MalBlkPtr curBlk = cntxt->curprg->def;
			InstrPtr curInstr;

			*e = 0;
			nextChar(cntxt);
			while ((c = currChar(cntxt))) {
				if (e < start + 256 - 1)
					*e++ = c;
				nextChar(cntxt);
				if (c == '\n' || c == '\r') {
					*e = 0;
					if (e > start)
						e--;
					/* prevChar(cntxt);*/
					break;
				}
			}
			if (e > start)
				*e = 0;
			if (! skipcomments && e > start && curBlk->stop > 0 ) {
				ValRecord cst;
				curInstr = newInstruction(curBlk, NULL, NULL);
				curInstr->token= REMsymbol;
				curInstr->barrier= 0;
				cst.vtype = TYPE_str;
				cst.len = (int) strlen(start);
				cst.val.sval = GDKstrdup(start);
				getArg(curInstr, 0) = defConstant(curBlk, TYPE_str, &cst);
				clrVarConstant(curBlk, getArg(curInstr, 0));
				setVarDisabled(curBlk, getArg(curInstr, 0));
				pushInstruction(curBlk, curInstr);
			}
			echoInput(cntxt);
		}
			continue;
		case 'A': case 'a':
			if (MALkeyword(cntxt, "atom", 4) &&
				parseAtom(cntxt) != 0)
				break;
			goto allLeft;
		case 'b': case 'B':
			if (MALkeyword(cntxt, "barrier", 7)) {
				cntxt->blkmode++;
				cntrl = BARRIERsymbol;
			}
			goto allLeft;
		case 'C': case 'c':
			if (MALkeyword(cntxt, "command", 7)) {
				MalBlkPtr p = parseCommandPattern(cntxt, COMMANDsymbol);
				if (p) {
					p->unsafeProp = unsafeProp;
					p->sealedProp = sealedProp;
				}
				cntxt->curprg->def->unsafeProp = unsafeProp;
				cntxt->curprg->def->sealedProp = sealedProp;
				if (inlineProp)
					showException(cntxt->fdout, SYNTAX, "parseError", "INLINE ignored");
				inlineProp = 0;
				unsafeProp = 0;
				sealedProp = 0;
				continue;
			}
			if (MALkeyword(cntxt, "catch", 5)) {
				cntxt->blkmode++;
				cntrl = CATCHsymbol;
				goto allLeft;
			}
			goto allLeft;
		case 'E': case 'e':
			if (MALkeyword(cntxt, "exit", 4)) {
				if (cntxt->blkmode > 0)
					cntxt->blkmode--;
				cntrl = EXITsymbol;
			} else if (parseEnd(cntxt)) {
				break;
			}
			goto allLeft;
		case 'F': case 'f':
			if (MALkeyword(cntxt, "function", 8)) {
				MalBlkPtr p;
				cntxt->blkmode++;
				if ((p = parseFunction(cntxt, FUNCTIONsymbol))){
					p->inlineProp = inlineProp;
					p->unsafeProp = unsafeProp;
					p->sealedProp = sealedProp;
					cntxt->curprg->def->inlineProp = inlineProp;
					cntxt->curprg->def->unsafeProp = unsafeProp;
					cntxt->curprg->def->sealedProp = sealedProp;
					inlineProp = 0;
					unsafeProp = 0;
					sealedProp = 0;
					break;
				}
			} else if (MALkeyword(cntxt, "factory", 7)) {
				if( inlineProp )
					showException(cntxt->fdout, SYNTAX, "parseError", "INLINE ignored");
				if( unsafeProp)
					showException(cntxt->fdout, SYNTAX, "parseError", "UNSAFE ignored");
				if( sealedProp)
					showException(cntxt->fdout, SYNTAX, "parseError", "SEALED ignored");
				inlineProp = 0;
				unsafeProp = 0;
				sealedProp = 0;
				cntxt->blkmode++;
				parseFunction(cntxt, FACTORYsymbol);
				break;
			}
			goto allLeft;
		case 'I': case 'i': 
			if (MALkeyword(cntxt, "inline", 6)) {
				inlineProp= 1;
				skipSpace(cntxt);
				continue;
			} else
			if (parseInclude(cntxt))
				continue;
			goto allLeft;
		case 'L': case 'l':
			if (MALkeyword(cntxt, "leave", 5))
				cntrl = LEAVEsymbol;
			goto allLeft;
		case 'M': case 'm':
			if (MALkeyword(cntxt, "module", 6) &&
				parseModule(cntxt) != 0)
				break;
			goto allLeft;
		case 'P': case 'p':
			if (MALkeyword(cntxt, "pattern", 7)) {
				MalBlkPtr p;
				if( inlineProp )
					showException(cntxt->fdout, SYNTAX, "parseError", "INLINE ignored");
				p = parseCommandPattern(cntxt, PATTERNsymbol);
				if (p) {
					p->unsafeProp = unsafeProp;
					p->sealedProp = sealedProp;
				}
				cntxt->curprg->def->unsafeProp = unsafeProp;
				cntxt->curprg->def->sealedProp = sealedProp;
				inlineProp = 0;
				unsafeProp = 0;
				sealedProp = 0;
				continue;
			}
			goto allLeft;
		case 'R': case 'r':
			if (MALkeyword(cntxt, "redo", 4)) {
				cntrl = REDOsymbol;
				goto allLeft;
			}
			if (MALkeyword(cntxt, "raise", 5)) {
				cntrl = RAISEsymbol;
				goto allLeft;
			}
			if (MALkeyword(cntxt, "return", 6)) {
				cntrl = RETURNsymbol;
			}
			goto allLeft;
		case 's':
			if (MALkeyword(cntxt, "sealed", 6)) {
				sealedProp= 1;
				skipSpace(cntxt);
				continue;
			}
			goto allLeft;
		case 'U': case 'u': 
			if (MALkeyword(cntxt, "unsafe", 6)) {
				unsafeProp= 1;
				skipSpace(cntxt);
				continue;
			}
			goto allLeft;
		case 'Y': case 'y':
			if (MALkeyword(cntxt, "yield", 5)) {
				cntrl = YIELDsymbol;
				goto allLeft;
			}
			/* fall through */
		default: allLeft :
			parseAssign(cntxt, cntrl);
			cntrl = 0;
			if (curPrg->def->errors >= MAXERRORS) \
				return curPrg->def->errors;
		}
	}
	return curPrg->def->errors;
}

/*
 * Error display
 * Display the error information for the current client.
 * An arrow and state number is printed at the "appropriate" place.
 * If no lookahead character is a used and the next character is a newline,
 * we should also copy the input.
 */
static str
parseError(Client cntxt, str msg)
{
	Symbol curPrg;
	MalBlkPtr curBlk;
	char buf[1028];
	char *s = buf, *t, *l = lastline(cntxt);
	lng i;

	curPrg = cntxt->curprg;
	curBlk = curPrg->def;
	if (curBlk)
		curBlk->errors++;

	for (t = l; *t && *t != '\n' && s < buf+sizeof(buf)-4; t++) {
		*s++ = *t;
	}
	*s++ = '\n';
	*s = 0;
	if (s != buf + 1 && strlen(buf) < sizeof(buf) - 4) {
		showException(cntxt->fdout, SYNTAX, "parseError", "%s", buf);
		/* produce the position marker*/
		s = buf;
		i = position(cntxt) - 1;
		for (; i > 0 && s < buf+sizeof(buf)-4; i--) {
			*s++ = ((l && *(l + 1) && *l++ != '\t')) ? ' ' : '\t';
		}
		*s++ = '^';
		*s = 0;
	}

	if (msg && strlen(msg))
		snprintf(s, sizeof(buf)-(s-buf), "%s", msg);
	skipToEnd(cntxt);
	showException(cntxt->fdout, SYNTAX, "parseError", "%s", buf);
	return 0;
}

%{
/*-------------------------------------------------------------------------
 *
 * jsonpath_gram.y
 *	 Grammar definitions for jsonpath datatype
 *
 * Transforms tokenized jsonpath into tree of JsonPathParseItem structs.
 *
 * Copyright (c) 2019-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath_gram.y
 *
 *-------------------------------------------------------------------------
 */


typedef void* yyscan_t;

#include "jsonpath_internal.h"

static JsonPathParseItem *makeItemType(struct Node *escontext, JsonPathItemType type);
static JsonPathParseItem *makeItemString(struct Node *escontext, JsonPathString *s);
static JsonPathParseItem *makeItemVariable(struct Node *escontext, JsonPathString *s);
static JsonPathParseItem *makeItemKey(struct Node *escontext, JsonPathString *s);
static JsonPathParseItem *makeItemNumeric(struct Node *escontext, JsonPathString *s);
static JsonPathParseItem *makeItemBool(struct Node *escontext, bool val);
static JsonPathParseItem *makeItemBinary(struct Node *escontext, JsonPathItemType type,
										 JsonPathParseItem *la,
										 JsonPathParseItem *ra);
static JsonPathParseItem *makeItemUnary(struct Node *escontext, JsonPathItemType type,
										JsonPathParseItem *a);
static JsonPathParseItem *makeItemList(List *list);
static JsonPathParseItem *makeIndexArray(struct Node *escontext, List *list);
static JsonPathParseItem *makeAny(struct Node *escontext, int first, int last);
/*
static bool makeItemLikeRegex(JsonPathParseItem *expr,
							  JsonPathString *pattern,
							  JsonPathString *flags,
							  JsonPathParseItem ** result,
							  struct Node *escontext);
*/

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.
 */
#define YYMALLOC GDKmalloc
#define YYFREE   GDKfree

%}

/* BISON Declarations */
%define api.pure full
%expect 0
%name-prefix="jsonpath_yy"
%param {yyscan_t scanner}
%parse-param {JsonPathParseResult **result}
%parse-param {struct Node *escontext}
%lex-param {JsonPathParseResult **result}
%lex-param {struct Node *escontext}

%union
{
	JsonPathString		str;
	List			   *elems;	/* list of JsonPathParseItem */
	List			   *indexs;	/* list of integers */
	JsonPathParseItem  *value;
	JsonPathParseResult *result;
	JsonPathItemType	optype;
	bool				boolean;
	int					integer;
}

%token	<str>		TO_P NULL_P TRUE_P FALSE_P IS_P UNKNOWN_P EXISTS_P
%token	<str>		IDENT_P STRING_P NUMERIC_P INT_P VARIABLE_P
%token	<str>		OR_P AND_P NOT_P
%token	<str>		LESS_P LESSEQUAL_P EQUAL_P NOTEQUAL_P GREATEREQUAL_P GREATER_P
%token	<str>		ANY_P STRICT_P LAX_P LAST_P STARTS_P WITH_P LIKE_REGEX_P FLAG_P
%token	<str>		ABS_P SIZE_P TYPE_P FLOOR_P DOUBLE_P CEILING_P KEYVALUE_P
%token	<str>		DATETIME_P
%token	<str>		BIGINT_P BOOLEAN_P DATE_P DECIMAL_P INTEGER_P NUMBER_P
%token	<str>		STRINGFUNC_P TIME_P TIME_TZ_P TIMESTAMP_P TIMESTAMP_TZ_P

%type	<result>	result

%type	<value>		scalar_value path_primary expr array_accessor
					any_path accessor_op key predicate delimited_predicate
					index_elem starts_with_initial expr_or_predicate
					datetime_template opt_datetime_template csv_elem
					datetime_precision opt_datetime_precision

%type	<elems>		accessor_expr csv_list opt_csv_list

%type	<indexs>	index_list

%type	<optype>	comp_op method

%type	<boolean>	mode

%type	<str>		key_name

%type	<integer>	any_level

%left	OR_P
%left	AND_P
%right	NOT_P
%left	'+' '-'
%left	'*' '/' '%'
%left	UMINUS
%nonassoc '(' ')'

/* Grammar follows */
%%

result:
	mode expr_or_predicate			{
										*result = sa_alloc(escontext->sa, sizeof(JsonPathParseResult));
										(*result)->expr = $2;
										(*result)->lax = $1;
										(void) yynerrs;
									}
	| /* EMPTY */					{ *result = NULL; }
	;

expr_or_predicate:
	expr							{ $$ = $1; }
	| predicate						{ $$ = $1; }
	;

mode:
	STRICT_P						{ $$ = false; }
	| LAX_P							{ $$ = true; }
	| /* EMPTY */					{ $$ = true; }
	;

scalar_value:
	STRING_P						{ $$ = makeItemString(escontext, &$1); }
	| NULL_P						{ $$ = makeItemString(escontext, NULL); }
	| TRUE_P						{ $$ = makeItemBool(escontext, true); }
	| FALSE_P						{ $$ = makeItemBool(escontext, false); }
	| NUMERIC_P						{ $$ = makeItemNumeric(escontext, &$1); }
	| INT_P							{ $$ = makeItemNumeric(escontext, &$1); }
	| VARIABLE_P					{ $$ = makeItemVariable(escontext, &$1); }
	;

comp_op:
	EQUAL_P							{ $$ = jpiEqual; }
	| NOTEQUAL_P					{ $$ = jpiNotEqual; }
	| LESS_P						{ $$ = jpiLess; }
	| GREATER_P						{ $$ = jpiGreater; }
	| LESSEQUAL_P					{ $$ = jpiLessOrEqual; }
	| GREATEREQUAL_P				{ $$ = jpiGreaterOrEqual; }
	;

delimited_predicate:
	'(' predicate ')'				{ $$ = $2; }
	| EXISTS_P '(' expr ')'			{ $$ = makeItemUnary(escontext, jpiExists, $3); }
	;

predicate:
	delimited_predicate				{ $$ = $1; }
	| expr comp_op expr				{ $$ = makeItemBinary(escontext, $2, $1, $3); }
	| predicate AND_P predicate		{ $$ = makeItemBinary(escontext, jpiAnd, $1, $3); }
	| predicate OR_P predicate		{ $$ = makeItemBinary(escontext, jpiOr, $1, $3); }
	| NOT_P delimited_predicate		{ $$ = makeItemUnary(escontext, jpiNot, $2); }
	| '(' predicate ')' IS_P UNKNOWN_P
									{ $$ = makeItemUnary(escontext, jpiIsUnknown, $2); }
	| expr STARTS_P WITH_P starts_with_initial
									{ $$ = makeItemBinary(escontext, jpiStartsWith, $1, $4); }
/*
	| expr LIKE_REGEX_P STRING_P
	{
		JsonPathParseItem *jppitem;
		if (! makeItemLikeRegex($1, &$3, NULL, &jppitem, escontext))
			YYABORT;
		$$ = jppitem;
	}
	| expr LIKE_REGEX_P STRING_P FLAG_P STRING_P
	{
		JsonPathParseItem *jppitem;
		if (! makeItemLikeRegex($1, &$3, &$5, &jppitem, escontext))
			YYABORT;
		$$ = jppitem;
	}
*/
	;

starts_with_initial:
	STRING_P						{ $$ = makeItemString(escontext, &$1); }
	| VARIABLE_P					{ $$ = makeItemVariable(escontext, &$1); }
	;

path_primary:
	scalar_value					{ $$ = $1; }
	| '$'							{ $$ = makeItemType(escontext, jpiRoot); }
	| '@'							{ $$ = makeItemType(escontext, jpiCurrent); }
	| LAST_P						{ $$ = makeItemType(escontext, jpiLast); }
	;

accessor_expr:
	path_primary					{ $$ = list_make1($1); }
	| '(' expr ')' accessor_op		{ $$ = list_make2($2, $4); }
	| '(' predicate ')' accessor_op	{ $$ = list_make2($2, $4); }
	| accessor_expr accessor_op		{ $$ = lappend($1, $2); }
	;

expr:
	accessor_expr					{ $$ = makeItemList($1); }
	| '(' expr ')'					{ $$ = $2; }
	| '+' expr %prec UMINUS			{ $$ = makeItemUnary(escontext, jpiPlus, $2); }
	| '-' expr %prec UMINUS			{ $$ = makeItemUnary(escontext, jpiMinus, $2); }
	| expr '+' expr					{ $$ = makeItemBinary(escontext, jpiAdd, $1, $3); }
	| expr '-' expr					{ $$ = makeItemBinary(escontext, jpiSub, $1, $3); }
	| expr '*' expr					{ $$ = makeItemBinary(escontext, jpiMul, $1, $3); }
	| expr '/' expr					{ $$ = makeItemBinary(escontext, jpiDiv, $1, $3); }
	| expr '%' expr					{ $$ = makeItemBinary(escontext, jpiMod, $1, $3); }
	;

index_elem:
	expr							{ $$ = makeItemBinary(escontext, jpiSubscript, $1, NULL); }
	| expr TO_P expr				{ $$ = makeItemBinary(escontext, jpiSubscript, $1, $3); }
	;

index_list:
	index_elem						{ $$ = list_make1($1); }
	| index_list ',' index_elem		{ $$ = lappend($1, $3); }
	;

array_accessor:
	'[' '*' ']'						{ $$ = makeItemType(escontext, jpiAnyArray); }
	| '[' index_list ']'			{ $$ = makeIndexArray(escontext, $2); }
	;

any_level:
	INT_P							{ $$ = pg_strtoint32($1.val); }
	| LAST_P						{ $$ = -1; }
	;

any_path:
	ANY_P							{ $$ = makeAny(escontext, 0, -1); }
	| ANY_P '{' any_level '}'		{ $$ = makeAny(escontext, $3, $3); }
	| ANY_P '{' any_level TO_P any_level '}'
									{ $$ = makeAny(escontext, $3, $5); }
	;

accessor_op:
	'.' key							{ $$ = $2; }
	| '.' '*'						{ $$ = makeItemType(escontext, jpiAnyKey); }
	| array_accessor				{ $$ = $1; }
	| '.' any_path					{ $$ = $2; }
	| '.' method '(' ')'			{ $$ = makeItemType(escontext, $2); }
	| '?' '(' predicate ')'			{ $$ = makeItemUnary(escontext, jpiFilter, $3); }
	| '.' DECIMAL_P '(' opt_csv_list ')'
		{
			if (list_length($4) == 0)
				$$ = makeItemBinary(escontext, jpiDecimal, NULL, NULL);
			else if (list_length($4) == 1)
				$$ = makeItemBinary(escontext, jpiDecimal, linitial($4), NULL);
			else if (list_length($4) == 2)
				$$ = makeItemBinary(escontext, jpiDecimal, linitial($4), lsecond($4));
			else
				ereturn(escontext, false,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid input syntax for type %s", "jsonpath"),
						 errdetail(".decimal() can only have an optional precision[,scale].")));
		}
	| '.' DATETIME_P '(' opt_datetime_template ')'
		{ $$ = makeItemUnary(escontext, jpiDatetime, $4); }
	| '.' TIME_P '(' opt_datetime_precision ')'
		{ $$ = makeItemUnary(escontext, jpiTime, $4); }
	| '.' TIME_TZ_P '(' opt_datetime_precision ')'
		{ $$ = makeItemUnary(escontext, jpiTimeTz, $4); }
	| '.' TIMESTAMP_P '(' opt_datetime_precision ')'
		{ $$ = makeItemUnary(escontext, jpiTimestamp, $4); }
	| '.' TIMESTAMP_TZ_P '(' opt_datetime_precision ')'
		{ $$ = makeItemUnary(escontext, jpiTimestampTz, $4); }
	;

csv_elem:
	INT_P
		{ $$ = makeItemNumeric(escontext, &$1); }
	| '+' INT_P %prec UMINUS
		{ $$ = makeItemUnary(escontext, jpiPlus, makeItemNumeric(escontext, &$2)); }
	| '-' INT_P %prec UMINUS
		{ $$ = makeItemUnary(escontext, jpiMinus, makeItemNumeric(escontext, &$2)); }
	;

csv_list:
	csv_elem						{ $$ = list_make1($1); }
	| csv_list ',' csv_elem			{ $$ = lappend($1, $3); }
	;

opt_csv_list:
	csv_list						{ $$ = $1; }
	| /* EMPTY */					{ $$ = NULL; }
	;

datetime_precision:
	INT_P							{ $$ = makeItemNumeric(escontext, &$1); }
	;

opt_datetime_precision:
	datetime_precision				{ $$ = $1; }
	| /* EMPTY */					{ $$ = NULL; }
	;

datetime_template:
	STRING_P						{ $$ = makeItemString(escontext, &$1); }
	;

opt_datetime_template:
	datetime_template				{ $$ = $1; }
	| /* EMPTY */					{ $$ = NULL; }
	;

key:
	key_name						{ $$ = makeItemKey(escontext, &$1); }
	;

key_name:
	IDENT_P
	| STRING_P
	| TO_P
	| NULL_P
	| TRUE_P
	| FALSE_P
	| IS_P
	| UNKNOWN_P
	| EXISTS_P
	| STRICT_P
	| LAX_P
	| ABS_P
	| SIZE_P
	| TYPE_P
	| FLOOR_P
	| DOUBLE_P
	| CEILING_P
	| DATETIME_P
	| KEYVALUE_P
	| LAST_P
	| STARTS_P
	| WITH_P
	| LIKE_REGEX_P
	| FLAG_P
	| BIGINT_P
	| BOOLEAN_P
	| DATE_P
	| DECIMAL_P
	| INTEGER_P
	| NUMBER_P
	| STRINGFUNC_P
	| TIME_P
	| TIME_TZ_P
	| TIMESTAMP_P
	| TIMESTAMP_TZ_P
	;

method:
	ABS_P							{ $$ = jpiAbs; }
	| SIZE_P						{ $$ = jpiSize; }
	| TYPE_P						{ $$ = jpiType; }
	| FLOOR_P						{ $$ = jpiFloor; }
	| DOUBLE_P						{ $$ = jpiDouble; }
	| CEILING_P						{ $$ = jpiCeiling; }
	| KEYVALUE_P					{ $$ = jpiKeyValue; }
	| BIGINT_P						{ $$ = jpiBigint; }
	| BOOLEAN_P						{ $$ = jpiBoolean; }
	| DATE_P						{ $$ = jpiDate; }
	| INTEGER_P						{ $$ = jpiInteger; }
	| NUMBER_P						{ $$ = jpiNumber; }
	| STRINGFUNC_P					{ $$ = jpiStringFunc; }
	;
%%

/*
 * The helper functions below allocate and fill JsonPathParseItem's of various
 * types.
 */

static JsonPathParseItem *
makeItemType(struct Node *escontext, JsonPathItemType type)
{
	JsonPathParseItem *v = sa_alloc(escontext->sa, sizeof(*v));

	CHECK_FOR_INTERRUPTS();

	v->type = type;
	v->next = NULL;

	return v;
}

static JsonPathParseItem *
makeItemString(struct Node *escontext, JsonPathString *s)
{
	JsonPathParseItem *v;

	if (s == NULL)
	{
		v = makeItemType(escontext, jpiNull);
	}
	else
	{
		v = makeItemType(escontext, jpiString);
		v->value.string.val = s->val;
		v->value.string.len = s->len;
	}

	return v;
}

static JsonPathParseItem *
makeItemVariable(struct Node *escontext, JsonPathString *s)
{
	JsonPathParseItem *v;

	v = makeItemType(escontext, jpiVariable);
	v->value.string.val = s->val;
	v->value.string.len = s->len;

	return v;
}

static JsonPathParseItem *
makeItemKey(struct Node *escontext, JsonPathString *s)
{
	JsonPathParseItem *v;

	v = makeItemString(escontext, s);
	v->type = jpiKey;

	return v;
}

static JsonPathParseItem *
makeItemNumeric(struct Node *escontext, JsonPathString *s)
{
	JsonPathParseItem *v;
	v = makeItemType(escontext, jpiNumeric);

	const char* src = s->val;
	size_t llen = sizeof(lng);
	lng lval;
	lng* plval = &lval;

	size_t dlen = sizeof(dbl);
	dbl dval;
	dbl* pdval = &dval;

	if (lngFromStr(src, &llen, &plval, false) > 0) {
		Numeric num = {.type =YYJSON_SUBTYPE_SINT, .lnum = lval };
		v->value.numeric = num;
		return v;
	}
	else if (dblFromStr(src, &dlen, &pdval, false) > 0) {
		Numeric num = {.type =YYJSON_SUBTYPE_REAL, .dnum = dval };
		v->value.numeric = num;
		return v;
	}

	return v;
}

static JsonPathParseItem *
makeItemBool(struct Node *escontext, bool val)
{
	JsonPathParseItem *v = makeItemType(escontext, jpiBool);

	v->value.boolean = val;

	return v;
}

static JsonPathParseItem *
makeItemBinary(struct Node *escontext, JsonPathItemType type, JsonPathParseItem *la, JsonPathParseItem *ra)
{
	JsonPathParseItem *v = makeItemType(escontext, type);

	v->value.args.left = la;
	v->value.args.right = ra;

	return v;
}

static JsonPathParseItem *
makeItemUnary(struct Node *escontext, JsonPathItemType type, JsonPathParseItem *a)
{
	JsonPathParseItem *v;

	if (type == jpiPlus && a->type == jpiNumeric && !a->next)
		return a;

	if (type == jpiMinus && a->type == jpiNumeric && !a->next)
	{
		v = makeItemType(escontext, jpiNumeric);
		v->value.numeric = numeric_uminus(a->value.numeric);
		return v;
	}

	v = makeItemType(escontext, type);

	v->value.arg = a;

	return v;
}

static JsonPathParseItem *
makeItemList(List *list)
{
	JsonPathParseItem *head,
			   *end;
	ListCell   *cell;

	head = end = (JsonPathParseItem *) linitial(list);

	if (list_length(list) == 1)
		return head;

	/* append items to the end of already existing list */
	while (end->next)
		end = end->next;

	for_each_from(cell, list, 1)
	{
		JsonPathParseItem *c = (JsonPathParseItem *) lfirst(cell);

		end->next = c;
		end = c;
	}

	return head;
}

static JsonPathParseItem *
makeIndexArray(struct Node *escontext, List *list)
{
	JsonPathParseItem *v = makeItemType(escontext, jpiIndexArray);
	ListCell   *cell;
	int			i = 0;

	Assert(list != NIL);
	v->value.array.nelems = list_length(list);

	v->value.array.elems = sa_alloc(escontext->sa, sizeof(v->value.array.elems[0]) *
								  v->value.array.nelems);

	foreach(cell, list)
	{
		JsonPathParseItem *jpi = lfirst(cell);

		Assert(jpi->type == jpiSubscript);

		v->value.array.elems[i].from = jpi->value.args.left;
		v->value.array.elems[i++].to = jpi->value.args.right;
	}

	return v;
}

static JsonPathParseItem *
makeAny(struct Node *escontext, int first, int last)
{
	JsonPathParseItem *v = makeItemType(escontext, jpiAny);

	v->value.anybounds.first = (first >= 0) ? (uint32) first : PG_UINT32_MAX;
	v->value.anybounds.last = (last >= 0) ? (uint32) last : PG_UINT32_MAX;

	return v;
}

/*
static bool
makeItemLikeRegex(JsonPathParseItem *expr, JsonPathString *pattern,
				  JsonPathString *flags, JsonPathParseItem ** result,
				  struct Node *escontext)
{
	JsonPathParseItem *v = makeItemType(jpiLikeRegex);
	int			i;
	int			cflags;

	v->value.like_regex.expr = expr;
	v->value.like_regex.pattern = pattern->val;
	v->value.like_regex.patternlen = pattern->len;
*/
	/* Parse the flags string, convert to bitmask.  Duplicate flags are OK. */
/*
	v->value.like_regex.flags = 0;
	for (i = 0; flags && i < flags->len; i++)
	{
		switch (flags->val[i])
		{
			case 'i':
				v->value.like_regex.flags |= JSP_REGEX_ICASE;
				break;
			case 's':
				v->value.like_regex.flags |= JSP_REGEX_DOTALL;
				break;
			case 'm':
				v->value.like_regex.flags |= JSP_REGEX_MLINE;
				break;
			case 'x':
				v->value.like_regex.flags |= JSP_REGEX_WSPACE;
				break;
			case 'q':
				v->value.like_regex.flags |= JSP_REGEX_QUOTE;
				break;
			default:
				ereturn(escontext, false,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid input syntax for type %s", "jsonpath"),
						 errdetail("Unrecognized flag character \"%.*s\" in LIKE_REGEX predicate.",
								   pg_mblen(flags->val + i), flags->val + i)));
				break;
		}
	}
*/
	/* Convert flags to what pg_regcomp needs */
/*
	if ( !jspConvertRegexFlags(v->value.like_regex.flags, &cflags, escontext))
		 return false;
*/
	/* check regex validity */
/*
	{
		regex_t     re_tmp;
		pg_wchar   *wpattern;
		int         wpattern_len;
		int         re_result;

		wpattern = (pg_wchar *) sa_alloc(escontext->sa, (pattern->len + 1) * sizeof(pg_wchar));
		wpattern_len = pg_mb2wchar_with_len(pattern->val,
											wpattern,
											pattern->len);

		if ((re_result = pg_regcomp(&re_tmp, wpattern, wpattern_len, cflags,
									DEFAULT_COLLATION_OID)) != REG_OKAY)
		{
			char        errMsg[100];

			pg_regerror(re_result, &re_tmp, errMsg, sizeof(errMsg));
			ereturn(escontext, false,
					(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
					 errmsg("invalid regular expression: %s", errMsg)));
		}

		pg_regfree(&re_tmp);
	}

	*result = v;

	return true;
}
*/
/*
 * Convert from XQuery regex flags to those recognized by our regex library.
 */
/*
bool
jspConvertRegexFlags(uint32 xflags, int *result, struct Node *escontext)
{
*/
	/* By default, XQuery is very nearly the same as Spencer's AREs */
/*
	int			cflags = REG_ADVANCED;
*/
	/* Ignore-case means the same thing, too, modulo locale issues */
/*
	if (xflags & JSP_REGEX_ICASE)
		cflags |= REG_ICASE;
*/
	/* Per XQuery spec, if 'q' is specified then 'm', 's', 'x' are ignored */
/*
	if (xflags & JSP_REGEX_QUOTE)
	{
		cflags &= ~REG_ADVANCED;
		cflags |= REG_QUOTE;
	}
	else
	{
*/
		/* Note that dotall mode is the default in POSIX */
/*
		if (!(xflags & JSP_REGEX_DOTALL))
			cflags |= REG_NLSTOP;
		if (xflags & JSP_REGEX_MLINE)
			cflags |= REG_NLANCH;
*/
		/*
		 * XQuery's 'x' mode is related to Spencer's expanded mode, but it's
		 * not really enough alike to justify treating JSP_REGEX_WSPACE as
		 * REG_EXPANDED.  For now we treat 'x' as unimplemented; perhaps in
		 * future we'll modify the regex library to have an option for
		 * XQuery-style ignore-whitespace mode.
		 */
/*
		if (xflags & JSP_REGEX_WSPACE)
			ereturn(escontext, false,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("XQuery \"x\" flag (expanded regular expressions) is not implemented")));
	}

	*result = cflags;

	return true;
}
*/

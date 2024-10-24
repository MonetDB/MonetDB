%top{
/*-------------------------------------------------------------------------
 *
 * jsonpath_scan.l
 *	Lexical parser for jsonpath datatype
 *
 * Splits jsonpath string into tokens represented as JsonPathString structs.
 * Decodes unicode and hex escaped strings.
 *
 * Copyright (c) 2019-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath_scan.l
 *
 *-------------------------------------------------------------------------
 */

/*
 * NB: include jsonpath_gram.h only AFTER including jsonpath_internal.h,
 * because jsonpath_internal.h contains the declaration for JsonPathString.
 */

/* struct JsonPathString is shared between scan and gram */
}

%{

#include "jsonpath_internal.h"

static void addstring(yyscan_t yyscanner, bool init, char *s, int l);
static void addchar(yyscan_t yyscanner, bool init, char c);
static enum yytokentype checkKeyword(yyscan_t yyscanner);
static bool parseUnicode(yyscan_t yyscanner, char *s, int l, struct Node *escontext);
static bool parseHexChar(yyscan_t yyscanner, char *s, struct Node *escontext);

/* LCOV_EXCL_START */
#define YY_USER_INIT (void) result;

typedef struct extra_data {
	JsonPathString scanstring;
} extra_data;


%}

%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option warn
%option prefix="jsonpath_yy"
%option bison-bridge
%option noyyalloc
%option noyyrealloc
%option noyyfree
%option reentrant
%option extra-type="extra_data *"
/*
 * We use exclusive states for quoted and non-quoted strings,
 * quoted variable names and C-style comments.
 * Exclusive states:
 *  <xq> - quoted strings
 *  <xnq> - non-quoted strings
 *  <xvq> - quoted variable names
 *  <xc> - C-style comment
 */

%x xq
%x xnq
%x xvq
%x xc

special		[\?\%\$\.\[\]\{\}\(\)\|\&\!\=\<\>\@\#\,\*:\-\+\/]
blank		[ \t\n\r\f]
/* "other" means anything that's not special, blank, or '\' or '"' */
other		[^\?\%\$\.\[\]\{\}\(\)\|\&\!\=\<\>\@\#\,\*:\-\+\/\\\" \t\n\r\f]

decdigit	[0-9]
hexdigit	[0-9A-Fa-f]
octdigit	[0-7]
bindigit	[0-1]

/* DecimalInteger in ECMAScript; must not start with 0 unless it's exactly 0 */
decinteger	(0|[1-9](_?{decdigit})*)
/* DecimalDigits in ECMAScript; only used as part of other rules */
decdigits	{decdigit}(_?{decdigit})*
/* Non-decimal integers; in ECMAScript, these must not have underscore after prefix */
hexinteger	0[xX]{hexdigit}(_?{hexdigit})*
octinteger	0[oO]{octdigit}(_?{octdigit})*
bininteger	0[bB]{bindigit}(_?{bindigit})*

decimal		({decinteger}\.{decdigits}?|\.{decdigits})
real		({decinteger}|{decimal})[Ee][-+]?{decdigits}
realfail	({decinteger}|{decimal})[Ee][-+]

decinteger_junk	{decinteger}{other}
decimal_junk	{decimal}{other}
real_junk		{real}{other}

unicode		\\u({hexdigit}{4}|\{{hexdigit}{1,6}\})
unicodefail	\\u({hexdigit}{0,3}|\{{hexdigit}{0,6})
hex_char	\\x{hexdigit}{2}
hex_fail	\\x{hexdigit}{0,1}

%%

<xnq>{other}+					{
									addstring(yyscanner, false, yytext, yyleng);
								}

<xnq>{blank}+					{
									yylval->str = yyextra->scanstring;
									BEGIN INITIAL;
									return checkKeyword(yyscanner);
								}

<xnq>\/\*						{
									yylval->str = yyextra->scanstring;
									BEGIN xc;
								}

<xnq>({special}|\")				{
									yylval->str = yyextra->scanstring;
									yyless(0);
									BEGIN INITIAL;
									return checkKeyword(yyscanner);
								}

<xnq><<EOF>>					{
									yylval->str = yyextra->scanstring;
									BEGIN INITIAL;
									return checkKeyword(yyscanner);
								}

<xnq,xq,xvq>\\b				{ addchar(yyscanner, false, '\b'); }

<xnq,xq,xvq>\\f				{ addchar(yyscanner, false, '\f'); }

<xnq,xq,xvq>\\n				{ addchar(yyscanner, false, '\n'); }

<xnq,xq,xvq>\\r				{ addchar(yyscanner, false, '\r'); }

<xnq,xq,xvq>\\t				{ addchar(yyscanner, false, '\t'); }

<xnq,xq,xvq>\\v				{ addchar(yyscanner, false, '\v'); }

<xnq,xq,xvq>{unicode}+		{
								if (!parseUnicode(yyscanner, yytext, yyleng, escontext))
									yyterminate();
							}

<xnq,xq,xvq>{hex_char}		{
								if (!parseHexChar(yyscanner, yytext, escontext))
									yyterminate();
							}

<xnq,xq,xvq>{unicode}*{unicodefail} {
								jsonpath_yyerror(yyscanner, NULL, escontext,
												 "invalid Unicode escape sequence");
								yyterminate();
							}

<xnq,xq,xvq>{hex_fail}		{
								jsonpath_yyerror(yyscanner, NULL, escontext,
												 "invalid hexadecimal character sequence");
								yyterminate();
							}

<xnq,xq,xvq>{unicode}+\\	{
								/* throw back the \\, and treat as unicode */
								yyless(yyleng - 1);
								if (!parseUnicode(yyscanner, yytext, yyleng, escontext))
									yyterminate();
							}

<xnq,xq,xvq>\\.				{ addchar(yyscanner, false, yytext[1]); }

<xnq,xq,xvq>\\				{
							  jsonpath_yyerror(yyscanner, NULL, escontext,
											   "unexpected end after backslash");
							  yyterminate();
							}

<xq,xvq><<EOF>>				{
							  jsonpath_yyerror(yyscanner, NULL, escontext,
											   "unterminated quoted string");
							  yyterminate();
							}

<xq>\"							{
									yylval->str = yyextra->scanstring;
									BEGIN INITIAL;
									return STRING_P;
								}

<xvq>\"							{
									yylval->str = yyextra->scanstring;
									BEGIN INITIAL;
									return VARIABLE_P;
								}

<xq,xvq>[^\\\"]+				{ addstring(yyscanner, false, yytext, yyleng); }

<xc>\*\/						{ BEGIN INITIAL; }

<xc>[^\*]+						{ }

<xc>\*							{ }

<xc><<EOF>>						{
									jsonpath_yyerror(yyscanner,
										NULL, escontext,
										"unexpected end of comment");
									yyterminate();
								}
\&\&							{ return AND_P; }

\|\|							{ return OR_P; }

\!								{ return NOT_P; }

\*\*							{ return ANY_P; }

\<								{ return LESS_P; }

\<\=							{ return LESSEQUAL_P; }

\=\=							{ return EQUAL_P; }

\<\>							{ return NOTEQUAL_P; }

\!\=							{ return NOTEQUAL_P; }

\>\=							{ return GREATEREQUAL_P; }

\>								{ return GREATER_P; }

\${other}+						{
									addstring(yyscanner, true, yytext + 1, yyleng - 1);
									addchar(yyscanner, false, '\0');
									yylval->str = yyextra->scanstring;
									return VARIABLE_P;
								}

\$\"							{
									addchar(yyscanner, true, '\0');
									BEGIN xvq;
								}

{special}						{ return *yytext; }

{blank}+						{ /* ignore */ }

\/\*							{
									addchar(yyscanner, true, '\0');
									BEGIN xc;
								}

{real}							{
									addstring(yyscanner, true, yytext, yyleng);
									addchar(yyscanner, false, '\0');
									yylval->str = yyextra->scanstring;
									return NUMERIC_P;
								}

{decimal}						{
									addstring(yyscanner, true, yytext, yyleng);
									addchar(yyscanner, false, '\0');
									yylval->str = yyextra->scanstring;
									return NUMERIC_P;
								}

{decinteger}					{
									addstring(yyscanner, true, yytext, yyleng);
									addchar(yyscanner, false, '\0');
									yylval->str = yyextra->scanstring;
									return INT_P;
								}

{hexinteger}					{
									addstring(yyscanner, true, yytext, yyleng);
									addchar(yyscanner, false, '\0');
									yylval->str = yyextra->scanstring;
									return INT_P;
								}

{octinteger}					{
									addstring(yyscanner, true, yytext, yyleng);
									addchar(yyscanner, false, '\0');
									yylval->str = yyextra->scanstring;
									return INT_P;
								}

{bininteger}					{
									addstring(yyscanner, true, yytext, yyleng);
									addchar(yyscanner, false, '\0');
									yylval->str = yyextra->scanstring;
									return INT_P;
								}

{realfail}						{
									jsonpath_yyerror(yyscanner,
										NULL, escontext,
										"invalid numeric literal");
									yyterminate();
								}
{decinteger_junk}				{
									jsonpath_yyerror(yyscanner,
										NULL, escontext,
										"trailing junk after numeric literal");
									yyterminate();
								}
{decimal_junk}					{
									jsonpath_yyerror(yyscanner,
										NULL, escontext,
										"trailing junk after numeric literal");
									yyterminate();
								}
{real_junk}						{
									jsonpath_yyerror(yyscanner,
										NULL, escontext,
										"trailing junk after numeric literal");
									yyterminate();
								}
\"								{
									addchar(yyscanner, true, '\0');
									BEGIN xq;
								}

\\								{
									yyless(0);
									addchar(yyscanner, true, '\0');
									BEGIN xnq;
								}

{other}+						{
									addstring(yyscanner, true, yytext, yyleng);
									BEGIN xnq;
								}

<<EOF>>							{ yyterminate(); }

%%

/* LCOV_EXCL_STOP */

void
jsonpath_yyerror(yyscan_t yyscanner, JsonPathParseResult **result, struct Node *escontext,
				 const char *message)
{
	(void) result;
	/* don't overwrite escontext if it's already been set */
	if (SOFT_ERROR_OCCURRED(escontext))
		return;
	if (*yyget_text(yyscanner) == YY_END_OF_BUFFER_CHAR)
	{
		errsave(escontext,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 /* translator: %s is typically "syntax error" */
				 errmsg("%s at end of jsonpath input", _(message))));
	}
	else
	{
		errsave(escontext,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 /* translator: first %s is typically "syntax error" */
				 errmsg("%s at or near \"%s\" of jsonpath input",
						_(message), yyget_text(yyscanner))));
	}
}

typedef struct JsonPathKeyword
{
	int16		len;
	bool		lowercase;
	int			val;
	const char *keyword;
} JsonPathKeyword;

/*
 * Array of key words should be sorted by length and then
 * alphabetical order
 */
static const JsonPathKeyword keywords[] = {
	{ 2, false,	IS_P,		"is"},
	{ 2, false,	TO_P,		"to"},
	{ 3, false,	ABS_P,		"abs"},
	{ 3, false,	LAX_P,		"lax"},
	{ 4, false,	DATE_P,		"date"},
	{ 4, false,	FLAG_P,		"flag"},
	{ 4, false,	LAST_P,		"last"},
	{ 4, true,	NULL_P,		"null"},
	{ 4, false,	SIZE_P,		"size"},
	{ 4, false,	TIME_P,		"time"},
	{ 4, true,	TRUE_P,		"true"},
	{ 4, false,	TYPE_P,		"type"},
	{ 4, false,	WITH_P,		"with"},
	{ 5, true,	FALSE_P,	"false"},
	{ 5, false,	FLOOR_P,	"floor"},
	{ 6, false,	BIGINT_P,	"bigint"},
	{ 6, false,	DOUBLE_P,	"double"},
	{ 6, false,	EXISTS_P,	"exists"},
	{ 6, false,	NUMBER_P,	"number"},
	{ 6, false,	STARTS_P,	"starts"},
	{ 6, false,	STRICT_P,	"strict"},
	{ 6, false,	STRINGFUNC_P, "string"},
	{ 7, false,	BOOLEAN_P,	"boolean"},
	{ 7, false,	CEILING_P,	"ceiling"},
	{ 7, false,	DECIMAL_P,	"decimal"},
	{ 7, false,	INTEGER_P,	"integer"},
	{ 7, false,	TIME_TZ_P,	"time_tz"},
	{ 7, false,	UNKNOWN_P,	"unknown"},
	{ 8, false,	DATETIME_P,	"datetime"},
	{ 8, false,	KEYVALUE_P,	"keyvalue"},
	{ 9, false,	TIMESTAMP_P, "timestamp"},
	{ 10,false, LIKE_REGEX_P, "like_regex"},
	{ 12,false, TIMESTAMP_TZ_P, "timestamp_tz"},
};

/* Check if current scanstring value is a keyword */
static enum yytokentype
checkKeyword(yyscan_t yyscanner)
{
	int			res = IDENT_P;
	int			diff;
	const JsonPathKeyword  *StopLow = keywords,
						   *StopHigh = keywords + lengthof(keywords),
						   *StopMiddle;

	if (yyget_extra(yyscanner)->scanstring.len > keywords[lengthof(keywords) - 1].len)
		return res;

	while (StopLow < StopHigh)
	{
		StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);

		if (StopMiddle->len == yyget_extra(yyscanner)->scanstring.len)
			diff = pg_strncasecmp(StopMiddle->keyword, yyget_extra(yyscanner)->scanstring.val,
								  yyget_extra(yyscanner)->scanstring.len);
		else
			diff = StopMiddle->len - yyget_extra(yyscanner)->scanstring.len;

		if (diff < 0)
			StopLow = StopMiddle + 1;
		else if (diff > 0)
			StopHigh = StopMiddle;
		else
		{
			if (StopMiddle->lowercase)
				diff = strncmp(StopMiddle->keyword, yyget_extra(yyscanner)->scanstring.val,
							   yyget_extra(yyscanner)->scanstring.len);

			if (diff == 0)
				res = StopMiddle->val;

			break;
		}
	}

	return res;
}


/* Interface to jsonpath parser */
JsonPathParseResult *
parsejsonpath(const char *str, int len, allocator* sa, char* errmsg)
{
	struct Node escontext;
	escontext.sa = sa;
	escontext._errmsg = errmsg;
	JsonPathParseResult	*parseresult;

	yyscan_t yyscanner = NULL;
	extra_data ed = {0};

	yylex_init_extra(&ed, &yyscanner);

	if (len <= 0)
		len = strlen(str);
	yy_scan_bytes(str, len, yyscanner);

	if (jsonpath_yyparse(yyscanner, (void *) &parseresult, &escontext) != 0)
		jsonpath_yyerror(yyscanner, NULL, &escontext, "invalid input"); /* shouldn't happen */

	GDKfree(ed.scanstring.val);

	yylex_destroy(yyscanner);

	return parseresult;
}

/*
 * Resize yyget_extra(yyscanner)->scanstring so that it can append string of given length.
 * Reinitialize if required.
 */
static void
resizeString(yyscan_t yyscanner, bool init, int appendLen)
{
	if (init)
	{
		yyget_extra(yyscanner)->scanstring.total = Max(32, appendLen);
		yyget_extra(yyscanner)->scanstring.val = (char *) GDKmalloc(yyget_extra(yyscanner)->scanstring.total);
		yyget_extra(yyscanner)->scanstring.len = 0;
	}
	else
	{
		if (yyget_extra(yyscanner)->scanstring.len + appendLen >= yyget_extra(yyscanner)->scanstring.total)
		{
			while (yyget_extra(yyscanner)->scanstring.len + appendLen >= yyget_extra(yyscanner)->scanstring.total)
				yyget_extra(yyscanner)->scanstring.total *= 2;
			yyget_extra(yyscanner)->scanstring.val = GDKrealloc(yyget_extra(yyscanner)->scanstring.val, yyget_extra(yyscanner)->scanstring.total);
		}
	}
}

/* Add set of bytes at "s" of length "l" to yyget_extra(yyscanner)->scanstring */
static void
addstring(yyscan_t yyscanner, bool init, char *s, int l)
{
	resizeString(yyscanner, init, l + 1);
	memcpy(yyget_extra(yyscanner)->scanstring.val + yyget_extra(yyscanner)->scanstring.len, s, l);
	yyget_extra(yyscanner)->scanstring.len += l;
}

/* Add single byte "c" to yyget_extra(yyscanner)->scanstring */
static void
addchar(yyscan_t yyscanner, bool init, char c)
{
	resizeString(yyscanner, init, 1);
	yyget_extra(yyscanner)->scanstring.val[yyget_extra(yyscanner)->scanstring.len] = c;
	if (c != '\0')
		yyget_extra(yyscanner)->scanstring.len++;
}


/* Turn hex character into integer */
static bool
hexval(yyscan_t yyscanner, char c, int *result, struct Node *escontext)
{
	if (c >= '0' && c <= '9')
	{
		*result = c - '0';
		return true;
	}
	if (c >= 'a' && c <= 'f')
	{
		*result = c - 'a' + 0xA;
		return true;
	}
	if (c >= 'A' && c <= 'F')
	{
		*result = c - 'A' + 0xA;
		return true;
	}
	jsonpath_yyerror(yyscanner, NULL, escontext, "invalid hexadecimal digit");
	return false;
}

/* Add given unicode character to scanstring */
static bool
addUnicodeChar(yyscan_t yyscanner, int ch, struct Node *escontext)
{
	if (ch == 0)
	{
		/* We can't allow this, since our TEXT type doesn't */
		ereturn(escontext, false,
				(errcode(ERRCODE_UNTRANSLATABLE_CHARACTER),
				 errmsg("unsupported Unicode escape sequence"),
				  errdetail("\\u0000 cannot be converted to text.")));
	}
	else
	{
		char		cbuf[MAX_UNICODE_EQUIVALENT_STRING + 1];

		/*
		 * If we're trapping the error status, call the noerror form of the
		 * conversion function. Otherwise call the normal form which provides
		 * more detailed errors.
		 */
		if (!pg_unicode_to_server_noerror(ch, (unsigned char *) cbuf))
			ereturn(escontext, false,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("could not convert Unicode to server encoding")));
		addstring(yyscanner, false, cbuf, strlen(cbuf));
	}
	return true;
}

/* Add unicode character, processing any surrogate pairs */
static bool
addUnicode(yyscan_t yyscanner, int ch, int *hi_surrogate, struct Node *escontext)
{
	if (is_utf16_surrogate_first(ch))
	{
		if (*hi_surrogate != -1)
			ereturn(escontext, false,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "jsonpath"),
					 errdetail("Unicode high surrogate must not follow "
							   "a high surrogate.")));
		*hi_surrogate = ch;
		return true;
	}
	else if (is_utf16_surrogate_second(ch))
	{
		if (*hi_surrogate == -1)
			ereturn(escontext, false,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "jsonpath"),
					 errdetail("Unicode low surrogate must follow a high "
							   "surrogate.")));
		ch = surrogate_pair_to_codepoint(*hi_surrogate, ch);
		*hi_surrogate = -1;
	}
	else if (*hi_surrogate != -1)
	{
		ereturn(escontext, false,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s", "jsonpath"),
				 errdetail("Unicode low surrogate must follow a high "
						   "surrogate.")));
	}

	return addUnicodeChar(yyscanner, ch, escontext);
}

/*
 * parseUnicode was adopted from json_lex_string() in
 * src/backend/utils/adt/json.c
 */
static bool
parseUnicode(yyscan_t yyscanner, char *s, int l, struct Node *escontext)
{
	int			i = 2;
	int			hi_surrogate = -1;

	for (i = 2; i < l; i += 2)	/* skip '\u' */
	{
		int			ch = 0;
		int			j, si;

		if (s[i] == '{')	/* parse '\u{XX...}' */
		{
			while (s[++i] != '}' && i < l)
			{
				if (!hexval(yyscanner, s[i], &si, escontext))
					return false;
				ch = (ch << 4) | si;
			}
			i++;	/* skip '}' */
		}
		else		/* parse '\uXXXX' */
		{
			for (j = 0; j < 4 && i < l; j++)
			{
				if (!hexval(yyscanner, s[i++], &si, escontext))
					return false;
				ch = (ch << 4) | si;
			}
		}

		if (! addUnicode(yyscanner, ch, &hi_surrogate, escontext))
			return false;
	}

	if (hi_surrogate != -1)
	{
		ereturn(escontext, false,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s", "jsonpath"),
				 errdetail("Unicode low surrogate must follow a high "
						   "surrogate.")));
	}

	return true;
}

/* Parse sequence of hex-encoded characters */
static bool
parseHexChar(yyscan_t yyscanner, char *s, struct Node *escontext)
{
	int s2, s3, ch;
	if (!hexval(yyscanner, s[2], &s2, escontext))
		return false;
	if (!hexval(yyscanner, s[3], &s3, escontext))
		return false;

	ch = (s2 << 4) | s3;

	return addUnicodeChar(yyscanner, ch, escontext);
}

/*
 * Interface functions to make flex use GDKmalloc() instead of malloc().
 * It'd be better to make these static, but flex insists otherwise.
 */

void *
jsonpath_yyalloc(yy_size_t bytes, yyscan_t yyscanner)
{
	(void) yyscanner;
	return GDKmalloc(bytes);
}

void *
jsonpath_yyrealloc(void *ptr, yy_size_t bytes, yyscan_t yyscanner)
{
	(void) yyscanner;
	return GDKrealloc(ptr, bytes);;
}

void
jsonpath_yyfree(void *ptr, yyscan_t yyscanner)
{
	(void) yyscanner;
	GDKfree(ptr);
}

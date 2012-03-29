%option reentrant
%option bison-bridge
%option bison-locations
%option noyywrap
%option yylineno
%option case-insensitive
%option batch
%option nostdinit
%option nodefault
%option fast
%option extra-type="struct _jc *"

%{
#include "jaql.tab.h"
#include "jaql.h"

YYSTYPE yylval;
extern void jaqlerror(YYLTYPE* locp, jc* j, char const *msg);

/* set line numbers each time a token is recognised */
#define YY_USER_ACTION yylloc->first_line = yylineno;

#define YY_INPUT(buf, res, max_size) readinput(yyextra, buf, &res, max_size)
#define YY_NO_INPUT

static void readinput(jc *j, char *buf, int *res, size_t max_size) {
	if (j->buf != NULL && *j->buf != '\0') {
		*res = strlen(j->buf);
		if ((size_t)*res > max_size)
			*res = (int)max_size;
		memcpy(buf, j->buf, *res);
		j->buf += *res;
	} else {
		*res = YY_NULL;
	}
}
%}

 /* allow to disregard JSON bits */
%x ARR OBJ

%%
	/* if the parser expects a JSON bit, give it */
	if (yyextra->expect_json == '[') {
		yyextra->esc_depth = 1;
		yyextra->expect_json = 0;
		BEGIN(ARR);
	} else if (yyextra->expect_json == '{') {
		yyextra->esc_depth = 1;
		yyextra->expect_json = 0;
		BEGIN(OBJ);
	}

 /* core language */
each       return EACH;
filter     return FILTER;
transform  return TRANSFORM;
expand     return EXPAND;
unroll     return UNROLL;
group      return GROUP;
into       return INTO;
by         return BY;
as         return AS;
join       return JOIN;
preserve   return PRESERVE;
where      return WHERE;
in         return IN;
sort       return SORT;
top        return TOP;
desc       return DESC;
asc        return ASC;

 /* debugging aids */
explain    return EXPLAIN;
plan       return PLAN;
planf      return PLANF;
debug      return DEBUG;

 /* language constructs */
"->"       return _ARROW;
-?[0-9]+               {yylval->j_number = atol(yytext); return _NUMBER;}
-?[0-9]+\.[0-9e+-]+    {yylval->j_double = atof(yytext); return _DOUBLE;}
\"[^\"]*\"             {yytext[yyleng - 1] = '\0';
                        yylval->j_string = GDKstrdup(yytext + 1);
                        return _STRING;}
\$         return _DOLLAR;
=          return _ASSIGN;
==         return _EQUALS;
!=         return _NEQUAL;
">"        return _GREATER;
">="       return _GEQUAL;
"<"        return _LESS;
"<="       return _LEQUAL;
not        return _NOT;
and        return _AND;
or         return _OR;
true       return _TRUE;
false      return _FALSE;
;          return _SCOLON;
\.         return _DOT;
":"        return ':';
","        return ',';
"("        return '(';
")"        return ')';
"+"        return '+';
"-"        return '-';
"*"        return '*';
"/"        return '/';
<ARR>{
	"]"   %{
		if (--yyextra->esc_depth == 0) {
			yylval->j_json = GDKstrdup(yytext - 1);
			BEGIN(INITIAL);
			return _ARRAY;
		} else {
			yymore();
		}
	%}
	"["   yymore(); yyextra->esc_depth++;
	.     yymore();
}
"]"        return ']';
"["        return '[';
<OBJ>{
	"}"   %{
		if (--yyextra->esc_depth == 0) {
			yylval->j_json = GDKstrdup(yytext - 1);
			BEGIN(INITIAL);
			return _OBJECT;
		} else {
			yymore();
		}
	%}
	"{"   yymore(); yyextra->esc_depth++;
	.     yymore();
}
"}"        return '}';
"{"        return '{';
[ \t\r\n]+ /* ignore whitespace */;
("#"|"//").*\n /* ignore comments */;
[a-zA-Z_][a-zA-Z0-9_]* {yylval->j_ident = GDKstrdup(yytext); return _IDENT;}
<<EOF>>    {yyextra->buf = NULL; return EOF;}
.                      {
	char buf[32];
	snprintf(buf, sizeof(buf), "unexpected character: %c", yytext[0]);
	jaqlerror(yylloc_param, yyextra, buf);
	yyextra->buf = NULL;
	YY_FLUSH_BUFFER;
	BEGIN(0);
	return EOF;
	}
%%

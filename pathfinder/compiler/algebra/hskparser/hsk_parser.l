%{
/**
 * Performs lexical analysis of the output of the Haskell XQuery-to-
 * Algebra Mapper.
 */

#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "pathfinder.h"
#include "mem.h"
#include "oops.h"
#include "algebra.h"
#include "hsk_parser.tab.h"

/** avoid `defined but not used' warnings */
#define YY_NO_UNPUT

/* override YYLTYPE definition */
#define YYLTYPE PFloc_t

/* bison 1.33 does not include this line in hsk_parser.tab.h.
 * So we do it here.
 */
extern YYLTYPE yylloc;

/** increment row and column numbers (required in YY_USER_ACTION) */
static int i;

/** how many spaces make up one tabstop */
#define TAB_STEP 8

/**
 * Executed (in the flex-generated scanner) each time a rule matches,
 * after yytext and yyleng were set, but before the action is
 * triggered.  Used to calculate length of a token and to increment
 * the line number in case of newline commands.
 */ 
#define YY_USER_ACTION                                      \
 do {                                                       \
   yylloc.first_col = yylloc.last_col + 1;                  \
   yylloc.first_row = yylloc.last_row;                      \
                                                            \
   for (i = 0; i < hskleng; i++) {                          \
     if (hsktext[i] == '\n') {                              \
       yylloc.last_col = 0;                                 \
       yylloc.last_row++;                                   \
     }                                                      \
     else {                                                 \
       yylloc.last_col++;                                   \
     }                                                      \
   }                                                        \
 } while(0);

/* used to copy yytext to */
char *s = NULL;

int hskwrap(void)
{
 return 1;
}

%}

%s XPMODE ATTMODE TYPEMODE RELMODE ANY

/* integers consist of at least one digit */
digits [0-9]+


/* Regular expressions */

%%
<INITIAL>
{
"["                    {return LSQBR;}
"]"                    {return RSQBR;}
"("                    {return LBRACK;}
")"                    {return RBRACK;}
":"                    {return COLON;}
","                    {return COMMA;}
"/"                    {return DIVIDE;}


"+"                    {return PLUS;}
"*"                    {return TIMES;}
"-"                    {return MINUS;}
div                    {return DIV;}
"<"                    {return LT;}
">"                    {return GT;}
"="                    {return EQUAL;}
NOT                    {return NOT;}
AND                    {return AND;}
OR                     {return OR;}
NEG                    {return NEG;}


ROW\#                  {return ROWNUM;}
"¶"                    {return PROJ;}
"|X|"                  {return EQJOIN;}
"/|"                   {BEGIN(XPMODE); return SCJOIN;}
"×"                    {return CROSSPR;}
"U"                    {return UNION;}
SEL                    {return SEL;}
TYPE                   {return TYPE;}
TBL                    {BEGIN(ATTMODE); return TBL;}
ELEM                   {return ELEM;}
REL                    {BEGIN(RELMODE); return REL;}
SUM                    {return SUM;}
COUNT                  {return COUNT;}
"\\\\"                 {return DIFF;}
DIST                   {return DIST;}
CINT                   {return CINT;}
CSTR                   {return CSTR;}
CDEC                   {return CDEC;}
CDBL                   {return CDBL;}
TEXT                   {return TEXT;}
![01]                  {/* representation of boolean values */
                        /* Delete ! character. */
                        s = PFstrndup(hsktext + 1, hskleng - 1);
                        hsklval.ival=atoi(s);
                        return BLN;}
@{digits}             {/* representation of node references */
                        /* Delete @ character. */
                        s = PFstrndup(hsktext + 1, hskleng - 1);
                        hsklval.ival=atoi(s);
                        return NODEREF;}
&{digits}             {/* representation of unsigned integers */
                        /* Delete & character. */
                        s = PFstrndup(hsktext + 1, hskleng - 1);
                        hsklval.ival=atoi(s);
                        return NAT;}
{digits}               {/* representation of integers */
                        s = PFstrdup(hsktext);
                        hsklval.ival=atoi(s);
                        return INT;}
"%"{digits}"."{digits} {/* representation of decimals */
                        /* Delete % character. */
                        s = PFstrndup(hsktext + 1, hskleng - 1);
                        hsklval.fval=(float)atof(s);
                        return DEC;}
"#"(({digits}"."{digits}([eE][+-]?{digits})?)|({digits}[eE][+-]?{digits})) {
                        /* representation of doubles */
                        /* Delete # character. */
                        s = PFstrndup(hsktext + 1, hskleng - 1);
                        hsklval.dval=atof(s);
                        return DBL;}
[a-zA-Z0-9._ø]+        {/* representation of strings */
                        s = PFstrdup(hsktext);
                        hsklval.attval = s;
                        return ATTREF;
                       }
\"[^\"]*\"             {/* Remove quotes. */
                        s = PFstrndup(hsktext + 1, hskleng - 2);
                        s[hskleng - 2] = '\0';
                        hsklval.sval = s;
                        return STR;
                       }
}


<XPMODE>
{
"["                    {BEGIN(INITIAL); return LSQBR;}
"::"                   {return DBLCOLON;}
descendant             {return DESC;}
descendant\-or\-self   {return DESCSELF;}
ancestor               {return ANC;}
ancestor\-or\-self     {return ANCSELF;}
following              {return FOL;}
preceding              {return PREC;}
following\-sibling     {return FOLSIBL;}
preceding\-sibling     {return PRECSIBL;}
child                  {return CHILD;}
parent                 {return PARENT;}
self                   {return SELF;}
attribute              {return ATTRIB;}
"*"                    {return XMLELEM;}
node\(\)               {return NODES;}
text\(\)               {return TEXT;}
[_a-zA-Z][_a-zA-Z0-9]* {s = PFstrdup(hsktext);
                        hsklval.sval = s;
                        return TAG;
                       }
}


<ATTMODE>
{
"("                    {return LBRACK;}
")"                    {BEGIN(INITIAL); return RBRACK;}
","                    {return COMMA;}
[a-zA-Z0-9_]+          {BEGIN(TYPEMODE);
                        s = PFstrdup(hsktext);
                        hsklval.sval = s;
                        return SCHMATT;
                       }
}


<TYPEMODE>
{
","                    {return COMMA;}
")"                    {BEGIN(ATTMODE); return RBRACK;}
[a-zA-Z0-9_ø]+         {s = PFstrdup(hsktext);
                        hsklval.sval = s;
                        return SCHMTYPE;
                       }
}


<RELMODE>[a-zA-Z0-9._]+ {BEGIN(INITIAL);
                         s = PFstrdup(hsktext);
                         hsklval.sval = s;
                         return RELREF;
                        }


<ANY>
{
\n                     {continue;}
.                      {continue;}
}


" "                    {continue;}
\n                     {/* After Haskell algebra output, there follows
                         * a newline and then the result of the XQuery
                         * statement. This keeps the additional output
                         * from being scanned and parsed.
                         */
                        BEGIN(ANY); continue;}

.                      {
                         PFoops_loc (OOPS_PARSE, yylloc,
                                     "illegal token `%s'", hsktext);
                       }

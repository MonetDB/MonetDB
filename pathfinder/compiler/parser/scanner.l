/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/* Hey Emacs, this is -*-C-*- */

/**
 * @file
 *
 * XQuery (W3C WD October 29, 2004) terminals and lexer state machine.
 * Feed this into `flex'.
 *
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the Pathfinder system.
 * 
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2006 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

%{

/*
#define DEBUG_SCANNER
#define DEBUG_LEXSTATE
*/

/**
 * This is a implementation of the W3C document ``XQuery 1.0:
 * An XML Query Language'', W3C Working Draft October 29, 2004.
 */

#include "pathfinder.h"

/* atoi, atof, strtoul */
#include <stdlib.h>
#include <assert.h>
/* strncmp */
#include <string.h>
/* isspace */
#include <ctype.h>

/* import XQuery abstract syntax definitions */
#include "abssyn.h"
/* arrays for lexical stack */
#include "array.h"
/* PFstrdup */
#include "mem.h"
/* PFoops */
#include "oops.h"

#include "scanner.h"

/* override YYLTYPE definition in "parser.tab.h" */
#define YYLTYPE PFloc_t

/* import tokens from parser specification */
#include "parser.tab.h"

/* bison 1.33 does not include this line in XQuery.tab.h. So we do it here. */
extern YYLTYPE pflloc;

/* input buffer */
extern char *pfinput;

/** how many spaces make up one tabstop */
#define TAB_STEP 8

/**
 * Executed (in the flex-generated scanner) each time a rule matches,
 * after yytext and yyleng were set, but before the action is
 * triggered.  Used to calculate length of a token (esp. in the
 * presence of tabstops) and to increment the line number in case of
 * newline commands.
 */ 
#define YY_USER_ACTION                                      \
 do {                                                       \
   int i;                                                   \
   pflloc.first_col = pflloc.last_col + 1;                  \
   pflloc.first_row = pflloc.last_row;                      \
                                                            \
   for (i = 0; i < yyleng; i++) {                           \
     if (yytext[i] == '\n') {                               \
       pflloc.last_col = 0;                                 \
       pflloc.last_row++;                                   \
     }                                                      \
     else if (yytext[i] == '\t') {                          \
       pflloc.last_col = ((pflloc.last_col + TAB_STEP) /    \
                          TAB_STEP) * TAB_STEP;             \
     }                                                      \
     else {                                                 \
       pflloc.last_col++;                                   \
     }                                                      \
   }                                                        \
 } while(0);

#define YY_INPUT(buf,result,max_size)                       \
 { int c = *pfinput;                                        \
   pfinput += (c != 0);                                     \
   result = (c != 0) ? (buf[0] = c, 1) : YY_NULL;           \
 }

/**
 * define DEBUG_SCANNER to activate debugging output in parser
 * (if set, every token received is printed to stderr)
 */
#ifdef DEBUG_SCANNER
#define yield(t) PFlog ("token: " #t); return (t)
#else
#define yield(t) return (t)
#endif

/* lexical state stack */
static PFarray_t *lexical_stack = NULL;
static unsigned int lexical_sp = 0;

/** initialize the lexical state stack when scanner starts up 
#define YY_USER_INIT lexical_sp = 0; lexical_stack = PFarray (sizeof (int))
*/

void
lexical_init()
{
        lexical_sp = 0; 
        lexical_stack = PFarray (sizeof (int));
}

   
static void
popState (void)
{
    if (lexical_sp == 0) {
        PFoops (OOPS_PARSE, "parsing problem: stack empty\n");
        PFexit(-1);
    }
        
#ifdef DEBUG_LEXSTATE
    fprintf (stderr, "popState: going to state %i\n",
                     *((int *) PFarray_at (lexical_stack, lexical_sp - 1)));
#endif

    lexical_sp--;
    BEGIN(*((int *) PFarray_at (lexical_stack, lexical_sp)));
}

static void
pushState (int s)
{
#ifdef DEBUG_LEXSTATE
    PFlog ("pushState: pushing state %i\n", s);
#endif

    *((int *) PFarray_at (lexical_stack, lexical_sp++)) = s;
}

static void
gotoState (int s)
{
#ifdef DEBUG_LEXSTATE
    PFlog ("gotoState: going to state %i\n", s);
#endif

    BEGIN (s);
}

/**
 * Initialize lexical scanner before parsing the in-memory string
 * pointed to by @a buf.
 */
void
PFscanner_init (char *buf)
{
    /* initialize lexical stack */
    lexical_sp = 0; 
    lexical_stack = PFarray (sizeof (int));

    /*
     * In case yyin/pfin is not set, initialize it to /dev/null (Unix)
     * resp. C:nul (Windows); otherwise it gets initialized to stdin,
     * which might conflict with Mserver's console:
     * The lexer eventually calls fileno() for yyin/pfin, which in
     * case of yyin/pfin=stdin might be blocked as there is a waiting
     * select/read on the console(=stdin)...
     */
    if (!pfin) {
#ifdef __MINGW32__              /* or any kind of native Windows */
        pfin = fopen("C:nul","r");
#else
        pfin = fopen("/dev/null","r");
#endif
    }
    if (pfinput) {
        YY_FLUSH_BUFFER;
        gotoState(0);
    }

    *PFerrbuf = 0;
    pfinput = buf;
    pflineno = 1;
}

/* forward declarations */
static char entity_ref (const char *s, char **endptr);
static char *extract_string (const char *s);
static PFqname_t extract_qname (const char *s, unsigned int index);
static char *extract_ncname (const char *s, unsigned int index);

%}

/* no wrapping after EOF; hence, no libfl (-lfl) needed */
%option noyywrap

/* we need the `unput' feature of flex */
%option unput

/* provide token line number to the parser */
%option yylineno

/*
 * We don't use the REJECT command and indicate this here. Unfortunately,
 * flex still includes the code to support REJECT, resulting in the compiler
 * warning `label `find_rule' defined but not used'.
 */
%option noreject

/* lexical states */
/*   1 DEFAULT                         */
/*   2 OPERATOR                        */
/*   3 NAMESPACEDECL                   */
/*   4 NAMESPACEKEYWORD                */
/*   5 VARNAME                         */
/*   6 SINGLETYPE                      */
/*   7 ITEMTYPE                        */
/*   8 OCCURRENCEINDICATOR             */
/*   9 KINDTEST                        */
/*  10 KINDTESTFORPI                   */
/*  11 START_TAG                       */
/*  12 QUOT_ATTRIBUTE_CONTENT          */
/*  13 APOS_ATTRIBUTE_CONTENT          */
/*  14 ELEMENT_CONTENT                 */
/*  15 XML_COMMENT                     */
/*  16 PROCESSING_INSTRUCTION          */
/*  17 PROCESSING_INSTRUCTION_CONTENT  */
/*  18 CDATA_SECTION                   */
/*  19 END_TAG                         */

%x DEFAULT
%x OPERATOR
%x NAMESPACEDECL
%x NAMESPACEKEYWORD
%x VARNAME
%x SINGLETYPE
%x ITEMTYPE
%x OCCURRENCEINDICATOR
%x KINDTEST
%x KINDTESTFORPI
%x START_TAG
%x QUOT_ATTRIBUTE_CONTENT
%x APOS_ATTRIBUTE_CONTENT
%x ELEMENT_CONTENT
%x XML_COMMENT
%x PROCESSING_INSTRUCTION
%x PROCESSING_INSTRUCTION_CONTENT
%x CDATA_SECTION
%x END_TAG

/*
 * XQuery 1.0 syntactic constructs
 *
 * lex/flex currently only supports 8 bit charsets. The character classes
 * are thus limited to the lower 8 bits.
 */

/* XML Character Classes (http://www.w3.org/TR/REC-xml/#CharClasses) */
/* 8 bit characters from the Letter class
   see http://www.w3.org/TR/REC-xml/#NT-Letter */
Letter                           [\x41-\x5a\x61-\x7a\xc0-\xd6\xd8-\xf6\xf8-\xff]
/* 8 bit characters from the Digit class
   see http://www.w3.org/TR/REC-xml/#NT-Digit */
Digit                            [\x30-\x39]
Digits                           {Digit}+
/* 0x00B7 is the only 8 bit Extender char
   see http://www.w3.org/TR/REC-xml/#NT-Extender */
Extender                         "\xb7"

/* An XML Name, minus the ":"
   see http://www.w3.org/TR/REC-xml-names/#NT-NCName */
NCName                           ({Letter}|"_"){NCNameChar}*
NCNameChar                       {Letter}|{Digit}|"."|"-"|"_"|{Extender}

/* QName: http://www.w3.org/TR/REC-xml-names/#NT-QName */
QName                            ({Prefix}":")?{LocalPart}
Prefix                           {NCName}
LocalPart                        {NCName}

/* Whitespace http://www.w3.org/TR/REC-xml/#NT-S */
S                                {WhiteSpaceChar}+
WhiteSpaceChar                   [\x20\x09\x0d\x0a]

/* 8 bit Char http://www.w3.org/TR/REC-xml/#NT-Char */
PFChar                           [\x09\x0a\x0d\x20-\xff]

/* ElementContentChar */
QuotAttrContentChar              [\x09\x0a\x0d\x20-\x21\x23-\x25\x27-\x3b\x3d\x3e\x3f-\x7a\x7c\x7e-\xff]
AposAttrContentChar              [\x09\x0a\x0d\x20-\x25\x28-\x3b\x3d\x3e\x3f-\x7a\x7c\x7e-\xff]
ElementContentChar               [\x09\x0a\x0d\x20-\x25\x27-\x3b\x3d\x3e\x3f-\x7a\x7c\x7e-\xff]

/* XQuery WD [132] */
IntegerLiteral                   {Digits}
/* XQuery WD [133] */
DecimalLiteral                   ("."{Digits})|({Digits}"."[0-9]*)
/* XQuery WD [134] */
DoubleLiteral                    (("."{Digits})|({Digits}("."[0-9]*)?))[eE][-+]?{Digits}
/* XQuery WD [135] */
StringLiteral                    ("\""({PredefinedEntityRef}|{CharRef}|"\"\""|[^"&])*"\"")|("'"({PredefinedEntityRef}|{CharRef}|"''"|[^'&])*"'")
/* XQuery WD [136] http://www.w3.org/TR/REC-xml/#NT-PITarget
   !W3C: This pattern also matches "xml".
   FIXME: Catch [xX][mM][lL] in parser rule. */
PITarget                         ({Letter}|"_"|":"){NameChar}*
NameChar                         {Letter}|{Digit}|"."|"-"|"_"|{Extender}

/* XQuery WD [140] */
PredefinedEntityRef              "&"("lt"|"gt"|"amp"|"quot"|"apos")";"
/* XQuery WD [141] or http://www.w3.org/TR/REC-xml/#NT-CharRef */
CharRef                          ("&#"[0-9]+";")|("&#x"[0-9a-fA-F]+";")

ExprComment                      "(:"[^:]([^(:]|"("[^:]|":"[^)])*":)"

/* whitespace/comments in compound keywords */
_                                ({S}|{ExprComment})+

%%

<INITIAL,DEFAULT>{

"xquery"{_}"version"               { yield (xquery_version); }

"module"{_}"namespace"             { gotoState (NAMESPACEDECL);
                                     yield (module_namespace); }

";"                                { yield (semicolon); }

"declare"{_}"namespace"            { gotoState (NAMESPACEDECL);
                                     yield (declare_namespace); }

"declare"{_}"xmlspace"{_}"preserve" { yield (declare_xmlspace_preserve); }
"declare"{_}"xmlspace"{_}"strip"   { yield (declare_xmlspace_strip); }

"declare"{_}"default"{_}"element"  { gotoState (NAMESPACEKEYWORD);
                                     yield (declare_default_element); }
"declare"{_}"default"{_}"function" { gotoState (NAMESPACEKEYWORD);
                                     yield (declare_default_function); }
"declare"{_}"ordering"{_}"ordered" { yield (declare_ordering_ordered); }
"declare"{_}"ordering"{_}"unordered" { yield (declare_ordering_unordered); }
"declare"{_}"default"{_}"order"    { yield (declare_default_order); }
"declare"{_}"inherit-namespaces"{_}"yes" { yield (declare_inherit_namespaces_yes); }
"declare"{_}"inherit-namespaces"{_}"no" { yield (declare_inherit_namespaces_no); }
"empty"{_}"greatest"               { yield (empty_greatest); }
"empty"{_}"least"                  { yield (empty_least); }
"declare"{_}"default"{_}"collation" { yield (declare_default_collation); }
"declare"{_}"base-uri"             { yield (declare_base_uri); }
"import"{_}"schema"                { gotoState (NAMESPACEKEYWORD);
                                     yield (import_schema); }
"import"{_}"module"                { gotoState (NAMESPACEKEYWORD);
                                     yield (import_module); }
"at"{_}{StringLiteral}             { pflval.str = extract_string (yytext);
                                     yield (at_StringLiteral);
                                   }
"declare"{_}"variable"{_}"$"       { gotoState (VARNAME);
                                     yield (declare_variable_dollar); }
"declare"{_}"construction"{_}"preserve" { yield (declare_construction_preserve); }
"declare"{_}"construction"{_}"strip" { yield (declare_construction_strip); }
"declare function"                 { yield (declare_function); }
"$"                                { gotoState (VARNAME); yield (dollar); }
")"                                { gotoState (OPERATOR); yield (rparen); }
")"{_}?"as"{_}                     { /* trailing whitspace in pattern prevents
                                        illegal parsing of `...) ascending' in
                                        order by clauses here. (bug #1359816) */                                     gotoState (ITEMTYPE); yield (rparen_as); }
"{"                                { yield (lbrace); }
"}"                                { popState (); yield (rbrace); }
","                                { yield (comma); }


"for"{_}"$"                        { gotoState (VARNAME); yield (for_dollar); }
"let"{_}"$"                        { gotoState (VARNAME); yield (let_dollar); }
"some"{_}"$"                       { gotoState (VARNAME); yield (some_dollar); }
"every"{_}"$"                      { gotoState (VARNAME); yield (every_dollar);}
"typeswitch"{_}?"("/[^:]           { yield (typeswitch_lparen); }
"if"{_}?"("/[^:]                   { yield (if_lparen); }
"+"                                { yield (plus); /* unary plus */ }
"-"                                { yield (minus); /* unary minus */ }
"validate"{_}?"{"                  { pushState (OPERATOR);
                                     yield (validate_lbrace); }
"validate"{_}"lax"{_}?"{"          { pushState (OPERATOR);
                                     yield (validate_lax_lbrace); }
"validate"{_}"strict"{_}?"{"       { pushState (OPERATOR);
                                     yield (validate_strict_lbrace); }
"/"                                { yield (slash); }
"//"                               { yield (slash_slash); }
"child::"                          { yield (child_colon_colon); }
"descendant::"                     { yield (descendant_colon_colon); }
"attribute::"                      { yield (attribute_colon_colon); }
"self::"                           { yield (self_colon_colon); }
"descendant-or-self::"             { yield (descendant_or_self_colon_colon); }
"following-sibling::"              { yield (following_sibling_colon_colon); }
"following::"                      { yield (following_colon_colon); }
"@"                                { yield (atsign); }
"parent::"                         { yield (parent_colon_colon); }
"ancestor::"                       { yield (ancestor_colon_colon); }
"preceding-sibling::"              { yield (preceding_sibling_colon_colon); }
"preceding::"                      { yield (preceding_colon_colon); }
"ancestor-or-self::"               { yield (ancestor_or_self_colon_colon); }
"select-narrow::"                  { yield (select_narrow_colon_colon); }
"select-wide::"                    { yield (select_wide_colon_colon); }
"reject-narrow::"                  { yield (reject_narrow_colon_colon); }
"reject-wide::"                    { yield (reject_wide_colon_colon); }
".."                               { gotoState (OPERATOR); yield (dot_dot); }
"."                                { gotoState (OPERATOR); yield (dot); }
"*"                                { gotoState (OPERATOR); yield (star); }
{NCName}":*"                       { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OPERATOR);
                                     yield (NCName_Colon_Star); }
"*:"{NCName}                       { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OPERATOR); 
                                     yield (Star_Colon_NCName); }
"document-node"{_}?"("/[^:]        { pushState (OCCURRENCEINDICATOR);
                                     yield (document_node_lparen); }
"text"{_}?"("/[^:]                 { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (text_lparen); }
"comment"{_}?"("/[^:]              { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (comment_lparen); }
"processing-instruction"{_}?"("/[^:] { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTESTFORPI);
                                     yield (pi_lparen); }
"attribute"{_}?"("/[^:]            { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (attribute_lparen); }
"schema-attribute"{_}?"("/[^:]     { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (schema_attribute_lparen); }
"element"{_}?"("/[^:]              { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (element_lparen); }
"schema-element"{_}?"("/[^:]       { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (schema_element_lparen); }
"node"{_}?"("{_}?")"               { gotoState (OCCURRENCEINDICATOR);
                                     yield (node_lrparens); }
"empty"{_}?"("{_}?")"              { gotoState (OPERATOR);
                                     yield (empty_lrparens); }
"item"{_}?"("{_}?")"               { gotoState (OCCURRENCEINDICATOR);
                                     yield (item_lrparens); }
"("                                { yield (lparen); }
"ordered"{_}?"{"                   { pushState (OPERATOR);
                                     yield (ordered_lbrace); }
"unordered"{_}?"{"                 { pushState (OPERATOR);
                                     yield (unordered_lbrace); }
"<"                                { pushState (OPERATOR);
                                     gotoState (START_TAG); yield (less_than); }
"document"{_}?"{"                  { pushState (OPERATOR);
                                     yield (document_lbrace); }
"element"{_}?"{"                   { pushState (OPERATOR); pushState (DEFAULT);
                                     yield (element_lbrace); }
"attribute"{_}?"{"                 { pushState (OPERATOR); pushState (DEFAULT);
                                     yield (attribute_lbrace); }
"text"{_}?"{"                      { pushState (OPERATOR);
                                     yield (text_lbrace); }
"comment"{_}?"{"                   { pushState (OPERATOR);
                                     yield (comment_lbrace); }
"processing-instruction"{_}?"{"    { pushState (OPERATOR);
                                     yield (pi_lbrace); }
"element"{_}{QName}{_}?"{"         { pflval.qname = extract_qname (yytext, 1);
                                     pushState (OPERATOR);
                                     yield (Element_QName_LBrace);
                                   }
"attribute"{_}{QName}{_}?"{"       { pflval.qname = extract_qname (yytext, 1);
                                     pushState (OPERATOR);
                                     yield (Attribute_QName_LBrace);
                                   }
"processing-instruction"{_}{NCName}{_}?"{" {
                                     pflval.str = extract_ncname (yytext, 1);
                                     pushState (OPERATOR);
                                     yield (PI_NCName_LBrace);
                                   }

"<!--"                             { pushState (OPERATOR);
                                     gotoState (XML_COMMENT);
                                     yield (xml_comment_start); }
"<?"                               { pushState (OPERATOR);
                                     gotoState (PROCESSING_INSTRUCTION);
                                     yield (lt_question_mark); }
"<![CDATA["                        { pushState (OPERATOR);
                                     gotoState (CDATA_SECTION);
                                     yield (cdata_start); }

{QName}{_}?"("/[^:]                { pflval.qname = extract_qname (yytext, 0);
                                     yield (QName_LParen);
                                   }
{IntegerLiteral}                   { pflval.num = atoll (yytext);
                                     gotoState (OPERATOR);
                                     yield (IntegerLiteral);
                                   }
{DecimalLiteral}                   { pflval.dec = atof (yytext);
                                     gotoState (OPERATOR);
                                     yield (DecimalLiteral);
                                   }
{DoubleLiteral}                    { pflval.dbl = atof (yytext);
                                     gotoState (OPERATOR);
                                     yield (DoubleLiteral);
                                   }
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OPERATOR);
                                     yield (QName);
                                   }
{StringLiteral}                    { pflval.str = extract_string (yytext);
                                     gotoState (OPERATOR);
                                     yield (StringLiteral);
                                   }
{_}                                ;
.                                  { yield (invalid_character); }
}

<OPERATOR>{
"encoding"{_}{StringLiteral}       { pflval.str = extract_string (yytext);
                                     gotoState (DEFAULT);
                                     yield (encoding_StringLiteral);
                                   }

")"{_}?"as"{_}                     { /* trailing whitspace in pattern prevents
                                        illegal parsing of `...) ascending' in
                                        order by clauses here. (bug #1359816) */
                                     gotoState (ITEMTYPE); yield (rparen_as); }
"as"                               { gotoState (ITEMTYPE); yield (as); }
":="                               { gotoState (DEFAULT); yield (colon_equals);}
"external"                         { gotoState (DEFAULT); yield (external_); }

"at"{_}"$"                         { gotoState (VARNAME); yield (at_dollar); }
"for"{_}"$"                        { gotoState (VARNAME); yield (for_dollar); }
"let"{_}"$"                        { gotoState (VARNAME); yield (let_dollar); }
"in"                               { gotoState (DEFAULT); yield (in_); }

"{"                                { pushState (OPERATOR);
                                     gotoState (DEFAULT);
                                     yield (lbrace); }
"}"                                { popState (); yield (rbrace); }
";"                                { gotoState (DEFAULT); yield (semicolon); }
","                                { gotoState (DEFAULT); yield (comma); }
"return"                           { gotoState (DEFAULT); yield (return_); }
"where"                            { gotoState (DEFAULT); yield (where); }
"order"{_}"by"                     { gotoState (DEFAULT); yield (order_by); }
"stable"{_}"order"{_}"by"          { gotoState (DEFAULT);
                                     yield (stable_order_by); }
"ascending"                        { yield (ascending); }
"descending"                       { yield (descending); }
"collation"                        { yield (collation); }
"empty"{_}"greatest"               { yield (empty_greatest); }
"empty"{_}"least"                  { yield (empty_least); }
{StringLiteral}                    { pflval.str = extract_string (yytext);
                                     yield (StringLiteral);
                                   }
"satisfies"                        { gotoState (DEFAULT); yield (satisfies); }
")"                                { yield (rparen); }
"case"                             { gotoState (ITEMTYPE); yield (case_); }
"default"                          { yield (default_); }
"$"                                { gotoState (VARNAME); yield (dollar); }
"then"                             { gotoState (DEFAULT); yield (then); }
"else"                             { gotoState (DEFAULT); yield (else_); }
"or"                               { gotoState (DEFAULT); yield (or); }
"and"                              { gotoState (DEFAULT); yield (and); }
"to"                               { gotoState (DEFAULT); yield (to_); }
"union"                            { gotoState (DEFAULT); yield (union_); }
"|"                                { gotoState (DEFAULT); yield (pipe_); }
"intersect"                        { gotoState (DEFAULT); yield (intersect); }
"except"                           { gotoState (DEFAULT); yield (except); }
"+"                                { gotoState (DEFAULT); yield (plus); }
"-"                                { gotoState (DEFAULT); yield (minus); }
"*"                                { gotoState (DEFAULT); yield (star); }
"div"                              { gotoState (DEFAULT); yield (div_); }
"idiv"                             { gotoState (DEFAULT); yield (idiv); }
"mod"                              { gotoState (DEFAULT); yield (mod); }
"instance"{_}"of"                  { gotoState (ITEMTYPE); yield (instance_of);}
"treat"{_}"as"                     { gotoState (ITEMTYPE); yield (treat_as); }
"castable"{_}"as"                  { gotoState (SINGLETYPE);
                                     yield (castable_as); }
"cast"{_}"as"                      { gotoState (SINGLETYPE); yield (cast_as); }
"="                                { gotoState (DEFAULT); yield (equals); }
"!="                               { gotoState (DEFAULT); yield (excl_equals); }
"<"                                { gotoState (DEFAULT); yield (less_than); }
"<="                               { gotoState (DEFAULT);
                                     yield (less_than_equal); }
">"                                { gotoState (DEFAULT); yield (greater_than);}
">="                               { gotoState (DEFAULT);
                                     yield (greater_than_equal); }
"eq"                               { gotoState (DEFAULT); yield (eq); }
"ne"                               { gotoState (DEFAULT); yield (ne); }
"lt"                               { gotoState (DEFAULT); yield (lt); }
"le"                               { gotoState (DEFAULT); yield (le); }
"gt"                               { gotoState (DEFAULT); yield (gt); }
"ge"                               { gotoState (DEFAULT); yield (ge); }
"is"                               { gotoState (DEFAULT); yield (is); }
"<<"                               { gotoState (DEFAULT); yield (lt_lt); }
">>"                               { gotoState (DEFAULT); yield (gt_gt); }
"/"                                { gotoState (DEFAULT); yield (slash); }
"//"                               { gotoState (DEFAULT); yield (slash_slash); }
"["                                { gotoState (DEFAULT); yield (lbracket); }
"]"                                { yield (rbracket); }
"?"                                { yield (question_mark); }
{_}                                ;
.                                  { yield (invalid_character); }
}

<NAMESPACEDECL>{
{StringLiteral}                    { pflval.str = extract_string (yytext);
                                     gotoState (DEFAULT);
                                     yield (StringLiteral);
                                   }
{NCName}                           { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (NCName);
                                   }
"="                                { yield (equals); }
{_}                                ;
.                                  { yield (invalid_character); }
}

<NAMESPACEKEYWORD>{
"namespace"                        { gotoState (NAMESPACEDECL);
                                     yield (namespace); }
"default"{_}"element"              { yield (default_element); }
{StringLiteral}                    { pflval.str = extract_string (yytext);
                                     gotoState (DEFAULT);
                                     yield (StringLiteral);
                                   }
{_}                                ;
.                                  { yield (invalid_character); }
}

<VARNAME>{
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OPERATOR);
                                     yield (QName);
                                   }
.                                  { yield (invalid_character); }
}

<SINGLETYPE>{
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OPERATOR);
                                     yield (QName);
                                   }
{_}                                ;
.                                  { yield (invalid_character); }
}

<ITEMTYPE>{
"$"                                { gotoState (VARNAME); yield (dollar); }
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OCCURRENCEINDICATOR);
                                     yield (QName);
                                   }
"document-node"{_}?"("/[^:]        { pushState (OCCURRENCEINDICATOR);
                                     yield (document_node_lparen); }
"text"{_}?"("/[^:]                 { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (text_lparen); }
"comment"{_}?"("/[^:]              { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (comment_lparen); }
"processing-instruction"{_}?"("/[^:] { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTESTFORPI);
                                     yield (pi_lparen); }
"attribute"{_}?"("/[^:]            { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (attribute_lparen); }
"schema-attribute"{_}?"("/[^:]     { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (schema_attribute_lparen); }
"element"{_}?"("/[^:]              { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (element_lparen); }
"schema-element"{_}?"("/[^:]       { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (schema_element_lparen); }
"node"{_}?"("{_}?")"               { gotoState (OCCURRENCEINDICATOR);
                                     yield (node_lrparens); }
"empty"{_}?"("{_}?")"              { gotoState (OPERATOR);
                                     yield (empty_lrparens); }
"item"{_}?"("{_}?")"               { gotoState (OCCURRENCEINDICATOR);
                                     yield (item_lrparens); }
{_}                                ;
.                                  { yield (invalid_character); }
}

<OCCURRENCEINDICATOR>{
"*"                                { gotoState (OPERATOR);
                                     yield (star); }
"?"                                { gotoState (OPERATOR);
                                     yield (question_mark); }
"+"                                { gotoState (OPERATOR);
                                     yield (plus); }
{_}                                ;
.                                  { unput (yytext[yyleng-1]);
                                     gotoState (OPERATOR); }
}

<KINDTEST>{
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     yield (QName);
                                   }
","                                { yield (comma); }
"?"                                { yield (question_mark); }
")"                                { popState (); yield (rparen); }
"*"                                { yield (star); }
{_}                                ;
.                                  { yield (invalid_character); }
}

<KINDTESTFORPI>{
{NCName}                           { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (NCName);
                                   }
{StringLiteral}                    { pflval.str = extract_string (yytext);
                                     yield (StringLiteral);
                                   }
")"                                { popState (); yield (rparen); }
{_}                                ;
.                                  { yield (invalid_character); }
}

<START_TAG>{
{QName}                            { pflval.qname 
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     yield (QName);
                                   }
{S}                                { yield (S); }
"="                                { yield (equals); }
"\""                               { gotoState (QUOT_ATTRIBUTE_CONTENT);
                                     yield (quot_); }
"'"                                { gotoState (APOS_ATTRIBUTE_CONTENT);
                                     yield (apos); }
"/>"                               { popState (); yield (slash_gt); }
">"                                { gotoState (ELEMENT_CONTENT);
                                     yield (greater_than); }
.                                  { yield (invalid_character); }
}

<QUOT_ATTRIBUTE_CONTENT>{
"\""                               { gotoState (START_TAG); yield (quot_); }
"\"\""                             { yield (escape_quot); }
"{{"                               { yield (lbrace_lbrace); }
"}}"                               { yield (rbrace_rbrace); }
"{"                                { pushState (QUOT_ATTRIBUTE_CONTENT);
                                     gotoState (DEFAULT);
                                     yield (lbrace); }
{QuotAttrContentChar}              { pflval.chr = *yytext;
                                     yield (AttrContentChar);
                                   }
{PredefinedEntityRef}              { pflval.chr = entity_ref (yytext, NULL);
                                     yield (PredefinedEntityRef);
                                   }
{CharRef}                          { pflval.chr = entity_ref (yytext, NULL);
                                     yield (CharRef);
                                   }
.                                  { yield (invalid_character); }
}

<APOS_ATTRIBUTE_CONTENT>{
"'"                                { gotoState (START_TAG);
                                     yield (apos); }
"\"\""                             { yield (escape_apos); }
"{{"                               { yield (lbrace_lbrace); }
"}}"                               { yield (rbrace_rbrace); }
"{"                                { pushState (APOS_ATTRIBUTE_CONTENT);
                                     gotoState (DEFAULT);
                                     yield (lbrace); }
{AposAttrContentChar}              { pflval.chr = *yytext;
                                     yield (AttrContentChar);
                                   }
{PredefinedEntityRef}              { pflval.chr = entity_ref (yytext, NULL);
                                     yield (PredefinedEntityRef);
                                   }
{CharRef}                          { pflval.chr = entity_ref (yytext, NULL);
                                     yield (CharRef);
                                   }
.                                  { yield (invalid_character); }
}

<ELEMENT_CONTENT>{
"</"                               { gotoState (END_TAG); yield (lt_slash); }
"{{"                               { yield (lbrace_lbrace); }
"}}"                               { yield (rbrace_rbrace); }
"{"                                { pushState (ELEMENT_CONTENT);
                                     gotoState (DEFAULT);
                                     yield (lbrace); }
"<!--"                             { pushState (ELEMENT_CONTENT);
                                     gotoState (XML_COMMENT);
                                     yield (xml_comment_start); }
"<?"                               { pushState (ELEMENT_CONTENT);
                                     gotoState (PROCESSING_INSTRUCTION);
                                     yield (lt_question_mark); }
"<![CDATA["                        { pushState (ELEMENT_CONTENT);
                                     gotoState (CDATA_SECTION);
                                     yield (cdata_start); }
"<"                                { pushState (ELEMENT_CONTENT);
                                     gotoState (START_TAG); yield (less_than); }
{PredefinedEntityRef}              { pflval.chr = entity_ref (yytext, NULL);
                                     yield (PredefinedEntityRef);
                                   }
{CharRef}                          { pflval.chr = entity_ref (yytext, NULL);
                                     yield (CharRef);
                                   }
{ElementContentChar}               { pflval.chr = *yytext;
                                     yield (ElementContentChar);
                                   }
.                                  { yield (invalid_character); }
}

<XML_COMMENT>{
"-->"                              { popState (); yield (xml_comment_end); }
{PFChar}                           { pflval.chr = *yytext;
                                     yield (PFChar);
                                   }
.                                  { yield (invalid_character); }
}

<PROCESSING_INSTRUCTION>{
{S}                                { gotoState (PROCESSING_INSTRUCTION_CONTENT);
                                     yield (S); }
"?>"                               { popState (); yield (question_mark_gt); }
{PITarget}                         { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (PITarget);
                                   }
.                                  { yield (invalid_character); }
}

<PROCESSING_INSTRUCTION_CONTENT>{
"?>"                               { popState (); yield (question_mark_gt); }
{PFChar}                           { pflval.chr = *yytext;
                                     yield (PFChar);
                                   }
.                                  { yield (invalid_character); }
}

<CDATA_SECTION>{
"]]>"                              { popState (); yield (cdata_end); }
{PFChar}                           { pflval.chr = *yytext;
                                     yield (PFChar);
                                   }
.                                  { yield (invalid_character); }
}

<END_TAG>{
">"                                { popState (); yield (greater_than); }
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     yield (QName);
                                   }
{S}                                { yield (S); }
.                                  { yield (invalid_character); }
}

%%

static char
entity_ref (const char *s, char **endptr)
{
    assert (*s == '&');

    if (s[1] == '#') {
        /* this is a character reference (`&#') */
        if (s[2] == 'x') {
            return (char) strtol (s + 3, endptr, 16);
        } else {
            return (char) strtol (s + 2, endptr, 10);
        }
    }
    else {
        /* this is a predefined entity */
        if (!strncmp (s + 1, "lt", 2)) {
            if (endptr)
                *endptr = (char *) s + 3;
            return '<';
        }
        else if (!strncmp (s + 1, "gt", 2)) {
            if (endptr)
                *endptr = (char *) s + 3;
            return '>';
        }
        else if (!strncmp (s + 1, "amp", 3)) {
            if (endptr)
                *endptr = (char *) s + 4;
            return '&';
        }
        else if (!strncmp (s + 1, "quot", 4)) {
            if (endptr)
                *endptr = (char *) s + 5;
            return '"';
        }
        else if (!strncmp (s + 1, "apos", 4)) {
            if (endptr)
                *endptr = (char *) s + 5;
            return '\'';
        }
    }

    PFoops (OOPS_PARSE, "error in entity reference: `%s'", s);
    assert(0); /* we should never come here! */
    return '\0'; /* pacify picky compilers */
}

/**
 * Extract a string literal from a pattern match.
 * The string may occur anywhere in the match. This function
 * also respects XQuery comments.
 */
static char *
extract_string (const char *s)
{
    unsigned int  start = 0;
    unsigned int  end = 0;
    PFarray_t    *ret = PFarray (sizeof (char));
    char         *endptr;
    unsigned int  i = 0;
    unsigned short ws_level = 0;

    /* find first quote character, skipping comments */
    while (true) {

        assert (s[start]);

        if (s[start] == '(' && s[start+1] && s[start+1] == ':') {
            ws_level = 1;
            start += 2;
            while (ws_level > 0) {
                assert (s[start]);
                if (s[start] == '(' && s[start+1] && s[start+1] == ':') {
                    start += 2;
                    ws_level++;
                }
                else if (s[start] == ':' && s[start+1] && s[start+1]==')') {
                    start += 2;
                    ws_level--;
                }
                else
                    start++;
            }
        }
        else if (s[start] != '"' && s[start] != '\'')
            start++;
        else
            break;
    }

    if (!s[start])
        return NULL;

    end = start;

    while (s[++end]) {
        /* test for quotes/apostrophs */
        if (s[end] == s[start]) {
            if (s[end + 1] && s[end] == s[end+1])
                *((char *) PFarray_at (ret, i++)) = s[end++];
            else
                return PFstrndup (ret->base, i);
        }
        /* test for entities */
        else if (s[end] == '&') {
            *((char *) PFarray_at (ret, i++))
                = entity_ref (s + end, &endptr);
            end = endptr - s;
        }
        /* everything else */
        else {
            *((char *) PFarray_at (ret, i++)) = s[end];
        }
    }
    PFoops (OOPS_PARSE, "Error in StringLiteral");
    assert(0); /* we should never come here! */
    return NULL; /* pacify picky compilers */
}

/**
 * Test if a character is an NC name char as defined by the W3C.
 * (See also the lex pattern `NCNameChar' above.)
 */
static bool
isncnamechar (const char c)
{
    return ((c >= '\x41' && c <= '\x5a')
            || (c >= '\x61' && c <= '\x7a')
            || (c >= '\xc0' && c <= '\xd6')
            || (c >= '\xd8' && c <= '\xf6')
            || (c >= '\xf8' && c <= '\xff')
            || (c >= '\x30' && c <= '\x39')
            || c == '.' || c == '-' || c == '_' || c == '\xb7');
}

/**
 * Extract word number @a index from a pattern match and return
 * it as a QName.
 */
static PFqname_t
extract_qname (const char *s, unsigned int index)
{
    unsigned int start = 0;
    unsigned int end;
    unsigned int word = 0;
    unsigned short ws_level = 0;

    while (word < index) {

        assert (s[start]);

        /* skip a word */
        while (s[start] && isncnamechar (s[start]))
            start++;

        /* skip whitespace (including comments) */
        while (true) {

            assert (s[start]);

            if (isspace (s[start]))
                start++;
            else if (s[start] == '(' && s[start+1] && s[start+1] == ':') {
                ws_level = 1;
                start += 2;
                while (ws_level > 0) {
                    assert (s[start]);
                    if (s[start] == '(' && s[start+1] && s[start+1] == ':') {
                        start += 2;
                        ws_level++;
                    }
                    else if (s[start] == ':' && s[start+1] && s[start+1]==')') {
                        start += 2;
                        ws_level--;
                    }
                    else
                        start++;
                }
            }
            else
                break;
        }

        word++;
    }

    for (end = start; s[end] && !isspace (s[end])
                             && s[end] != '{'
                             && s[end] != '('; end++)
        /* do nothing */;

    return PFstr_qname (PFstrndup (s + start, end - start));
}

/**
 * Extract word number @a index from a pattern match and return
 * it as a string.
 */
static char *
extract_ncname (const char *s, unsigned int index)
{
    unsigned int start = 0;
    unsigned int end;
    unsigned int word = 0;
    unsigned short ws_level = 0;

    while (word < index) {

        assert (s[start]);

        /* skip a word */
        while (s[start] && isncnamechar (s[start]))
            start++;

        /* skip whitespace (including comments) */
        while (true) {

            assert (s[start]);

            if (isspace (s[start]))
                start++;
            else if (s[start] == '(' && s[start+1] && s[start+1] == ':') {
                ws_level = 1;
                start += 2;
                while (ws_level > 0) {
                    assert (s[start]);
                    if (s[start] == '(' && s[start+1] && s[start+1] == ':') {
                        start += 2;
                        ws_level++;
                    }
                    else if (s[start] == ':' && s[start+1] && s[start+1]==')') {
                        start += 2;
                        ws_level--;
                    }
                    else
                        start++;
                }
            }
            else
                break;
        }

        word++;
    }

    for (end = start; s[end] && !isspace (s[end])
                             && s[end] != '{'
                             && s[end] != '('; end++)
        /* do nothing */;

    return PFstrndup (s + start, end - start);
}

/* vim:set shiftwidth=4 expandtab: */


/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/* Hey Emacs, this is -*-C-*- */

/**
 * @file
 *
 * XQuery (W3C WD November 15, 2002) terminals and lexer state machine.
 * Feed this into `flex'.
 *
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

%{

/**
 * This is a implementation of the W3C document ``XQuery 1.0:
 * An XML Query Language'', W3C Working Draft November 15, 2002.
 *  
 * Places where we deviate from the W3C Working Draft are marked
 * by `!W3C'.
 *
 * !W3C: The Pathfinder lexer state machine collapses the XQuery lexer
 * states DEFAULT, OPERATOR, and QNAME states into a single DEFAULT
 * state. (As it currently stands, the W3C spec is simply too broken
 * to make sense of these states.)
 *
 * We implement states NAMESPACEDECL, NAMESPACEKEYWORD, 
 * XMLSPACE_DECL, ITEMTYPE, VARNAME, START_TAG, ELEMENT_CONTENT, END_TAG,
 * XML_COMMENT, PROCESSING_INSTRUCTION,
 * PROCESSING_INSTRUCTION_CONTENT, CDATA_SECTION,
 * QUOT_ATTRIBUTE_CONTENT, APOS_ATTRIBUTE_CONTENT.
 */

#include "pathfinder.h"

/* atoi, atof, strtoul */
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* import XQuery token and abstract syntax definitions */
#include "abssyn.h"
/* PFarray_t */
#include "array.h"
/* PFstrdup */
#include "mem.h"

/* override YYLTYPE definition in "XQuery.tab.h" */
#define YYLTYPE PFloc_t

/* #include "XQuery.tab.h" */
#include "parser.tab.h"

/* bison 1.33 does not include this line in XQuery.tab.h. So we do it here. */
extern YYLTYPE pflloc;

/** avoid `defined but not used' warnings */
#define YY_NO_UNPUT

/** increment row and column numbers (required in YY_USER_ACTION) */
static int i;

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

/* prototypes */
static char predefined_entity (char *ent);
static char char_ref (char *ref);
static char *expand_esc_quots (char *quot);

/**
 * define DEBUG_SCANNER to activate debugging output in parser
 * (if set, every token received is printed to stderr)
 */
#ifdef DEBUG_SCANNER
#define yield(t) PFlog ("token: " #t); return (t)
#else
#define yield(t) return (t)
#endif


/** 
 * lexical state stack (see W3C XQuery, A.1.1 and A.1.2) 
 */

/** the lexical stack itself (grows dynamically) */
static PFarray_t *lexical_stack;

/** initialize the lexical state stack when scanner starts up */
#define YY_USER_INIT (lexical_stack = PFarray (sizeof (int)))
		     
/** current stack pointer for #lexical_stack */
static unsigned lexical_sp = 0;

static void
popState ()
{
    assert (lexical_sp > 0);
    BEGIN (*((int *) PFarray_at (lexical_stack, --lexical_sp)));

#ifdef DEBUG_LEXSTATE
    PFlog ("popState (), now in state %d", 
            *((int *) PFarray_at (lexical_stack, lexical_sp)));
#endif
}

static void
pushState (int s)
{
    *((int *) PFarray_at (lexical_stack, lexical_sp++)) = s;

#ifdef DEBUG_LEXSTATE
    PFlog ("pushState (%d)", s);
#endif
}

static void
gotoState (int s)
{
    BEGIN (s);

#ifdef DEBUG_LEXSTATE
    PFlog ("gotoState (%d)", s);
#endif
}


/** 
 * NB: as of W3C XQuery WD November 15, 2002 the paren stack seems to
 * be  u n u s e d.  (This mess is waiting to be abandoned, I hope.  We
 * keep it here in case it raises its ugly head again.  To be deleted.)
 *
 * paren stack (see W3C XQuery, A.1.2)
 * (we use the paren stack to ``remember'' if the lexer expects to see
 *  an (optional) SequenceType after a `,' token, e.g.
 *
 *               define foo ($x, node $y)
 *   vs.
 *               foo ($x, $y)
 * )
 *
 */

#ifdef USE_PAREN_STATE_STACK

/** 
 * maximum depth of paren stack
 * (this effectively limits the maximum depth of nested parenthesized
 *  expressions, including function calls, etc.)
 */
#define PAREN_STACK_DEPTH 256

/** the paren stack itself */
static int paren_stack[PAREN_STACK_DEPTH];

/** current stack pointer for #paren_stack */
static int paren_sp = 0;

static void
popParenState (int s)
{
    assert (paren_sp > 0);
    /* yes, we indeed ignore the state being popped */
    paren_sp--;

    gotoState (s);

#ifdef DEBUG_LEXSTATE
    PFlog ("popParenState (%d)", s);
#endif
}

static void 
pushParenState (int s)
{
    assert (paren_sp < PAREN_STACK_DEPTH + 1);
    paren_stack[paren_sp++] = s;

    gotoState (s);

#ifdef DEBUG_LEXSTATE
    PFlog ("pushParenState (%d)", s);
#endif
}

static void
resetParenStateOrSwitch (int s)
{
    if (paren_sp)
        gotoState (paren_stack[paren_sp - 1]);
    else
        gotoState (s);

#ifdef DEBUG_LEXSTATE
    PFlog ("resetParenStateOrSwitch (%d), now in state %d",
            s,
            paren_sp ? paren_stack[paren_sp - 1] : s);
#endif
}
#endif

%}

/* no wrapping after EOF; hence, no libfl (-lfl) needed */
%option noyywrap

/* provide token line number to the parser */
%option yylineno

/**
 * XQuery 1.0 lexical states (see W3C XQuery, A.1.2)
 *
 * We maintain the lexer state using bison's internal state stack.
 * The additional paren stack is implemented separately (but not used
 * anymore as of the W3C XQuery November 15, 2002 draft).
 */

%x DEFAULT
%x NAMESPACEDECL
%x NAMESPACEKEYWORD
%x XMLSPACE_DECL
%x ITEMTYPE
%x VARNAME
%x START_TAG
%x ELEMENT_CONTENT
%x END_TAG
%x XML_COMMENT
%x PROCESSING_INSTRUCTION
%x PROCESSING_INSTRUCTION_CONTENT
%x CDATA_SECTION
%x QUOT_ATTRIBUTE_CONTENT
%x APOS_ATTRIBUTE_CONTENT

/**
 * XQuery 1.0 syntactic constructs (see W3C XQuery, A.1.1)
 *
 * @bug !W3C 
 * need to define `CombiningChar' to inlcude umlauts, diacritics, ... 
 * and `Extender' to include dots, special colons, etc. as valid `Nmchar's
 *
 * notes on W3C XQuery, A.1.1:
 * - `BaseChar' apparently unused
 * - `Ideographic' apparently unused
 * - `NCNameChar' apparently unused
 */

/* character classes */
Letter                            [a-zA-Z]                            
Digit                             [0-9]                               

/* identifiers */
NCName                            {Nmstart}{Nmchar}*
QName                             ({Prefix}":")?{LocalPart}
Prefix                            {NCName}
LocalPart                         {NCName}

/* string literals and numbers */
IntegerLiteral                    {Digits}
DecimalLiteral                    ("."{Digits})|({Digits}"."[0-9]*)
DoubleLiteral                     (("."{Digits})|({Digits}("."{Digit}*)?))("e"|"E")("+"|"-")?{Digits}
StringLiteral                     ("\""("\"\""|[^"])*"\"")|("'"("''"|[^'])*"'")

/* comments */
ExprComment                       "{--"([^-]|"-"[^-]|"--"+[^}])*"-""-"+"}"

/* defined tokens */
S                                 {WhiteSpaceChar}+
Nmstart                           {Letter}|"_"                        
Nmchar                            {Letter}|{Digit}|"."|"-"|"_"        
Digits                            [0-9]+ 
EscapeQuot                        "\"\""
URLLiteral                        ("\""("\"\""|[^"])*"\"")|("'"("''"|[^'])*"'")
PITarget                          {NCName}
VarName                           {QName}
NCNameForPrefix                   {Nmstart}{Nmchar}*
PredefinedEntityRef               "&"("lt"|"gt"|"amp"|"quot"|"apos")";"
HexDigits                         ([0-9]|[a-f]|[A-F])+                
CharRef                           "&#"({Digits}|("x"{HexDigits}))";"
EscapeApos                        "''"
Char                              "\n"|.
WhiteSpaceChar                    [ \t\n\r]

/* whitespace/comments in compound keywords */
_                                 ({S}|{ExprComment})+

%%


<INITIAL,DEFAULT>{  

"?"                               yield ('?'); 
"["                               yield ('['); 
"]"                               yield (']'); 
"+"                               yield ('+'); 
"-"                               yield ('-'); 
"("				  yield ('(');
")"                               yield (')');
";"                               yield (';'); 
"*"                               yield ('*');
","                               yield (',');
"$"                               { pushState (DEFAULT);
                                    gotoState (VARNAME);
                                    yield ('$');
                                  }

"{"                               { pushState (DEFAULT);
                                    yield ('{'); 
                                  }
"}"                               { popState (); 
                                    yield ('}');
                                  }


"attribute"{S}{QName}{_}*"{"      { /* return the attribute QName */
				    pflval.qname =
                                      PFstr_qname (strtok (strpbrk (
                                                            yytext, " \t\n\r"),
                                                     " \t\n\r{"));
   
                                    pushState (DEFAULT);
                                    yield (attribute_QName_lbrace);
                                  }
"element"{S}{QName}{_}*"{"        { /* return the element tag QName */
				    pflval.qname =
                                      PFstr_qname (strtok (strpbrk (
                                                            yytext, " \t\n\r"),
                                                     " \t\n\r{"));

                                    pushState (DEFAULT);
                                    yield (element_QName_lbrace);
                                  }
"element"{_}*"{"                  { pushState (DEFAULT);
                                    yield (element_lbrace);
                                  }
"document"{_}*"{"                 { pushState (DEFAULT);
                                    yield (document_lbrace);
                                  }
"attribute"{_}*"{"                { pushState (DEFAULT);
                                    yield (attribute_lbrace);
                                  }
"text"{_}*"{"                     { pushState (DEFAULT);
                                    yield (text_lbrace);
                                  }


{IntegerLiteral}                  { /* return the int value
                                     * represented by this integer literal 
				     */
                                    pflval.num = atoi (yytext);

                                    yield (IntegerLiteral);
                                  }
{DecimalLiteral}                  { /* return double value represented
                                     * by this decimal literal
                                     */
				    pflval.dec = atof (yytext);

                                    yield (DecimalLiteral);
                                  }
{DoubleLiteral}                   { /* return double value represented
                                     * by this double literal
                                     */
				    pflval.dec = atof (yytext);

                                    yield (DoubleLiteral);
                                  }
{StringLiteral}                   { /* return string between `"' or `''
                                     * represented by the string literal,
                                     * expand escaped quotation marks
                                     */
                                    pflval.str = expand_esc_quots (
                                                   strpbrk (yytext, "'\""));

                                    yield (StringLiteral);
				  }
"at"{S}{StringLiteral}            { /* return string between `"' or `''
                                     * represented by the string literal,
                                     * expand escaped quotation marks
                                     */
                                    pflval.str = expand_esc_quots (
                                                   strpbrk (yytext, "'\""));

                                    yield (at_StringLiteral);
                                  }



"default"{_}"collation"{_}"="     { gotoState (NAMESPACEDECL);
                                    yield (default_collation_equals);
                                  }
"declare"{_}"namespace"           { gotoState (NAMESPACEDECL);
                                    yield (declare_namespace);
                                  }

"default"{_}"element"             { gotoState (NAMESPACEKEYWORD);
                                    yield (default_element);
                                  }
"default"{_}"function"            { gotoState (NAMESPACEKEYWORD);
                                    yield (default_function);
                                  }
"import"{_}"schema"               { gotoState (NAMESPACEKEYWORD);
                                    yield (import_schema);
                                  }


"declare"{_}"xmlspace"            { gotoState (XMLSPACE_DECL);
                                    yield (declare_xmlspace);
                                  }
"define"{_}"function"             yield (define_function);

"<"                               { pushState (DEFAULT);
                                    gotoState (START_TAG);
                                    yield ('<');
                                  }
"<!--"                            { pushState (DEFAULT);
                                    gotoState (XML_COMMENT);
                                    yield (xml_comment_start);
                                  }
"<?"                              { pushState (DEFAULT);
                                    gotoState (PROCESSING_INSTRUCTION);
                                    yield (pi_start);
                                  }
"<![CDATA["                       { pushState (DEFAULT);
                                    gotoState (CDATA_SECTION);
                                    yield (cdata_start);
                                  }


"div"                             yield (div_);
"idiv"                            yield (idiv);
"mod"                             yield (mod);
"and"                             yield (and);
"or"                              yield (or);
"return"                          yield (return_);
"then"                            yield (then_);
"else"                            yield (else_);
"to"                              yield (to);
"union"                           yield (union_);
"intersect"                       yield (intersect);
"except"                          yield (except);
"="                               yield ('=');
"is"                              yield (is);
"!="                              yield (neq);
"isnot"                           yield (isnot);
"<="                              yield (lte);
">="                              yield (gte);
"<"{_}                            yield (lt_s);
">"                               yield ('>');
"|"                               yield ('|');
"<<"                              yield (lt_lt);
">>"                              yield (gt_gt);
"eq"                              yield (eq);
"ne"                              yield (ne);
"gt"                              yield (gt);
"ge"                              yield (ge);
"lt"                              yield (lt);
"le"                              yield (le);
"in"                              yield (in);
"at"                              yield (at);
"context"                         yield (context);
"where"                           yield (where);
"order"{_}"by"                    yield (order_by);
"satisfies"                       yield (satisfies);
":="                              yield (colon_equals);

"for"{_}"$"                       { pushState (DEFAULT);
                                    gotoState (VARNAME);
                                    yield (for_dollar);
                                  }
"let"{_}"$"                       { pushState (DEFAULT);
                                    gotoState (VARNAME);
                                    yield (let_dollar);
                                  }
"some"{_}"$"                      { pushState (DEFAULT);
                                    gotoState (VARNAME);
                                    yield (some_dollar);
                                  }
"every"{_}"$"                     { pushState (DEFAULT);
                                    gotoState (VARNAME);
                                    yield (every_dollar);
                                  }

"if"{_}*"("                       yield (if_lparen);
"typeswitch"{_}*"("               yield (typeswitch_lparen);
"validate"                        yield (validate);
"validate"{_}*"{"                 { pushState (DEFAULT);
                                    yield (validate_lbrace);
                                  }

"stable"{_}"order"{_}"by"         yield (stable_order_by);
"collation"                       yield (collation);
"ascending"                       yield (ascending);
"descending"                      yield (descending);
"empty"{_}"greatest"              yield (empty_greatest);
"empty"{_}"least"                 yield (empty_least);

"typeswitch"{_}*"("               yield (typeswitch_lparen);
"default"                         yield (default_);

"case"                            { gotoState (ITEMTYPE);
                                    yield (case_);
                                  }
"instance"{_}"of"                 { gotoState (ITEMTYPE);
                                    yield (instance_of);
                                  }
"castable"{_}"as"                 { gotoState (ITEMTYPE);
                                    yield (castable_as);
                                  }
"cast"{_}"as"                     { gotoState (ITEMTYPE);
                                    yield (cast_as);
                                  }
"treat"{_}"as"                    { gotoState (ITEMTYPE);
                                    yield (treat_as);
                                  }
")"{_}*"as"                       { gotoState (ITEMTYPE);
                                    yield (rparen_as);
                                  }
"as"                              { gotoState (ITEMTYPE);
                                    yield (as);
                                  }

"type"{S}{QName}                  { /* return the type QName */ 
				    pflval.qname =
                                      PFstr_qname (strtok (strpbrk (
                                                            yytext, " \t\n\r"),
                                                     " \t\n\r"));

                                    yield (type_QName); 
                                  }

"of"{_}"type"                     yield (of_type);

"."                               yield ('.');
".."                              yield (dot_dot);
"/"                               yield ('/');
"//"                              yield (slash_slash);
"child::"                         yield (child_colon_colon);
"descendant::"                    yield (descendant_colon_colon);
"parent::"                        yield (parent_colon_colon);
"attribute::"                     yield (attribute_colon_colon);
"self::"                          yield (self_colon_colon);
"descendant-or-self::"            yield (descendant_or_self_colon_colon);
"ancestor::"                      yield (ancestor_colon_colon);
"ancestor-or-self::"              yield (ancestor_or_self_colon_colon);
"preceding::"                     yield (preceding_colon_colon);
"following::"                     yield (following_colon_colon);
"following-sibling::"             yield (following_sibling_colon_colon);
"preceding-sibling::"             yield (preceding_sibling_colon_colon);
"@"                               yield ('@');

"text"{_}*"("                     yield (text_lparen);
"comment"{_}*"("                  yield (comment_lparen);
"node"{_}*"("                     yield (node_lparen);
"processing-instruction"{_}*"("   yield (processing_instruction_lparen);

"*:*"                             yield ('*');
{NCName}":*"                      { /* return wildcard as QName */
                                    pflval.qname = PFstr_qname (yytext);

                                    yield (NCName_colon_star);
                                  }
"*:"{NCName}                      { /* return wildcard as QName */
                                    pflval.qname = PFstr_qname (yytext);

                                    yield (star_colon_NCName);
                                  }
{QName}                           { /* return the QName as-is */
				    pflval.qname = PFstr_qname (yytext);

                                    yield (QName);
                                  }

{ExprComment}                     ; 
{S}                               ;

}


<NAMESPACEKEYWORD>{

{StringLiteral}                   { /* return string between `"' or `''
                                     * represented by the string literal,
                                     * expand escaped quotation marks
                                     */
                                    pflval.str = expand_esc_quots (
                                                   strpbrk (yytext, "'\""));

                                    gotoState (DEFAULT);
                                    yield (StringLiteral);
				  }

"namespace"                       { gotoState (NAMESPACEDECL);
                                    yield (namespace);
                                  }

"default"{_}"element"             yield (default_element);
"default"{_}"function"            yield (default_function);

{ExprComment}                     ; 
{S}                               ;

}

<NAMESPACEDECL>{

"="                               yield ('=');
{NCNameForPrefix}                 { /* return the NCName as-is */
                                    pflval.str = PFstrdup (yytext);

				    yield (NCNameForPrefix);
				  }
{URLLiteral}                      { /* return string between `"' or `''
                                     * represented by the string literal,
                                     * expand escaped quotation marks
                                     */
                                    pflval.str = expand_esc_quots (
                                                   strpbrk (yytext, "'\""));

                                    gotoState (DEFAULT);
                                    yield (URLLiteral);
				  }

{ExprComment}                     ; 
{S}                               ;

}


<XMLSPACE_DECL>{

"preserve"                        { gotoState (DEFAULT);
                                    yield (preserve);
                                  }
"strip"                           { gotoState (DEFAULT);
                                    yield (strip);
                                  }

"="                               yield ('=');

{ExprComment}                     ; 
{S}                               ;

}


<ITEMTYPE>{

"attribute"                       { gotoState (DEFAULT);
                                    yield (attribute);
                                  }
"element"                         { gotoState (DEFAULT);
                                    yield (element);
                                  }
"node"                            { gotoState (DEFAULT);
                                    yield (node);
                                  }
"document"                        { gotoState (DEFAULT);
                                    yield (document);
                                  }
"comment"                         { gotoState (DEFAULT);
                                    yield (comment);
                                  }
"text"                            { gotoState (DEFAULT);
                                    yield (text);
                                  }
"processing-instruction"          { gotoState (DEFAULT);
                                    yield (processing_instruction);
                                  }
"item"                            { gotoState (DEFAULT);
                                    yield (item);
                                  }
"atomic"{_}"value"                { gotoState (DEFAULT);
                                    yield (atomic_value);
                                  }
"empty"                           { gotoState (DEFAULT);
                                    yield (empty_);
                                  }

"as"                              yield (as);

{QName}                           { /* return the QName (named type) */
				    pflval.qname = PFstr_qname (yytext);

				    gotoState (DEFAULT);
                                    yield (QName);
                                  }

"$"                               { pushState (ITEMTYPE);
                                    gotoState (VARNAME); 
                                    yield ('$');
                                  }

{ExprComment}                     ; 
{S}                               ;

}


<VARNAME>{

{VarName}                         { /* return the QName */
				    pflval.qname = PFstr_qname (yytext);

                                    popState ();
                                    yield (VarName);
                                  }

{ExprComment}                     ; 
{S}                               ;

}

<START_TAG>{

"{"                               { pushState (START_TAG);
				    gotoState (DEFAULT);
				    yield ('{');
                                  }

">"                               { gotoState (ELEMENT_CONTENT);
                                    yield ('>');
                                  }

"\""                              { gotoState (QUOT_ATTRIBUTE_CONTENT);
	                            yield ('"');
                                  }

"'"                               { gotoState (APOS_ATTRIBUTE_CONTENT);
                                    yield ('\'');
                                  }

{S}                               yield (S);
{QName}                           { /* return the tag name */
				    pflval.qname = PFstr_qname (yytext);

				    yield (QName);
				  }
"="                               yield ('=');
"/>"                              { popState ();
				    yield (slash_gt);
                                  }
}

<ELEMENT_CONTENT>{

"{"                               { pushState (ELEMENT_CONTENT);
                                    gotoState (DEFAULT);
                                    yield ('{');
                                  }

"<"                               { pushState (ELEMENT_CONTENT);
                                    gotoState (START_TAG);
                                    yield ('<');
                                  }

"</"                              { gotoState (END_TAG);
                                    yield (lt_slash);
                                  }

"<!--"                            { pushState (ELEMENT_CONTENT);
                                    gotoState (XML_COMMENT);
                                    yield (xml_comment_start);
                                  }

"<?"                              { pushState (ELEMENT_CONTENT);
				    gotoState (PROCESSING_INSTRUCTION);
                                    yield (pi_start);
                                  }

"<![CDATA["                       { pushState (ELEMENT_CONTENT);
				    gotoState (CDATA_SECTION);
				    yield (cdata_start);

				    /* !W3C we do not recognize "]]>" here */
				  }

{ExprComment}                     ;

{PredefinedEntityRef}             { /* map predefined entity and return
                                     * character it represents
                                     */
                                    pflval.chr = 
                                      predefined_entity (yytext + 1); 

                                    yield (PredefinedEntityRef);
                                  }
{CharRef}                         { /* return the character referenced
                                     * by its (hexa)decimal code value
                                     */
                                    pflval.chr = char_ref (yytext + 2);

				    yield (CharRef);
                                  }				    

"{{"                              yield (lbrace_lbrace);
"}}"                              yield (rbrace_rbrace);

{Char}                            { /* return the character itself */
                                    pflval.chr = *yytext;

				    yield (Char);
				  }
}

<END_TAG>{

"{"                               { pushState (END_TAG);
                                    gotoState (DEFAULT);
                                    yield ('{');
                                  }

{S}                               yield (S);

{QName}                           { /* return the tag name */
				    pflval.qname = PFstr_qname (yytext);

				    yield (QName);
				  }

">"                               { popState ();
                                    yield ('>');
                                  }
}

<XML_COMMENT>{

{Char}                            { /* return the character itself */
                                    pflval.chr = *yytext;

				    yield (Char);
				  }
"-->"                             { popState ();
                                    yield (xml_comment_end);
                                  }
}


<PROCESSING_INSTRUCTION>{

{PITarget}                        { /* return the processing-instruction
                                     * target as-is
                                     */
				    pflval.str = PFstrdup (yytext);

                                    gotoState (PROCESSING_INSTRUCTION_CONTENT);
				    yield (PITarget);
                                  }
}

<PROCESSING_INSTRUCTION_CONTENT>{

{Char}                            { /* return the character itself */
                                    pflval.chr = *yytext;

				    yield (Char);
				  }

"?>"                              { popState ();
                                    yield (pi_end);
                                  }
}

<CDATA_SECTION>{

"]]>"                             { popState ();
                                    yield (cdata_end);
                                  }

{Char}                            { /* return the character itself */
                                    pflval.chr = *yytext;

				    yield (Char);
				  }
}

<QUOT_ATTRIBUTE_CONTENT>{

"{"                               { pushState (QUOT_ATTRIBUTE_CONTENT);
				    gotoState (DEFAULT);
                                    yield ('{');
				  }

"\""                              { gotoState (START_TAG);
                                    yield ('"');
                                  }

{EscapeQuot}                      yield (EscapeQuot);
{PredefinedEntityRef}             { /* map predefined entity and return
                                     * character it represents
                                     */
                                    pflval.chr = 
                                      predefined_entity (yytext + 1); 

                                    yield (PredefinedEntityRef);
                                  }
{CharRef}                         { /* return the character referenced
                                     * by its (hexa)decimal code value
                                     */
                                    pflval.chr = char_ref (yytext + 2);

				    yield (CharRef);
				  }
"{{"                              yield (lbrace_lbrace);
"}}"                              yield (rbrace_rbrace);

{Char}                            { /* return the character itself */
                                    pflval.chr = *yytext;
 
                                    yield (Char);  
                                  }
}

<APOS_ATTRIBUTE_CONTENT>{

"{"                               { pushState (APOS_ATTRIBUTE_CONTENT);
				    gotoState (DEFAULT);
                                    yield ('{');
				  }

"'"                               { gotoState (START_TAG);
                                    yield ('\'');
                                  }

{EscapeApos}                      yield (EscapeApos);
{PredefinedEntityRef}             { /* map predefined entity and return
                                     * character it represents
                                     */
                                    pflval.chr = 
                                      predefined_entity (yytext + 1); 

                                    yield (PredefinedEntityRef);
                                  }
{CharRef}                         { /* return the character referenced
                                     * by its (hexa)decimal code value
                                     */
                                    pflval.chr = char_ref (yytext + 2);

				    yield (CharRef);
                                  }

"{{"                              yield (lbrace_lbrace);
"}}"                              yield (rbrace_rbrace);

{Char}                            { /* return the character itself */
                                    pflval.chr = *yytext;
 
                                    yield (Char);  
                                  }
}

<*>{
.|"\n"                            yield (*yytext);
}

%%

/* Map an XML predefined entity and return the
 * character it represents.
 */
static char predefined_entity (char *ent)
{
    switch (*ent) {
        case 'l': /* &lt; */
            return '<';
        case 'g': /* &gt; */
            return '>';
        case 'q': /* &quot; */
            return '\"';
        case 'a': /* &amp; or &apos; */
            return (*(ent + 1) == 'm') ? '&' : '\'';
        default:  /* entity unknown to us */
            return '\0';
    }
}

/* Return the character referenced by the given
 * (hexa)decimal code value.
 */
static char char_ref (char *ref)
{
    switch (*ref) {
        case 'x': /* hexadecimal */
            return (char) strtoul (ref + 1, (char **) 0, 16);
        default: /* decimal */
            return (char) strtoul (ref,     (char **) 0, 10);
    }
}

/** 
 * Expand escaped quotation marks in a string literal:
 * ".."".." --> "..".."
 * '..''..' --> '..'..'
 *
 * @param quot string to expand
 * @return expanded string (newly allocated)
 */
static char *expand_esc_quots (char *quot)
{
    /* string with escaped quotation marks expanded */
    char *e, *exp;
    /* remember surrounding quote type (' or ") */
    char quote = *quot;
    /* cut off closing quote */
    *(quot + strlen (quot) - 1) = '\0';

    exp = (char *) PFmalloc (strlen (quot));

    e = exp;

    while (*++quot)
        switch (*quot == quote) {
            case true:
                if (*++quot == quote)
                    *e++ = quote;
                else
                    /* shouldn't happen (due to lexer regexp) */
                    return 0;
                break;
            default:
                *e++ = *quot;
        }

    *e = '\0';

    return exp;
}

/* vim:set shiftwidth=4 expandtab: */

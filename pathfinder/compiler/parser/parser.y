/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * XQuery (W3C WD November 15, 2002) grammar description
 * and parse tree construction.  Feed this into `bison'.
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
#include "pathfinder.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "parser.h"

/* PFarray_t */
#include "array.h"
#include "oops.h"

/** root node of the parse tree */
static PFpnode_t *root;

/* temporay node memory */
static PFpnode_t *c, *c1;

/* avoid `implicit declaration of yylex' warning */
extern int yylex (void);

/* bison error callback */
void yyerror (const char *s);

/* bison: generate verbose parsing error messages */
#define YYERROR_VERBOSE 

/* In several cases, the semantic actions of a grammar rule cannot
 * construct a complete abstract syntax tree, e.g., consider the
 * generation of a right-deep abstract syntax tree from a left-recursive
 * grammar rule.
 *
 * Whenever such a situation arises, we let the semantic action
 * construct as much of the tree as possible with parts of the tree
 * unspecified.  The semantic action then returns the ROOT of this
 * tree as well as pointer to the node under which the yet unspecified
 * tree part will reside; this node is subsequently referred to as the
 * HOLE.
 *
 * Below, ROOT and HOLE are collected into an array of abstract syntax
 * tree pointers of length 2.
 */
typedef PFpnode_t *phole_t[2];

#define ROOT 0
#define HOLE 1

/* fix-up the HOLE of an abstract syntax tree t and 
 * replace its c-th leaf by e:
 *
 *               t[ROOT]
 *                 / \
 *     t[HOLE] --> -e--
 */
#define FIXUP(c,t,e) ((t)[HOLE])->child[c] = (e)

/* recursively flatten a location path @a p */
static PFpnode_t *flatten_locpath (PFpnode_t *p, PFpnode_t *r);

/* construct a nil node */
#define nil(loc) p_leaf (p_nil, (loc))

/* scanner information to provide better error messages */
extern char *yytext;
extern int yylineno;

/**
 * remember if boundary whitespace is to be stripped or preserved;
 * XQuery default is to remove boundary whitespace (can also be 
 * explicitely specified by declaring "xmlspace = strip"); if it is
 * to be kept, "xmlspace = preserve" must be specified in the prolog.
 */
static bool xmlspace_preserve = false;

/*
 * Check if the input string consists of whitespace only.
 * If this is the case and xmlspace_preserve is set to false, the 
 * abstract syntax tree must be altered, i.e., we do not
 * create a text node but an empty sequence node.
 */
static bool is_whitespace (char *s);

/**
 * override default bison type for locations with custom type
 * defined in include/abssyn.h
 */
#define YYLTYPE PFloc_t

/** 
 * override bison default location tracking for a rule
 * of the form (rhs consists of n symbols, n >= 0):
 *                   lhs: rhs 
 */
#define YYLLOC_DEFAULT(lhs, rhs, n)                   \
  do                                                  \
      if ((n)) {                                      \
        (lhs).first_row  = (rhs)[1].first_row;        \
        (lhs).first_col  = (rhs)[1].first_col;        \
        (lhs).last_row   = (rhs)[(n)].last_row;       \
        (lhs).last_col   = (rhs)[(n)].last_col;       \
      }                                               \
      else                                            \
        (lhs) = (rhs)[0];                             \
 while (0)

/* calculate new location when joining two or more abssyn nodes */
static PFloc_t 
loc_rng (PFloc_t left, PFloc_t right) 
{
  PFloc_t loc;

  loc.first_row = left.first_row;
  loc.first_col = left.first_col;

  loc.last_row = right.last_row;
  loc.last_col = right.last_col;

  return loc;
}

/** calculate maximum location when joining two nodes */
static PFloc_t 
max_loc (PFloc_t loc1, PFloc_t loc2) 
{
    PFloc_t loc = loc1;
    /* verify if loc1's location begins after loc2's location;
     * in this case, extend loc1 to begin at loc2's start-location
     */
    if (loc1.first_row == loc2.first_row)
    {
        if (loc1.first_col > loc2.first_col)
            loc.first_col = loc2.first_col;
    }
    else if (loc1.first_row > loc2.first_row)
    {
        loc.first_row = loc2.first_row;
        loc.first_col = loc2.first_col;
    }

    /* verify if loc1's location ends before loc2's location;
     * in this case, extend loc1 to end at loc2's end-location
     */
    if (loc1.last_row == loc2.last_row)
    {
        if (loc1.last_col < loc2.last_col)
            loc.last_col = loc2.last_col;
    }
    else if (loc1.last_row < loc2.last_row)
    {
        loc.last_row = loc2.last_row;
        loc.last_col = loc2.last_col;
    }

    return loc;
}

%}

/* And finally: enable automatic location tracking (@n)
 */
%locations

/* semantic actions compute semantic values 
 * of the following types
 * (cf. field sem of PFpnode_t)
 */
%union {
    int          num;
    double       dec;
    double       dbl;
    bool         tru;
    char        *str;
    char         chr;
    PFqname_t    qname;
    PFpnode_t   *pnode;
    PFpnode_t   *phole[2];
    PFptype_t    ptype;
    PFpaxis_t    axis;
    PFpsort_t    mode;
    PFpoci_t     oci;
    PFarray_t   *buf;
}

/* terminals (see W3C XQuery, A.1.1 and A1.2)
 *
 * We list the terminal names alphabetically here.  If appropiate
 * we use 
 * (1) the character sequence associated with the terminal as its
 *     name (e.g., "comment" --> comment, "if{S}(" --> if_lparen),
 * (2) terminals associated with XQuery syntacic constructs are
 *     named like the construct (e.g., StringLiteral), and
 * (3) we add a trailing underscore to avoid clashed with reserved 
 *     C keywords (e.g., "default" --> default_).
 */

%token Char
%token CharRef
%token DecimalLiteral
%token DoubleLiteral
%token EscapeApos
%token EscapeQuot
%token IntegerLiteral
%token NCName_colon_star                "NCName:*"
%token PITarget
%token PredefinedEntityRef
%token QName                            
%token S
%token StringLiteral                    
%token URLLiteral
%token VarName
%token ancestor_colon_colon             "ancestor::"
%token ancestor_or_self_colon_colon     "ancestor-or-self::" 
%token and                              "and"
%token as                               "as"
%token ascending                        "ascending"
%token at                               "at"
%token at_StringLiteral                 "at StringLiteral"
%token atomic_value                     "atomic value"
%token attribute                        "attribute"
%token attribute_QName_lbrace           "attribute QName {"
%token attribute_colon_colon            "attribute::"
%token attribute_lbrace                 "attribute {"
%token case_                            "case"
%token cast_as                          "cast as"
%token castable_as                      "castable as"
%token cdata_end                        "]]>"
%token cdata_start                      "<![CDATA["
%token child_colon_colon                "child::"
%token collation                        "collation"
%token colon_equals                     ":="
%token comment                          "comment"
%token comment_lparen                   "comment ("
%token context                          "context"
%token declare_namespace                "declare namespace"
%token declare_xmlspace                 "declare xmlspace"
%token default_                         "default"
%token default_collation_equals         "default collation ="
%token default_element                  "default element"
%token default_function                 "default function"
%token define_function                  "define function"
%token descendant_colon_colon           "descendant::"
%token descendant_or_self_colon_colon   "descendant-or-self::"
%token descending                       "descending"
%token div_                             "div"
%token document                         "document"
%token document_lbrace                  "document {"
%token dot_dot                          ".."
%token element                          "element"
%token element_QName_lbrace             "element QName {"
%token element_lbrace                   "element {"
%token else_                            "else"
%token empty_                           "empty"
%token empty_greatest                   "empty greatest"
%token empty_least                      "empty least"
%token eq                               "eq"
%token NCNameForPrefix                  "NCNameForPrefix"
%token every_dollar                     "every $"
%token except                           "except"
%token following_colon_colon            "following::"
%token following_sibling_colon_colon    "following-sibling::"
%token for_dollar                       "for $"
%token ge                               "ge"
%token gt                               "gt"
%token gt_gt                            ">>"
%token gte                              ">="
%token idiv                             "idiv"
%token if_lparen                        "if ("
%token import_schema                    "import schema"
%token in                               "in"
%token instance_of                      "instance of"
%token intersect                        "intersect"
%token is                               "is"
%token isnot                            "isnot"
%token item                             "item"
%token lbrace_lbrace                    "{{"
%token le                               "le"
%token let_dollar                       "let $"
%token lt                               "lt"
%token lt_s                             "< "
%token lt_lt                            "<<"
%token lt_slash                         "</"
%token lte                              "<="
%token mod                              "mod"
%token namespace                        "namespace"
%token ne                               "ne"
%token neq                              "!="
%token node                             "node"
%token node_lparen                      "node ("
%token of_type                          "of type"
%token or                               "or"
%token order_by                         "order by"
%token parent_colon_colon               "parent::"
%token pi_end                           "?>"
%token pi_start                         "<?"
%token preceding_colon_colon            "preceding::"
%token preceding_sibling_colon_colon    "preceding-sibling::"
%token preserve                         "preserve"
%token processing_instruction           "processing-instruction"
%token processing_instruction_lparen    "processing-instruction ("
%token rbrace_rbrace                    "}}"
%token return_                          "return"
%token rparen_as                        ") as"
%token satisfies                        "satisfies"
%token self_colon_colon                 "self::"
%token slash_gt                         "/>"
%token slash_slash                      "//"
%token some_dollar                      "some $"
%token stable_order_by                  "stable order by"
%token star_colon_NCName                "*:NCName"
%token strip                            "strip"
%token text                             "text"
%token text_lbrace                      "text {"
%token text_lparen                      "text ("
%token then_                            "then"
%token to                               "to"
%token treat_as                         "treat as"
%token type_QName                       "type QName"
%token typeswitch_lparen                "typeswitch ("
%token union_                           "union"
%token validate                         "validate"
%token validate_lbrace                  "validate {"
%token where                            "where"
%token xml_comment_end                  "-->"
%token xml_comment_start                "<!--"

%type <str>    "at StringLiteral"
               StringLiteral
               URLLiteral
               NCNameForPrefix
               PITarget
               OptCollation_

%type <qname>  "attribute QName {" 
               "element QName {"
               "type QName"
               "*:NCName"
               "NCName:*"
               QName
               VarName
               WildCard
               AtomicType

%type <num>    IntegerLiteral
               OptAscDesc_
               OptEmpty_

%type <dec>    DecimalLiteral
               DoubleLiteral

%type <chr>    PredefinedEntityRef
               CharRef
               Char

%type <phole>  Predicates
               ForLetClauses_ 
               AttributeList
               SchemaSrc_
               SchemaImport

%type <ptype>  SomeEvery_
               Comparison_
               ValueComp
               GeneralComp
               NodeComp
               OrderComp

%type <axis>   ForwardAxis
               ReverseAxis

%type <mode>   OrderModifier

%type <oci>    OccurrenceIndicator

%type <buf>    ElementContentTexts_
               Chars_
               AposAttributeContentTexts_
               QuotAttributeContentTexts_

%type <chr>    ElementContentText_
               AposAttributeContentText_
               QuotAttributeContentText_

%type <pnode>  AbbreviatedForwardStep AbbreviatedReverseStep
               AdditiveExpr AndExpr AnyKindTest
               AposAttributeValueContents_ 
               AposAttributeContent 
               AttributeValue
               CaseClause CaseClauses_ CastableExpr
               CastExpr
               CdataSection CommentTest ComparisonExpr
               Characters_
               ComputedAttributeConstructor
               ComputedDocumentConstructor ComputedElementConstructor
               ComputedTextConstructor
               Constructor DeclsImports_ DefaultCollationDecl
               DefaultNamespaceDecl ElemAtt_ ElemOrAttrType
               ElementConstructor ElementContent
               ElementContents_
               EnclosedExpr Expr ExprSequence FLWRExpr ForwardStep
               FuncArgList_ FunctionCall FunctionDefn FunctionDefns_
               IfExpr InstanceofExpr IntersectExceptExpr ItemType
               KindTest Literal MultiplicativeExpr NamespaceDecl
               NameTest NodeTest NumericLiteral OptElemOrAttrType_
               OptExprSequence_ OptFuncArgList_ OptParamList_
               OptAs_ OptSchemaContext_ OptSchemaLoc_
               OptTypeDeclaration_
               OptVarName_ OptWhereClause_ OptOrderByClause_ 
               OptPositionalVar_
               OrderByClause OrderSpecList OrExpr Param ParamList
               ParenthesizedExpr PathExpr PositionalVar PrimaryExpr
               ProcessingInstructionTest QuantifiedExpr Query
               QueryBody QueryProlog
               QuotAttributeValueContents_
               QuotAttributeContent
               RangeExpr RelativePathExpr ReverseStep SchemaContext
               SchemaContextStep SchemaContextSteps_
               SchemaGlobalContext SchemaType SequenceType SingleType 
               StepExpr Step_ SubNamespaceDecl TextTest
               TreatExpr TypeDeclaration TypeswitchExpr UnaryExpr UnionExpr
               ValidateExpr ValueExpr WhereClause
               XmlComment XmlProcessingInstruction ForLetClause_
               ForClause LetClause LetBindings_
               VarBindings_ VarPosBindings_ XMLSpaceDecl

/* We expect a harmless shift/reduce conflict (interpret a `*' following
 * a path expression as wildcard name test instead as the binary
 * multiplication operator:
 *
 *                 / * foo      parsed as     (/*) foo 
 *         
 * (use parentheses if you mean (/) * foo).
 *
 * Five more harmless shift/reduce conflicts are in the rules for
 * ElementContents_. Characters (or the other four choices in
 * ElementContentText_) can either be immediately reduced up to
 * ElementContents_ (ending up in many text nodes that only contain
 * one character) or shifted to collect characters according to
 * ElementContentTexts_ :: ElementContentTexts_ ElementContentText_.
 * The latter is what we want; bison's default is to shift, so we're
 * all set.
 */
%expect 18

%%

/* non-termninals (see W3C XQuery, A.2 (BNF))
 *
 * (The [numbers] refer to the grammar rules in the W3C XQuery draft.)
 */


/* [21] */
Query                       : QueryProlog QueryBody
                              { /* assign parse tree root */
                                root = $$ = p_wire2 (p_xquery, @$, $1, $2); 
                              }     
                            ;

/* [22] */
QueryProlog                 : DeclsImports_ FunctionDefns_
                              { $$ = p_wire2 (p_prolog, @$, $1, $2); }
                            ;

DeclsImports_               : /* empty */
                              { $$ = nil (@$); }
                            | NamespaceDecl DeclsImports_
                              { $$ = p_wire2 (p_decl_imps, @$, $1, $2); }
                            | XMLSpaceDecl DeclsImports_
                              { $$ = p_wire2 (p_decl_imps, @$, $1, $2); }
                            | DefaultNamespaceDecl DeclsImports_
                              { $$ = p_wire2 (p_decl_imps, @$, $1, $2); }
                            | DefaultCollationDecl DeclsImports_
                              { $$ = p_wire2 (p_decl_imps, @$, $1, $2); }
                            | SchemaImport DeclsImports_
                              { /* FIXME: check correct location settings
                                 */
                                $$ = p_wire2 (p_decl_imps, 
                                              @$,
                                              $1[ROOT],
                                              $1[HOLE]
                                              ? p_wire2 (p_decl_imps,
                                                         loc_rng($1[HOLE]->loc,
                                                                 @2),
                                                         $1[HOLE],
                                                         $2)
                                              : $2); 
                              }
                            ;

FunctionDefns_              : /* empty */
                              { $$ = nil (@$); }
                            | FunctionDefn FunctionDefns_
                              { $$ = p_wire2 (p_fun_decls, @$, $1, $2); }
                            ;

/* [23] */
QueryBody                   : OptExprSequence_
                              { $$ = $1; }
                            ;

OptExprSequence_            : /* empty */
                              { $$ = p_leaf (p_empty_seq, @$); }
                            | ExprSequence
                              { $$ = $1; }
                            ;

/* [24] */
ExprSequence                : Expr
                              { $$ = p_wire2 (p_exprseq, @$,
                                              $1,
                                              p_leaf (p_empty_seq, @1)); }
                            | Expr ',' ExprSequence
                              { $$ = p_wire2 (p_exprseq, @$, $1, $3); }
                            ;

/* [25] */
Expr                        : OrExpr
                              { $$ = $1; }
                            ;

/* [26] */
OrExpr                      : AndExpr
                              { $$ = $1; }
                            | OrExpr "or" AndExpr
                              { $$ = p_wire2 (p_or, @$, $1, $3); }
                            ;

/* [27] */
AndExpr                     : FLWRExpr
                              { $$ = $1; }
                            | AndExpr "and" FLWRExpr 
                              { $$ = p_wire2 (p_and, @$, $1, $3); }
                            ;


/* [28] */
FLWRExpr                    : QuantifiedExpr
                              { $$ = $1; }
                            | ForLetClauses_ 
                              OptWhereClause_ OptOrderByClause_
                              "return" FLWRExpr
                              { $$ = p_wire4 (p_flwr, @$, 
                                              $1[ROOT], $2, $3, $5); 
                              }
                            ;

ForLetClauses_              : ForLetClause_
                              { $$[ROOT] = $$[HOLE] = $1; }
                            | ForLetClauses_ ForLetClause_
                              { FIXUP (1, $1, $2);
                                $$[HOLE] = $2;
                                $$[ROOT] = $1[ROOT];
                              }
                            ;

ForLetClause_               : ForClause
                              { $$ = $1; }
                            | LetClause
                              { $$ = $1; }
                            ;

OptWhereClause_             : /* empty */
                              { $$ = nil (@$); }
                            | WhereClause
                              { $$ = $1; }
                            ;

OptOrderByClause_           : /* empty */
                              { $$ = nil (@$); }
                            | OrderByClause
                              { $$ = $1; }
                            ;

/* [29] */
QuantifiedExpr              : TypeswitchExpr
                              { $$ = $1; }
                            | SomeEvery_ VarBindings_ 
                              "satisfies" QuantifiedExpr
                              { $$ = p_wire2 ($1, @$, $2, $4); }
                            ;

SomeEvery_                  : "some $"
                              { $$ = p_some; }
                            | "every $"
                              { $$ = p_every; }
                            ;

VarBindings_                : VarName OptTypeDeclaration_ "in" Expr
                              { $$ = p_wire2 (p_binds,
                                              @$,
                                              p_wire4 (p_bind,
                                                       loc_rng (@1, @4),
                                                       $2,
                                                       nil (loc_rng (@1, @4)),
                                                       (c = p_leaf (p_varref, 
                                                                    @1),
                                                        c->sem.qname = $1,
                                                        c),
                                                       $4),
                                              nil (loc_rng (@1, @4))); 
                              }
                            | VarName OptTypeDeclaration_ "in" Expr
                              ',' '$' VarBindings_
                              { $$ = p_wire2 (p_binds, @$,
                                              p_wire4 (p_bind, 
                                                       loc_rng (@1, @4),
                                                       $2,
                                                       nil (loc_rng (@1, @4)),
                                                       (c = p_leaf (p_varref, 
                                                                    @1),
                                                        c->sem.qname = $1,
                                                        c),
                                                       $4),
                                              $7);
                              }
                            ;


OptTypeDeclaration_         : /* empty */
                              { $$ = nil (@$); }
                            | TypeDeclaration
                              { $$ = $1; }
                            ;

/* [30] */
TypeswitchExpr              : IfExpr
                              { $$ = $1; }
                            | "typeswitch (" Expr ')' CaseClauses_
                              "default" OptVarName_ "return" TypeswitchExpr
                              { $$ = p_wire4 (p_typeswitch, @$, 
                                              $2, $4, $6, $8); 
                              }
                            ;

OptVarName_                 : /* empty */
                              { $$ = nil (@$); }
                            | '$' VarName
                              { ($$ = p_leaf (p_varref, @$))->sem.qname = $2; }
                            ;

CaseClauses_                : CaseClause
                              { $$ = p_wire2 (p_cases, @$, $1, nil (@1)); }
                            | CaseClause CaseClauses_
                              { $$ = p_wire2 (p_cases, @$, $1, $2); }
                            ;

/* [31] */
IfExpr                      : InstanceofExpr
                              { $$ = $1; }
                            | "if (" Expr ')' "then" Expr "else" IfExpr
                              { $$ = p_wire3 (p_if, @$, $2, $5, $7); }
                            ;
                              
/* [32] */
InstanceofExpr              : CastableExpr
                              { $$ = $1; }
                            | CastableExpr "instance of" SequenceType
                              { $$ = p_wire2 (p_instof, @$, $1, $3); }
                            ;

/* [33] */
CastableExpr                : ComparisonExpr
                              { $$ = $1; }
                            | ComparisonExpr "castable as" SingleType
                              { $$ = p_wire2 (p_castable, @$, $1, $3); }
                            ;

/* [34] */
ComparisonExpr              : RangeExpr
                              { $$ = $1; }
                            | RangeExpr Comparison_ RangeExpr
                              { $$ = p_wire2 ($2, @$, $1, $3); }
                            ;

Comparison_                 : ValueComp
                              { $$ = $1; }
                            | GeneralComp
                              { $$ = $1; }
                            | NodeComp
                              { $$ = $1; }
                            | OrderComp
                              { $$ = $1; }
                            ;

/* [35] */
RangeExpr                   : AdditiveExpr 
                              { $$ = $1; }
                            | AdditiveExpr "to" AdditiveExpr
                              { $$ = p_wire2 (p_range, @$, $1, $3); }
                            ;

/* [36] */
AdditiveExpr                : MultiplicativeExpr
                              { $$ = $1; }
                            | AdditiveExpr '+' MultiplicativeExpr
                              { $$ = p_wire2 (p_plus, @$, $1, $3); }
                            | AdditiveExpr '-' MultiplicativeExpr
                              { $$ = p_wire2 (p_minus, @$, $1, $3); }
                            ;

/* [37] */
MultiplicativeExpr          : UnaryExpr
                              { $$ = $1; }
                            | MultiplicativeExpr '*'    UnaryExpr
                              { $$ = p_wire2 (p_mult, @$, $1, $3); }
                            | MultiplicativeExpr "div"  UnaryExpr
                              { $$ = p_wire2 (p_div, @$, $1, $3); }
                            | MultiplicativeExpr "idiv" UnaryExpr
                              { $$ = p_wire2 (p_idiv, @$, $1, $3); }
                            | MultiplicativeExpr "mod"  UnaryExpr
                              { $$ = p_wire2 (p_mod, @$, $1, $3); }
                            ;

/* [38] */
UnaryExpr                   : UnionExpr
                              { $$ = $1; }
                            | '-' UnionExpr
                              { $$ = p_wire1 (p_uminus, @$, $2); }
                            | '+' UnionExpr
                              { $$ = p_wire1 (p_uplus, @$, $2); }
                            ;

/* [39] */
UnionExpr                   : IntersectExceptExpr 
                              { $$ = $1; }
                            | UnionExpr "union" IntersectExceptExpr
                              { $$ = p_wire2 (p_union, @$, $1, $3); }
                            | UnionExpr '|'     IntersectExceptExpr
                              { $$ = p_wire2 (p_union, @$, $1, $3); }
                            ;

/* [40] */
IntersectExceptExpr         : ValueExpr
                              { $$ = $1; }
                            | IntersectExceptExpr "intersect" ValueExpr
                              { $$ = p_wire2 (p_intersect, @$, $1, $3); }
                            | IntersectExceptExpr "except"    ValueExpr
                              { $$ = p_wire2 (p_except, @$, $1, $3); }
                            ;

/* [41] */
ValueExpr                   : ValidateExpr
                              { $$ = $1; }
                            | CastExpr
                              { $$ = $1; }
                            | TreatExpr
                              { $$ = $1; }
                            | Constructor
                              { $$ = $1; }
                            | PathExpr
                              { $$ = flatten_locpath ($1, 0); }
                            ;

/* [42] */
PathExpr                    : '/'
                              { $$ = p_leaf (p_root, @$); }
                            | '/' RelativePathExpr
                              { $$ = p_wire2 (p_locpath, @$, 
                                              $2, p_leaf (p_root, @1)); }
                            | "//" RelativePathExpr
                              { $$ = p_wire2 (
                                      p_locpath,
                                      @$,
                                      p_wire2 (
                                       p_locpath,
                                       @$,
                                       $2,
                                       p_wire2 (
                                        p_locpath,
                                        @1,
                                        (c = p_wire1 (
                                              p_step,
                                              @1,
                                              (c1 = p_wire1 (p_kindt,
                                                             @1, nil (@1)),
                                               c1->sem.kind = p_kind_node,
                                               c1)),
                                         c->sem.axis = p_descendant_or_self,
                                         c),
                                        p_leaf (p_dot, @1))),
                                      p_leaf (p_root, @1));
                              }
                            | RelativePathExpr
                              { $$ = $1; }
                            ;

/* [43] */
RelativePathExpr            : StepExpr
                              { $$ = $1; }
                            | StepExpr '/' RelativePathExpr
                              { $$ = p_wire2 (p_locpath, @$, $3, $1); }
                            | StepExpr "//" RelativePathExpr 
                              { $$ = p_wire2 (
                                      p_locpath, 
                                      @$,
                                      p_wire2 (
                                       p_locpath,
                                       @$,
                                       $3,
                                       p_wire2 (
                                        p_locpath, @2,
                                        (c = p_wire1 (
                                              p_step,
                                              @2,
                                              (c1 = p_wire1 (p_kindt, @2,
                                                             nil (@2)),
                                               c1->sem.kind = p_kind_node,
                                               c1)),
                                         c->sem.axis = p_descendant_or_self,
                                         c),
                                        p_leaf (p_dot, @2))),
                                      $1);
                              }
                            ;
/* [44] */
StepExpr                    : Step_ Predicates
                              { $$ = ($2[ROOT] ? FIXUP (0, $2, $1), $2[ROOT] :
                                                 $1); 
                              }
                            ;

Step_                       : ForwardStep
                              { $$ = p_wire2 (p_locpath, @$, $1,
                                              p_leaf (p_dot, @$)); }
                            | ReverseStep
                              { $$ = p_wire2 (p_locpath, @$, $1,
                                              p_leaf (p_dot, @$)); }
                            | PrimaryExpr
                              { $$ = $1; }
                            ;

/* [45] */
ForClause                   : "for $" VarPosBindings_
                              { $$ = $2; }
                            ;
                      

VarPosBindings_             : VarName OptTypeDeclaration_ OptPositionalVar_
                              "in" Expr
                              { $$ = p_wire2 (p_binds,
                                              @$,
                                              p_wire4 (p_bind,
                                                       loc_rng (@1, @5),
                                                       $2,
                                                       $3,
                                                       (c = p_leaf (p_varref, 
                                                                    @1),
                                                        c->sem.qname = $1,
                                                        c),
                                                       $5),
                                              nil (loc_rng (@1, @5))); 
                              }
                            | VarName OptTypeDeclaration_ OptPositionalVar_
                              "in" Expr ',' '$' VarPosBindings_
                              { $$ = p_wire2 (p_binds, @$,
                                              p_wire4 (p_bind, 
                                                       loc_rng (@1, @5),
                                                       $2,
                                                       $3,
                                                       (c = p_leaf (p_varref, 
                                                                    @1),
                                                        c->sem.qname = $1,
                                                        c),
                                                       $5),
                                              $8);
                              }
                            ;

OptPositionalVar_           : /* empty */
                              { $$ = nil (@$); }
                            | PositionalVar
                              { $$ = $1; }
                            ;

/* [46] */
LetClause                   : "let $" LetBindings_
                              { $$ = $2; }
                            ;

LetBindings_                : VarName OptTypeDeclaration_ ":=" Expr
                              { $$ = p_wire2 (p_binds,
                                              @$,
                                              p_wire3 (p_let,
                                                       loc_rng (@1, @4),
                                                       $2,
                                                       (c = p_leaf (p_varref, 
                                                                    @1),
                                                        c->sem.qname = $1,
                                                        c),
                                                       $4),
                                              nil (loc_rng (@1, @4))); 
                              }
                            | VarName OptTypeDeclaration_ ":=" Expr
                              ',' '$' LetBindings_
                              { $$ = p_wire2 (p_binds, @$,
                                              p_wire3 (p_let, 
                                                       loc_rng (@1, @4),
                                                       $2,
                                                       (c = p_leaf (p_varref, 
                                                                    @1),
                                                        c->sem.qname = $1,
                                                        c),
                                                       $4),
                                              $7);
                              }
                            ;

/* [47] */
WhereClause                 : "where" Expr
                              { $$ = $2; }
                            ;
/* [48] */
PositionalVar               : "at" '$' VarName
                              { ($$ = p_leaf (p_varref, loc_rng (@2, @3)))
                                  ->sem.qname = $3;
                              }
                            ;

/* [49] */
ValidateExpr                : "validate {" Expr '}'
                              { $$ = p_wire2 (p_validate, @$, nil (@$), $2); }
                            | "validate" SchemaContext '{' Expr '}'
                              { $$ = p_wire2 (p_validate, @$, $2, $4); }
                            ;

OptSchemaContext_           : /* empty */
                              { $$ = nil (@$); }
                            | SchemaContext
                              { $$ = $1; }
                            ;

/* [50] */
CastExpr                    : "cast as" SingleType ParenthesizedExpr
                              { $$ = p_wire2 (p_cast, @$, $2, $3); }
                            ;

/* [51] */
TreatExpr                   : "treat as" SequenceType ParenthesizedExpr
                              { $$ = p_wire2 (p_treat, @$, $2, $3); }
                            ;

/* [52] */
Constructor                 : ElementConstructor
                              { $$ = $1; }
                            | XmlComment
                              { $$ = $1; }
                            | XmlProcessingInstruction
                              { $$ = $1; }
                            | CdataSection
                              { $$ = $1; }
                            | ComputedDocumentConstructor
                              { $$ = $1; }
                            | ComputedElementConstructor
                              { $$ = $1; }
                            | ComputedAttributeConstructor
                              { $$ = $1; }
                            | ComputedTextConstructor
                              { $$ = $1; }
                            ;

/* [53] */
GeneralComp                 : '='
                              { $$ = p_eq; }
                            | "!="
                              { $$ = p_ne; }
                            | "< "
                              { $$ = p_lt; }
                            | "<="
                              { $$ = p_le; }
                            | '>'
                              { $$ = p_gt; }
                            | ">="
                              { $$ = p_ge; }
                            ;

/* [54] */
ValueComp                   : "eq"
                              { $$ = p_val_eq; }
                            | "ne"
                              { $$ = p_val_ne; }
                            | "lt"
                              { $$ = p_val_lt; }
                            | "le"
                              { $$ = p_val_le; }
                            | "gt"
                              { $$ = p_val_gt; }
                            | "ge"
                              { $$ = p_val_ge; }
                            ;

/* [55] */
NodeComp                    : "is"
                              { $$ = p_is; }
                            | "isnot"
                              { $$ = p_nis; }
                            ;

/* [56] */
OrderComp                   : "<<"
                              { $$ = p_ltlt; }
                            | ">>"
                              { $$ = p_gtgt; }
                            ;

/* [57] */
OrderByClause               : "order by" OrderSpecList
                              { ($$ = p_wire1 (p_orderby, @$, 
                                               $2))->sem.tru = false; 
                              }
                            | "stable order by" OrderSpecList
                              { ($$ = p_wire1 (p_orderby, @$, 
                                               $2))->sem.tru = true; 
                              }
                            ;

/* [58/59] */
OrderSpecList               : Expr OrderModifier 
                              { ($$ = p_wire2 (p_orderspecs, @$, 
                                               $1, 
                                               nil (@$)))->sem.mode = $2; 
                              }
                            | Expr OrderModifier ',' OrderSpecList
                              { ($$ = p_wire2 (p_orderspecs, @$, 
                                               $1, 
                                               $4))->sem.mode = $2; 
                              }
                            ;

/* [60] */
OrderModifier               : OptAscDesc_ OptEmpty_ OptCollation_
                              { ($$).dir = $1;
                                ($$).empty = $2;
                                ($$).coll = $3;
                              }
                            ;

OptAscDesc_                 : /* empty */
                              { $$ = p_asc; }
                            | "ascending"
                              { $$ = p_asc; }
                            | "descending"
                              { $$ = p_desc; }
                            ;

OptEmpty_                   : /* empty */
                              { $$ = p_least; }
                            | "empty greatest"
                              { $$ = p_greatest; }
                            | "empty least"
                              { $$ = p_least; }
                            ;

OptCollation_               : /* empty */
                              { $$ = 0; }
                            | "collation" StringLiteral
                              { $$ = $2; }
                            ;

/* [61] */
CaseClause                  : "case" SequenceType "return" Expr
                               { $$ = p_wire3 (p_case, @$, $2, nil (@$), $4); }
                            | "case" '$' VarName "as" SequenceType 
                              "return" Expr
                               { $$ = p_wire3 (p_case, @$, 
                                               $5, 
                                               (c = p_leaf (p_varref, 
                                                            loc_rng (@2, @3)),
                                                c->sem.qname = $3, 
                                                c),
                                               $7); 
                               }
                            ;

/* [62] */
PrimaryExpr                 : Literal
                              { $$ = $1; }
                            | FunctionCall
                              { $$ = $1; }
                            | '$' VarName
                              { ($$ = p_leaf (p_varref, @$))->sem.qname = $2; }
                            | ParenthesizedExpr
                              { $$ = $1; }
                            ;

/* [63] !W3C added all XPath axes */
ForwardAxis                 : "child::"
                              { $$ = p_child; }
                            | "descendant::"
                              { $$ = p_descendant; }
                            | "attribute::"
                              { $$ = p_attribute; }
                            | "self::"
                              { $$ = p_self; }
                            | "descendant-or-self::"
                              { $$ = p_descendant_or_self; }
                            | "following::"
                              { $$ = p_following; }
                            | "following-sibling::"
                              { $$ = p_following_sibling; }
                            ;

/* [64] !W3C added all XPath axes */
ReverseAxis                 : "parent::"
                              { $$ = p_parent; }
                            | "ancestor::"
                              { $$ = p_ancestor; }
                            | "ancestor-or-self::"
                              { $$ = p_ancestor_or_self; }
                            | "preceding::"
                              { $$ = p_preceding; }
                            | "preceding-sibling::"
                              { $$ = p_preceding_sibling; }
                            ;

/* [65] */
NodeTest                    : KindTest
                              { $$ = $1; }
                            | NameTest
                              { $$ = $1; }
                            ;

/* [66] */
NameTest                    : QName
                              { ($$ = p_leaf (p_namet, @$))->sem.qname = $1; }
                            | WildCard
                              { ($$ = p_leaf (p_namet, @$))->sem.qname = $1; }
                            ;

/* [67] */
WildCard                    : '*'
                              { $$ = PFstr_qname ("*:*"); }
                            | "NCName:*"
                              { $$ = $1; }
                            | "*:NCName"
                              { $$ = $1; }
                            ;

/* [68] */
KindTest                    : ProcessingInstructionTest
                              { $$ = $1; }
                            | CommentTest
                              { $$ = $1; }
                            | TextTest
                              { $$ = $1; }
                            | AnyKindTest
                              { $$ = $1; }
                            ;

/* [69] */
ProcessingInstructionTest   : "processing-instruction (" ')' 
                              { ($$ = p_wire1 (p_kindt, @$, nil (@$)))
                                  ->sem.kind = p_kind_pi; 
                              }
                            | "processing-instruction (" StringLiteral ')' 
                              { ($$ = p_wire1 (p_kindt, @$,
                                               (c = p_leaf (p_lit_str, @2),
                                                c->sem.str = $2,
                                                c)))->sem.kind = p_kind_pi; 
                              }
                            ;


/* [70] */
CommentTest                 : "comment (" ')'
                              { ($$ = p_wire1 (p_kindt, @$, nil (@$)))
                                  ->sem.kind = p_kind_comment; 
                              }
                            ;

/* [71] */
TextTest                    : "text (" ')'
                              { ($$ = p_wire1 (p_kindt, @$, nil (@$)))
                                  ->sem.kind = p_kind_text; 
                              }
                            ;

/* [72] */
AnyKindTest                 : "node (" ')'
                              { ($$ = p_wire1 (p_kindt, @$, nil (@$)))
                                  ->sem.kind = p_kind_node; 
                              }
                            ;

/* [73] */
ForwardStep                 : ForwardAxis NodeTest
                              { ($$ = p_wire1 (p_step, @$, 
                                               $2))->sem.axis = $1; 
                              }
                            | AbbreviatedForwardStep
                              { $$ = $1; }
                            ;

/* [74] */
ReverseStep                 : ReverseAxis NodeTest
                              { ($$ = p_wire1 (p_step, @$, 
                                               $2))->sem.axis = $1; 
                              }
                            | AbbreviatedReverseStep
                              { $$ = $1; }
                            ;

/* [75] */
AbbreviatedForwardStep      : '.'
                              { ($$ = p_wire1 (p_step,
                                             @$,
                                               (c = p_wire1 (p_kindt, @$, 
                                                             nil (@$)),
                                                c->sem.kind = p_kind_node,
                                                c)))->sem.axis = p_self; 
                              }
                            | '@' NameTest
                              { ($$ = p_wire1 (p_step,
                                               @$,
                                               $2))->sem.axis = p_attribute; 
                              }
                            | NodeTest
                              { ($$ = p_wire1 (p_step,
                                               @$,
                                               $1))->sem.axis = p_child; 
                              }
                            ;

/* [76] */
AbbreviatedReverseStep      : ".."
                              { ($$ = p_wire1 (p_step,
                                               @$,
                                               (c = p_wire1 (p_kindt, @$, 
                                                             nil (@$)),
                                                c->sem.kind = p_kind_node,
                                                c)))->sem.axis = p_parent; 
                              }
                            ;

/* [77] */
Predicates                  : /* empty */
                              { $$[ROOT] = $$[HOLE] = 0; }
                            | Predicates '[' Expr ']'
                              { $$[ROOT] = $$[HOLE] = 
                                  p_wire2 (p_pred, @$, nil (@3), $3); 
                                if ($1[ROOT]) {
                                  FIXUP (0, $$, $1[ROOT]);
                                  $$[HOLE] = $1[HOLE];
                                }
                              }
                            ;

/* [78] */
NumericLiteral              : IntegerLiteral
                              { ($$ = p_leaf (p_lit_int, @$))->sem.num = $1; }
                            | DecimalLiteral
                              { ($$ = p_leaf (p_lit_dec, @$))->sem.dec = $1; }
                            | DoubleLiteral
                              { ($$ = p_leaf (p_lit_dbl, @$))->sem.dbl = $1; }
                            ;

/* [79] */
Literal                     : NumericLiteral
                              { $$ = $1; }
                            | StringLiteral
                              { ($$ = p_leaf (p_lit_str, @$))->sem.str = $1; }
                            ;

/* [80] */
ParenthesizedExpr           : '(' OptExprSequence_ ')'
                              { $$ = $2; }
                            ;

/* [81] */
FunctionCall                : QName '(' OptFuncArgList_ ')'
                              { ($$ = p_wire1 (p_fun_ref, @$, 
                                               $3))->sem.qname = $1; 
                              }
                            ;

OptFuncArgList_             : /* empty */
                              { $$ = nil (@$); }
                            | FuncArgList_
                              { $$ = $1; }
                            ;

FuncArgList_                : Expr
                              { $$ = p_wire2 (p_args, @$, $1, nil (@1)); }
                            | FuncArgList_ ',' Expr
                              { /* NB: we build the argument list in
                                 * reverse order (this will be reversed
                                 * again by the core mapping)
                                 */
                                $$ = p_wire2 (p_args, @$, $3, $1); 
                              }
                            ;

/* [82] */
Param                       : '$' VarName OptTypeDeclaration_
                              { $$ = p_wire2 (p_param, @$,
                                              $3,
                                              (c = p_leaf (p_varref,
                                                           loc_rng (@1, @2)),
                                               c->sem.qname = $2,
                                               c)); 
                              }
                            ;

/* [83] */
SchemaContext               : "context" SchemaGlobalContext SchemaContextSteps_
                              { $$ = p_wire2 (p_schm_path, @$, $2, $3); }
                            ;

SchemaContextSteps_         : /* empty */
                              { $$ = nil (@$); }
                            | SchemaContextSteps_ '/' SchemaContextStep
                              { $$ = p_wire2 (p_schm_path, @$, $3, $1); }
                            ;

/* [84] */
SchemaGlobalContext         : QName
                              { ($$ = p_leaf (p_glob_schm, 
                                              @$))->sem.qname = $1; 
                              }
                            | "type QName"
                              { ($$ = p_leaf (p_glob_schm_ty, 
                                              @$))->sem.qname = $1; 
                              }
                            ;

/* [85] */
SchemaContextStep           : QName
                              { ($$ = p_leaf (p_schm_step, 
                                              @$))->sem.qname = $1; 
                              }
                            ;

/* [86] */
TypeDeclaration             : "as" SequenceType
                              { $$ = $2; }
                            ;

/* [87] */
SingleType                  : AtomicType
                              { ($$ = p_wire1 (p_seq_ty, @$, 
                                               (c = p_wire1 (p_atom_ty, @$, 
                                                             nil (@$)),
                                                c->sem.qname = $1,
                                                c)))->sem.oci = p_one; 
                              }
                            | AtomicType '?'
                              { ($$ = p_wire1 (p_seq_ty, @$, 
                                               (c = p_wire1 (p_atom_ty, @1, 
                                                             nil (@1)),
                                                c->sem.qname = $1,
                                                c)))->sem.oci = p_zero_or_one; 
                              }
                            ;

/* [88] */
SequenceType                : ItemType OccurrenceIndicator
                              { ($$ = p_wire1 (p_seq_ty, @$, 
                                               $1))->sem.oci = $2; 
                              }
                            | "empty"
                              { ($$ = p_wire1 (p_seq_ty, @$,
                                               p_leaf (p_empty_ty, @$)))
                                  ->sem.oci = p_one; 
                              }
                            ;

/* [89] */
ItemType                    : ElemAtt_
                              { $$ = $1; }
                            | "node"
                              { ($$ = p_wire1 (p_node_ty, @$, nil (@$)))
                                  ->sem.kind = p_kind_node; 
                              }
                            | "processing-instruction"
                              { ($$ = p_wire1 (p_node_ty, @$, nil (@$)))
                                  ->sem.kind = p_kind_pi; 
                              }
                            | "comment"
                              { ($$ = p_wire1 (p_node_ty, @$, nil (@$)))
                                  ->sem.kind = p_kind_comment; 
                              }
                            | "text"
                              { ($$ = p_wire1 (p_node_ty, @$, nil (@$)))
                                  ->sem.kind = p_kind_text; 
                              }
                            | "document"
                              { ($$ = p_wire1 (p_node_ty, @$, nil (@$)))
                                  ->sem.kind = p_kind_doc; 
                              }
                            | "item"
                              { $$ = p_wire1 (p_item_ty, @$, nil (@$)); }
                            | AtomicType
                              { ($$ = p_wire1 (p_atom_ty, @$, nil (@$)))
                                  ->sem.qname = $1; 
                              }
                            | "atomic value"
                              { $$ = p_wire1 (p_atomval_ty, @$, nil (@$)); }
                            ;

ElemAtt_                    : "element" OptElemOrAttrType_
                              { ($$ = p_wire1 (p_node_ty, @$, $2))
                                  ->sem.kind = p_kind_elem; 
                              }
                            | "attribute" OptElemOrAttrType_
                              { ($$ = p_wire1 (p_node_ty, @$, $2))
                                  ->sem.kind = p_kind_attr; 
                              }
                            ;

OptElemOrAttrType_          : /* empty */
                              { $$ = nil (@$); }
                            | ElemOrAttrType
                              { $$ = $1; }
                            ;

/* [90] */
ElemOrAttrType              : QName SchemaType
                              { $$ = p_wire2 (p_req_ty,
                                              @$,
                                              (c = p_leaf (p_req_name, @1),
                                               c->sem.qname = $1,
                                               c),
                                              $2);
                              }
                            | QName OptSchemaContext_
                              { $$ = p_wire2 (p_req_ty,
                                              @$,
                                              (c = p_leaf (p_req_name, @1),
                                               c->sem.qname = $1,
                                               c),
                                              $2); 
                              }
                            | SchemaType
                              { $$ = p_wire2 (p_req_ty, @$, nil (@1), $1); }
                            ;

/* [91] */
SchemaType                  : "of type" QName
                              { ($$ = p_leaf (p_named_ty, 
                                              @$))->sem.qname = $2; 
                              }
                            ;

/* [92] */
AtomicType                  : QName
                              { $$ = $1; }
                            ;

/* [93] */
OccurrenceIndicator         : /* empty */
                              { $$ = p_one; }
                            | '*'
                              { $$ = p_zero_or_more; }
                            | '+'
                              { $$ = p_one_or_more; }
                            | '?'
                              { $$ = p_zero_or_one; }
                            ;

/* [94] */
ElementConstructor          : '<' QName AttributeList "/>"
                              { if ($3[ROOT])
                                  $3[HOLE]->child[1] = p_leaf (p_empty_seq, @3);
                                else
                                  $3[ROOT] = p_leaf (p_empty_seq, @3);

                                $$ = p_wire2 (p_elem,
                                              @$,
                                              (c = p_leaf (p_tag, @2),
                                               c->sem.qname = $2,
                                               c),
                                              $3[ROOT]);
                              }
                            | '<' QName AttributeList '>'
                              ElementContents_ 
                              "</" QName OptS '>'
                              { /* XML well-formedness check:
                                 * start and end tag must match
                                 */
                                if (PFqname_eq ($2, $7)) {
                                  PFinfo_loc (OOPS_TAGMISMATCH, @$,
                                              "<%s> and </%s>",
                                              PFqname_str ($2),
                                              PFqname_str ($7));
                                  YYERROR;
                                }                                      

                                /* merge attribute list and element content */
                                 if ($3[ROOT]) 
                                   FIXUP (1, $3, $5);
                                 else
                                   $3[ROOT] = $5;

                                 $$ = p_wire2 (p_elem,
                                               @$,
                                               (c = p_leaf (p_tag, @2),
                                                c->sem.qname = $2,
                                                c),
                                               $3[ROOT]);
                              }                          
                            ;

OptS                        : /* empty */
                            | S
                            ;

ElementContents_            : /* empty */
                              { $$ = p_leaf (p_empty_seq, @$); }
                            | ElementContent ElementContents_
                              { $$ = p_wire2 (p_contseq, @$, $1, $2); }
                            ;

/* [95] */
ComputedDocumentConstructor : "document {" ExprSequence '}'
                              { $$ = p_wire1 (p_doc, @$, $2); }
                            ;

/* [96] */
ComputedElementConstructor  : "element QName {" OptExprSequence_ '}'
                              { $$ = p_wire2 (p_elem, @$,
                                              (c = p_leaf (p_tag, @1),
                                               c->sem.qname = $1,
                                               c),
                                              p_wire2 (p_contseq, @2, $2,
                                                       p_leaf (p_empty_seq, @2))
                                              ); 
                              }
                            | "element {" Expr '}' '{' OptExprSequence_ '}'
                              { $$ = p_wire2 (p_elem, @$, $2, 
                                              p_wire2 (p_contseq, @5, $5,
                                                       p_leaf (p_empty_seq,
                                                               @2))); }
                            ;

/* [97] */
ComputedAttributeConstructor: "attribute QName {" OptExprSequence_ '}'
                              { $$ = p_wire2 (p_attr, @$,
                                              (c = p_leaf (p_tag, @1),
                                               c->sem.qname = $1,
                                               c),
                                              p_wire2 (p_contseq, @2, $2,
                                                       p_leaf (p_empty_seq, @2))
                                              ); 
                              }
                            | "attribute {" Expr '}' '{' OptExprSequence_ '}'
                              { $$ = p_wire2 (p_attr, @$, $2,
                                              p_wire2 (p_contseq, @5, $5,
                                                       p_leaf (p_empty_seq,
                                                               @5))); }
                            ;

/* [98] */
ComputedTextConstructor     : "text {" OptExprSequence_ '}'
                              { $$ = p_wire1 (p_text, @$, $2); }
                            ;

/* [99] */
CdataSection                : "<![CDATA[" Characters_ "]]>"
                              { $$ = p_wire1 (p_text, @$,
                                              p_wire2 (p_exprseq, @2,
                                                       $2,
                                                       p_leaf (p_empty_seq, @2)
                                                      )); }
                            ;

/* make string from characters collected in dynamic array */
Characters_                 : Chars_
                              { /* add trailing '\0' to string */
				*((char *) PFarray_add ($1)) = '\0';

                                $$ = p_leaf (p_lit_str, @1);
                                $$->sem.str = (char *) PFarray_at ($1, 0);
                              }
                            ;
                              

/* collect single characters in a dynamic array */
Chars_                      : /* empty */
                              { /* initialize new dynamic array */
                                $$ = PFarray (sizeof (char)); }
                            | Chars_ Char
                              { /* append one charater to array */
				*((char *) PFarray_add ($1)) = $2;
				$$ = $1;
			      }
                            ;

/* [100] */
XmlProcessingInstruction    : "<?" PITarget Characters_ "?>"
                              { ($$ = p_wire1 (p_pi, @$, $3))->sem.str = $2; }
                            ;

/* [101] */
XmlComment                  : "<!--" Characters_ "-->"
                              { $$ = p_wire1 (p_comment, @$, $2); }
                            ;

/* [102] */
ElementContent              : ElementContentTexts_
                              { /* add trailing '\0' to string */
				*((char *) PFarray_add ($1)) = '\0';

				/* xmlspace handling */
                                if ((! xmlspace_preserve) && 
                                    (is_whitespace (PFarray_at ($1, 0))))
				  $$ = p_leaf (p_empty_seq, @1);
                                else
                                  $$ = p_wire1 (p_text, @1,
                                                p_wire2 (p_exprseq, @1,
                                                         (c = p_leaf 
                                                                (p_lit_str,
                                                                 @1),
                                                          c->sem.str = 
							    PFarray_at ($1, 0),
                                                          c),
                                                         p_leaf (p_empty_seq, 
                                                                 @1)
                                                        )); 
			      }
                            | ElementConstructor
                            | EnclosedExpr
                            | XmlComment
                            | XmlProcessingInstruction
                            | CdataSection
                            ;

ElementContentTexts_        : ElementContentText_
                              { /*
                                 * initialize new dynamic array and insert
                                 * one character
                                 */
				$$ = PFarray (sizeof (char));
				*((char *) PFarray_add ($$)) = $1;
			      }
                            | ElementContentTexts_ ElementContentText_
                              { /* append one charater to array */
				*((char *) PFarray_add ($1)) = $2;
				$$ = $1;
			      }
                            ;

ElementContentText_         : Char                   { $$ = $1; }
                            | "{{"                   { $$ = '{'; }
                            | "}}"                   { $$ = '}'; }
                            | CharRef                { $$ = $1; }
                            | PredefinedEntityRef    { $$ = $1; }
                            ;

/* [103] */
AttributeList               : /* empty */
                              { $$[ROOT] = $$[HOLE] = 0; }
                            | S
                              { $$[ROOT] = $$[HOLE] = 0; }
			    | S QName OptS '=' OptS AttributeValue
                              AttributeList
                              { $$[ROOT] = $$[HOLE] = 
                                  p_wire2 (p_contseq,
                                           @$,
                                           p_wire2 (p_attr,
                                                    loc_rng (@2, @6),
                                                    (c = p_leaf (p_tag, @2),
                                                     c->sem.qname = $2,
                                                     c),
                                                    $6),
                                           $7[ROOT]);
                                if ($7[ROOT])
                                  $$[HOLE] = $7[HOLE];
                              }
                            ;

/* [104] */
AttributeValue              : '"' QuotAttributeValueContents_ '"'
                              { $$ = $2; }
                            | '\'' AposAttributeValueContents_ '\''
                              { $$ = $2; }
                            ;



QuotAttributeValueContents_ : /* empty */
                              { $$ = p_leaf (p_empty_seq, @$); }
                            | QuotAttributeContent QuotAttributeValueContents_
                              { $$ = p_wire2 (p_contseq, @$, $1, $2); }
                            ;

QuotAttributeContent        : QuotAttributeContentTexts_
                              { /* add trailing '\0' to string */
				*((char *) PFarray_add ($1)) = '\0';

                                $$ = p_wire2 (p_contseq, @1,
                                              (c = p_leaf (p_lit_str, @1),
                                               c->sem.str = PFarray_at ($1, 0),
                                               c),
                                              p_leaf (p_empty_seq, @1)
                                             ); }
                            | EnclosedExpr
                              { $$ = $1; }
                            ;

QuotAttributeContentTexts_  : QuotAttributeContentText_
                              { /*
                                 * initialize new dynamic array and insert
                                 * one character
                                 */
				$$ = PFarray (sizeof (char));
				*((char *) PFarray_add ($$)) = $1;
			      }
                            | QuotAttributeContentTexts_
                              QuotAttributeContentText_
                              { /* append one charater to array */
				*((char *) PFarray_add ($1)) = $2;
			      }
                            ;

QuotAttributeContentText_   : Char                   { $$ = $1; }
                            | "{{"                   { $$ = '{'; }
                            | "}}"                   { $$ = '}'; }
                            | CharRef                { $$ = $1; }
                            | PredefinedEntityRef    { $$ = $1; }
                            | EscapeQuot             { $$ = '"'; }
                            ;

AposAttributeValueContents_ : /* empty */
                              { $$ = p_leaf (p_empty_seq, @$); }
                            | AposAttributeContent AposAttributeValueContents_
                              { $$ = p_wire2 (p_contseq, @$, $1, $2); }
                            ;

AposAttributeContent        : AposAttributeContentTexts_
                              { /* add trailing '\0' to string */
				*((char *) PFarray_add ($1)) = '\0';

                                $$ = p_wire2 (p_contseq, @1,
                                              (c = p_leaf (p_lit_str, @1),
                                               c->sem.str = PFarray_at ($1, 0),
                                               c),
                                              p_leaf (p_empty_seq, @1)
                                             ); }
                            | EnclosedExpr
                              { $$ = $1; }
                            ;

AposAttributeContentTexts_  : AposAttributeContentText_
                              { /*
                                 * initialize new dynamic array and insert
                                 * one character
                                 */
				$$ = PFarray (sizeof (char));
				*((char *) PFarray_add ($$)) = $1;
			      }
                            | AposAttributeContentTexts_
                              AposAttributeContentText_
                              { /* append one charater to array */
				*((char *) PFarray_add ($1)) = $2;
				$$ = $1;
			      }
                            ;

AposAttributeContentText_   : Char                   { $$ = $1; }
                            | "{{"                   { $$ = '{'; }
                            | "}}"                   { $$ = '}'; }
                            | CharRef                { $$ = $1; }
                            | PredefinedEntityRef    { $$ = $1; }
                            | EscapeApos             { $$ = '\''; }
                            ;

/* [106] */
EnclosedExpr                : '{' ExprSequence '}'
                              { $$ = $2; }
                            ;

/* [107] */
XMLSpaceDecl                : "declare xmlspace" '=' "preserve"
                              { ($$ = p_leaf (p_xmls_decl, 
                                              @$))->sem.tru = true; 
			        xmlspace_preserve = true;
                              }
                            | "declare xmlspace" '=' "strip"
                              { ($$ = p_leaf (p_xmls_decl, 
                                              @$))->sem.tru = false; 
			        xmlspace_preserve = false;
                              }
                            ;

/* [108] */
DefaultCollationDecl        : "default collation =" URLLiteral
                              { $$ = p_wire1 (p_coll_decl, 
                                              @$,
                                              (c = p_leaf (p_lit_str, @2),
                                               c->sem.str = $2,
                                               c));
                              }
                            ;

/* [109] */
NamespaceDecl               : "declare namespace" 
                              NCNameForPrefix '=' URLLiteral
                              { ($$ = p_wire1 (p_ns_decl, 
                                               @$,
                                               (c = p_leaf (p_lit_str, @4),
                                                c->sem.str = $4,
                                                c)))->sem.str = $2;
                              }
                            ;

/* [110] */
SubNamespaceDecl            : "namespace" NCNameForPrefix '=' URLLiteral
                              { ($$ = p_wire1 (p_ns_decl, 
                                               @$,
                                               (c = p_leaf (p_lit_str, @4),
                                                c->sem.str = $4,
                                                c)))->sem.str = $2;
                              }
                            ;

/* [111] */
DefaultNamespaceDecl        : "default element" "namespace" '=' URLLiteral
                              { $$ = p_wire1 (p_ens_decl, 
                                              @$,
                                              (c = p_leaf (p_lit_str, @4),
                                               c->sem.str = $4,
                                               c));
                              }
                            | "default function" "namespace" '=' URLLiteral
                              { $$ = p_wire1 (p_fns_decl, 
                                              @$,
                                              (c = p_leaf (p_lit_str, @4),
                                               c->sem.str = $4,
                                               c));
                              }
                            ;

/* [112] */
FunctionDefn                : "define function" QName '('
                              OptParamList_ OptAs_ EnclosedExpr
                              { ($$ = p_wire3 (p_fun_decl,  @$,
                                               $4,
                                               $5,
                                               $6))->sem.qname = $2;
                              }
                            ;

OptParamList_               : /* empty */
                              { $$ = nil (@$); }
                            | ParamList
                              { $$ = $1; }
                            ;
                            
OptAs_                      : ')'
                              { $$ = nil (@$); }
                            | ") as" SequenceType
                              { $$ = $2; }
                            ;

/* [113] */
ParamList                   : Param
                              { $$ = p_wire2 (p_params, @$, $1, nil (@1)); }
                            | Param ',' ParamList
                              { $$ = p_wire2 (p_params, @$, $1, $3); }
                            ;

/* [114] */
SchemaImport                : "import schema" SchemaSrc_ OptSchemaLoc_
                              { /* XQuery allows to merge a schema import
                                 * and an associated namespace declaration:
                                 *
                                 * import schema namespace ns = "ns" [at "url"]
                                 *
                                 * which is equivalent to
                                 *
                                 * import schema "ns" [at "url"]
                                 * namespace ns = "ns" 
                                 *
                                 * We thus return the schema import in $$[ROOT]
                                 * and the namespace declaration in $$[HOLE]
                                 * ($1[ROOT] == 0 if no namespace decl given)
                                 */
                                $$[ROOT] = p_wire2 (p_schm_imp, @$,
                                                    $2[HOLE], $3);
                                $$[HOLE] = $2[ROOT];
                              }
                            ;

SchemaSrc_                  : StringLiteral
                              { $$[ROOT] = 0; 
                                ($$[HOLE] = p_leaf (p_lit_str,
                                                    @$))->sem.str = $1; 
                              }
                            | SubNamespaceDecl
                              { $$[ROOT] = $1;
                                /* access URLLiteral */
                                $$[HOLE] = $1->child[0];
                              }
                            | DefaultNamespaceDecl
                              { $$[ROOT] = $1;
                                /* access URLLiteral */
                                $$[HOLE] = $1->child[0]; 
                              }
                            ;

OptSchemaLoc_               : /* empty */
                              { $$ = nil (@$); }
                            | "at StringLiteral"
                              { ($$ = p_leaf (p_lit_str, @$))->sem.str = $1; }
                            ;


%%


/**
 * Check if the input string consists of whitespace only.
 * If this is the case and @c xmlspace_preserve is set to false, the 
 * abstract syntax tree must be altered, i.e., we do not
 * create a text node but an empty sequence node.
 *
 * @param s string to test for non-whitespace characters
 * @return true if @c s consists of whitespace only
 */
static bool 
is_whitespace (char *s)
{
    while (*s)
        if (! isspace (*s))
            return false;
        else
            s++;

    return true;
}

/**                            
 * Recursively flatten a location path @a p
 * (call this function with @a r initially @c NULL).
 *
 * @param p (possibly nested) location path
 * @param r ``Hole'' that will be filled during recursive calls.
 *          Call with @a r = @c NULL from outside.
 * @return flat location path
 */
static PFpnode_t *
flatten_locpath (PFpnode_t *p, PFpnode_t *r)
{
    if (p->kind == p_locpath) {
        if (r) 
            switch (p->child[1]->kind) {
                case p_dot:
                    p->child[1] = r;
                    p->loc = max_loc(p->loc, r->loc);
                    break;
                case p_locpath:
                    p->child[1] = flatten_locpath (p->child[1], r);
                    break;
                default:
                    p->child[1] = p_wire2 (p_locpath, 
                            max_loc (p->child[1]->loc, r->loc), 
                            p->child[1], r);
            }
        if (p->child[0]->kind == p_locpath)
            return flatten_locpath (p->child[0], p->child[1]);
    }
    return p;
}

/**
 * Invoked by bison whenever a parsing error occurs.
 */
void yyerror (const char *s)
{
    if (yytext && *yytext)
        PFlog ("%s on line %d (next token is `%s')",
                s, yylineno,
                yytext);
    else
        PFlog ("%s on line %d",
                s, yylineno);
}

extern FILE* yyin; /* standard input of scanner, defined by flex */

/**
 * Parse an XQuery coming in on stdin (or whatever stdin might have
 * been dup'ed to)
 */
void
PFparse (FILE* pfin, PFpnode_t **r)
{
    yyin = pfin;
#if YYDEBUG
    yydebug = 1;
#endif

    /* initialisation of yylloc */
    yylloc.first_row = yylloc.last_row = 1;
    yylloc.first_col = yylloc.last_col = 0;

    if (yyparse ())
        PFoops (OOPS_PARSE, "XQuery parsing failed");

    *r = root;
}


/* vim:set shiftwidth=4 expandtab: */

/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * XQuery (W3C WD October 29, 2004) grammar description
 * and parse tree construction.  Feed this into `bison'.
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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2005 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

%{
#include "pathfinder.h"

#include "parser.h"

/* PFarray_t */
#include "array.h"
#include "oops.h"

/* isspace */
#include <ctype.h>
/* strcmp */
#include <string.h>

/** root node of the parse tree */
static PFpnode_t *root;

/* temporay node memory */
static PFpnode_t *c, *c1;

/* avoid `implicit declaration of yylex' warning */
extern int pflex (void);

/* bison error callback */
void pferror (const char *s);

/* construct a nil node */
#define nil(loc) leaf (p_nil, (loc))

/* bison: generate verbose parsing error messages */
#define YYERROR_VERBOSE 

#define leaf(t,loc)       p_leaf ((t), (loc))
#define wire1(t,loc,a)    p_wire1 ((t), (loc), (a))
#define wire2(t,loc,a,b)  p_wire2 ((t), (loc), (a), (b))
/*
#define leaf(t,loc)       PFabssyn_leaf ((t), (loc))
#define wire1(t,loc,a)    PFabssyn_wire1 ((t), (loc), (a))
#define wire2(t,loc,a,b)  PFabssyn_wire2 ((t), (loc), (a), (b))
*/

/* fix-up the hole of an abstract syntax tree t and 
 * replace its c-th leaf by e:
 *
 *               t.root
 *                 / \
 *      t.hole --> -e--
 */
#define FIXUP(c,t,e) ((t).hole)->child[c] = (e)


/* scanner information to provide better error messages */
extern char *pftext;
extern int pflineno;

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

/* recursively flatten a location path @a p */
static PFpnode_t *flatten_locpath (PFpnode_t *p, PFpnode_t *r);

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

/* And finally: enable automatic location tracking */
%locations

/* semantic actions compute semantic values 
 * of the following types
 * (cf. field sem of PFpnode_t)
 */
%union {
    int             num;
    dec             dec;
    double          dbl;
    char           *str;
    char            chr;
    PFqname_t       qname;
    PFpnode_t      *pnode;
    struct phole_t  phole;
    PFptype_t       ptype;
    PFpaxis_t       axis;
    PFsort_t        mode;
    PFpoci_t        oci;
    PFarray_t      *buf;
}

%token ancestor_colon_colon            "ancestor::"
%token ancestor_or_self_colon_colon    "ancestor-or-self::"
%token and                             "and"
%token apos                            "'"
%token as                              "as"
%token ascending                       "ascending"
%token at_dollar                       "at $"
%token atsign                          "@"
%token attribute_colon_colon           "attribute::"
%token attribute_lbrace                "attribute {"
%token attribute_lparen                "attribute ("
%token case_                           "case"
%token cast_as                         "cast as"
%token castable_as                     "castable as"
%token cdata_end                       "]]>"
%token cdata_start                     "<![CDATA["
%token child_colon_colon               "child::"
%token collation                       "collation"
%token colon_equals                    ":="
%token comma                           ","
%token comment_lbrace                  "comment {"
%token comment_lparen                  "comment ("
%token declare_base_uri                "declare base-uri"
%token declare_construction_preserve   "declare construction preserve"
%token declare_construction_strip      "declare construction strip"
%token declare_default_collation       "declare default collation"
%token declare_default_element         "declare default element"
%token declare_default_function        "declare default function"
%token declare_default_order           "declare default order"
%token declare_function                "declare function"
%token declare_inherit_namespaces_no   "declare inherit-namespaces no"
%token declare_inherit_namespaces_yes  "declare inherit-namespaces yes"
%token declare_namespace               "declare namespace"
%token declare_ordering_ordered        "declare ordering ordered"
%token declare_ordering_unordered      "declare ordering unordered"
%token declare_variable_dollar         "declare variable $"
%token declare_xmlspace_preserve       "declare xmlspace preserve"
%token declare_xmlspace_strip          "declare xmlspace strip"
%token default_                        "default"
%token default_element                 "default element"
%token descendant_colon_colon          "descendant::"
%token descendant_or_self_colon_colon  "descendant-or-self::"
%token descending                      "descending"
%token div_                            "div"
%token document_lbrace                 "document {"
%token document_node_lparen            "document-node ("
%token dollar                          "$"
%token dot                             "."
%token dot_dot                         ".."
%token element_lbrace                  "element {"
%token element_lparen                  "element ("
%token else_                           "else"
%token empty_greatest                  "empty greatest"
%token empty_least                     "empty least"
%token empty_lrparens                  "empty ()"
%token eq                              "eq"
%token equals                          "="
%token escape_apos                     "''"
%token escape_quot                     "\"\""
%token every_dollar                    "every $"
%token except                          "except"
%token excl_equals                     "!="
%token external                        "external"
%token following_colon_colon           "following::"
%token following_sibling_colon_colon   "following-sibling::"
%token for_dollar                      "for $"
%token ge                              "ge"
%token greater_than                    ">"
%token greater_than_equal              ">="
%token gt                              "gt"
%token gt_gt                           ">>"
%token idiv                            "idiv"
%token if_lparen                       "if ("
%token import_module                   "import module"
%token import_schema                   "import schema"
%token in                              "in"
%token instance_of                     "instance of"
%token intersect                       "intersect"
%token is                              "is"
%token item_lrparens                   "item ()"
%token lbrace                          "{"
%token lbrace_lbrace                   "{{"
%token lbracket                        "["
%token le                              "le"
%token less_than                       "<"
%token less_than_equal                 "<="
%token let_dollar                      "let $"
%token lparen                          "("
%token lt                              "lt"
%token lt_lt                           "<<"
%token lt_question_mark                "<?"
%token lt_slash                        "</"
%token minus                           "-"
%token mod                             "mod"
%token module_namespace                "module namespace"
%token namespace                       "namespace"
%token ne                              "ne"
%token node_lrparens                   "node ()"
%token or                              "or"
%token order_by                        "order by"
%token ordered_lbrace                  "ordered {"
%token parent_colon_colon              "parent::"
%token pi_lbrace                       "processing-instruction {"
%token pi_lparen                       "processing-instruction ("
%token pipe_                           "|"
%token plus                            "+"
%token preceding_colon_colon           "preceding::"
%token preceding_sibling_colon_colon   "preceding-sibling::"
%token question_mark                   "?"
%token question_mark_gt                "?>"
%token quot_                           "\""
%token rbrace                          "}"
%token rbrace_rbrace                   "}}"
%token rbracket                        "]"
%token return_                         "return"
%token rparen                          ")"
%token rparen_as                       ") as"
%token satisfies                       "satisfies"
%token schema_attribute_lparen         "schema-attribute ("
%token schema_element_lparen           "schema-element ("
%token self_colon_colon                "self::"
%token semicolon                       ";"
%token slash                           "/"
%token slash_gt                        "/>"
%token slash_slash                     "//"
%token some_dollar                     "some $"
%token stable_order_by                 "stable order by"
%token star                            "*"
%token text_lbrace                     "text {"
%token text_lparen                     "text ("
%token then                            "then"
%token to                              "to"
%token treat_as                        "treat as"
%token typeswitch_lparen               "typeswitch ("
%token union_                          "union"
%token unordered_lbrace                "unordered {"
%token validate_lax_lbrace             "validate lax {"
%token validate_lbrace                 "validate {"
%token validate_strict_lbrace          "validate strict {"
%token where                           "where"
%token xml_comment_end                 "-->"
%token xml_comment_start               "<!--"
%token xquery_version                  "xquery version"

%token AttrContentChar
%token Attribute_QName_LBrace
%token PFChar
%token CharRef
%token DecimalLiteral
%token DoubleLiteral
%token ElementContentChar
%token Element_QName_LBrace
%token IntegerLiteral
%token NCName
%token NCName_Colon_Star
%token PITarget
%token PI_NCName_LBrace
%token PredefinedEntityRef
%token QName
%token QName_LParen
%token S
%token Star_Colon_NCName
%token StringLiteral
%token at_StringLiteral
%token encoding_StringLiteral

/*
 * The lexer sends an error token if it reads something that shouldn't
 * appear. (We replace flex' default action with this.)
 */
%token invalid_character

%type <str>    NCName
               OptCollationStringLiteral_
               OptEncoding_
               PITarget
               PI_NCName_LBrace
               StringLiteral
               at_StringLiteral
               encoding_StringLiteral

%type <qname>  AttributeDeclaration
               AttributeName
               Attribute_QName_LBrace
               ElementDeclaration
               ElementName
               Element_QName_LBrace
               NCName_Colon_Star
               QName
               QName_LParen
               Star_Colon_NCName
               TypeName

%type <num>    IntegerLiteral
               OccurrenceIndicator
               OptAscendingDescending_
               OptEmptyGreatestLeast_
               SomeEvery_

%type <dec>    DecimalLiteral

%type <dbl>    DoubleLiteral

%type <chr>    "{{"
               "}}"
               AttrContentChar
               AttributeValueContText_
               PFChar
               CharRef
               ElementContentChar
               ElementContentText_
               EscapeApos
               EscapeQuot
               PredefinedEntityRef

%type <mode>   OrderModifier

%type <axis>   AttributeAxis_
               ReverseAxis
               ForwardAxis

%type <buf>    AttributeValueContTexts_
               Chars_
               ElementContentTexts_

%type <ptype>  DivOp_
               GeneralComp
               NodeComp
               ValueComp

%type <pnode>  AbbrevAttribStep_ AbbrevForwardStep AbbrevReverseStep
               AdditiveExpr AndExpr AnyKindTest AtomicType AttribNameOrWildcard
               AttribNodeTest AttribStep_ AttributeTest AttributeValueCont_
               AttributeValueConts_ AxisStep BaseURIDecl CDataSection
               CDataSectionContents CaseClause CastExpr CastableExpr Characters_
               CommentTest CompAttrConstructor CompCommentConstructor
               CompDocConstructor CompElemConstructor CompPIConstructor
               CompTextConstructor ComparisonExpr ComputedConstructor
               ConstructionDecl Constructor ContentExpr ContextItemExpr
               DefaultCollationDecl DefaultNamespaceDecl DirAttribute_
               DirAttributeValue DirCommentConstructor DirCommentContents
               DirElemConstructor DirElemContent DirElementContents_
               DirPIConstructor DirPIContents DirectConstructor DocumentTest
               ElementNameOrWildcard ElementTest EmptyOrderingDecl EnclosedExpr
               Expr ExprSingle FLWORExpr FilterExpr ForClause ForLetClause_
               ForwardStep FuncArgList_ FunctionCall FunctionDecl IfExpr
               InheritNamespacesDecl InstanceofExpr IntersectExceptExpr
               ItemType KindTest LetBindings_ LetClause LibraryModule Literal
               MainModule Module ModuleDecl MultiplicativeExpr NameTest
               NamespaceDecl NodeTest NumericLiteral OptAsSequenceType_
               OptAtStringLiterals_ OptContentExpr_ OptDollarVarNameAs_
               OptDollarVarName_ OptFuncArgList_ OptOrderByClause_
               OptParamList_ OptPositionalVar_ OptParamTypeDeclaration_
               OptTypeDeclaration_ OptWhereClause_ OrExpr OrderByClause
               OrderSpecList OrderedExpr OrderingModeDecl PITest Param
               ParamList ParenthesizedExpr PathExpr PositionalVar Predicate
               PrimaryExpr Prolog QuantifiedExpr QueryBody RangeExpr
               RelativePathExpr ReverseStep SchemaAttributeTest
               SchemaElementTest SequenceType Setter SingleType StepExpr
               StringLiterals_ TextTest TreatExpr TypeDeclaration
               TypeswitchExpr UnaryExpr UnionExpr UnorderedExpr
               ValidateExpr ValueExpr VarBindings_ VarDecl VarName
               VarPosBindings_ VarRef WhereClause Wildcard XMLSpaceDecl



%type <phole>  CaseClauses_ DirAttributeList ForLetClauses_ Import
               ImportAndNSDecls_ ModuleImport ModuleNS_ PredicateList
               SchemaImport SchemaSrc_ Setters_ VarFunDecls_

/*
 * We expect 16 shift/reduce conflicts:
 *
 *  -- There are 2 conflicts in path expressions: interpret a `*' following
 *     a path expression as a wildcard name test, instead as the binary
 *     multiplication operator:
 *
 *         / * foo       parsed as     (/*) foo
 *
 *     (Use parentheses if you mean (/) * foo, as also described in Section
 *     3.2 of the Working Draft.)
 *
 *  -- Similarly, there are two conflicts with a `<' following a `/' or `//'.
 *     It may either be interpreted as the binary comparison operator, or
 *     as the start of a direct element constructor. The latter is actually
 *     valid XQuery, as any expression may follow the `/' (see Section 3.2
 *     of the WD: a path expression E1/E2 is evaluated by first evaluating
 *     E1 to serve as the inner focus for the evaluation of E2; E2 may in
 *     fact return anything). There's nothing mentioned in the WD; bison
 *     chooses the latter (in alignment with the above two conflicts), and
 *     we keep it as that.
 *
 *  -- There are 12 conflicts in direct constructors, 7 in the attribute
 *     constructors, 5 in the element constructor. The single character
 *     tokens might either be shifted and collected into
 *     AttributeValueContTexts_ or ElementContentTexts_, or reduced
 *     immediately (ending up in many text chunks that only contain one
 *     character). The former is what we want; bison's default is to shift,
 *     so we're all set.
 */

%expect 16

%%

/* [1] */
Module                    : VersionDecl MainModule
                            { /* assign parse tree root
                               * version declaration is in global variable */
                              root = $$ = $2;
                            }
                          | VersionDecl LibraryModule
                            { /* assign parse tree root
                               * version declaration is in global variable */
                              root = $$ = $2;
                            }
                          ;

/* [2] */
VersionDecl               : /* empty */
                            { PFquery.version = "1.0";
                              PFquery.encoding = NULL; }
                          | "xquery version" StringLiteral OptEncoding_
                            Separator
                            { if (strcmp ($2, "1.0")) {
                                  PFinfo_loc (OOPS_PARSE, @$,
                                              "we only support XQuery version "
                                              "'1.0' (well, not even really "
                                              "that)");
                                  YYERROR;
                              }
                              PFquery.version = $2; PFquery.encoding = $3; }
                          ;

OptEncoding_              : /* empty */
                            { $$ = NULL; }
                          | encoding_StringLiteral
                            { $$ = $1; }
                          ;


/* [3] */
MainModule                : Prolog QueryBody
                            { $$ = wire2 (p_main_mod, @$, $1, $2); }
                          ;

/* [4] */
LibraryModule             : ModuleDecl Prolog
                            { $$ = wire2 (p_lib_mod, @$, $1, $2); }
                          ;

/* [5] */
ModuleDecl                : "module namespace" NCName
                              "=" StringLiteral Separator
                            {
                              PFinfo_loc (OOPS_NOTSUPPORTED, @$,
                                          "Pathfinder does currently not "
                                          "support XQuery modules.");
                              YYERROR;

                              ($$ = wire1 (p_mod_ns,
                                           @$,
                                           (c = leaf (p_lit_str, @4),
                                            c->sem.str = $4,
                                            c)))->sem.str = $2;
                            }
                          ;

/* [6] */
Prolog                    : Setters_ ImportAndNSDecls_ VarFunDecls_
                            { $$ = $1.root;
                              $1.hole->child[1] = $2.root;
                              $2.hole->child[1] = $3.root; }
                          ;

Setters_                  : /* empty */
                            { $$.root
                                  = $$.hole
                                  = wire2 (p_decl_imps, @$, nil (@$), nil (@$));
                            }
                          | Setter Separator Setters_ 
                            { $$.root = wire2 (p_decl_imps, @$, $1, $3.root);
                              $$.hole = $3.hole; }
                          ;

VarFunDecls_              : /* empty */
                            { $$.root
                                  = $$.hole
                                  = wire2 (p_decl_imps, @$, nil (@$), nil (@$));
                            }
                          | VarDecl Separator VarFunDecls_
                            { $$.root = wire2 (p_decl_imps, @$, $1, $3.root);
                              $$.hole = $3.hole; }
                          | FunctionDecl Separator VarFunDecls_
                            { $$.root = wire2 (p_decl_imps, @$, $1, $3.root);
                              $$.hole = $3.hole; }
                          ;

/* [7] */
Setter                    : XMLSpaceDecl { $$ = $1; }
                          | DefaultCollationDecl { $$ = $1; }
                          | BaseURIDecl { $$ = $1; }
                          | ConstructionDecl { $$ = $1; }
                          | OrderingModeDecl { $$ = $1; }
                          | EmptyOrderingDecl { $$ = $1; }
                          | InheritNamespacesDecl { $$ = $1; }
                          ;

ImportAndNSDecls_         : /* empty */
                            { $$.root
                                  = $$.hole
                                  = wire2 (p_decl_imps, @$, nil (@$), nil (@$));
                            }
                          | Import Separator ImportAndNSDecls_
                            { $$.root
                                  = wire2 (p_decl_imps, @$,
                                           $1.root,
                                           $1.hole
                                             ? wire2 (p_decl_imps,
                                                      loc_rng($1.hole->loc, @3),
                                                      $1.hole,
                                                      $3.root)
                                             : $3.root);
                              $$.hole = $3.hole;
                            }
                          | NamespaceDecl Separator ImportAndNSDecls_
                            { $$.root = wire2 (p_decl_imps, @$, $1, $3.root);
                              $$.hole = $3.hole; }
                          | DefaultNamespaceDecl Separator ImportAndNSDecls_
                            { $$.root = wire2 (p_decl_imps, @$, $1, $3.root);
                              $$.hole = $3.hole; }
                          ;

/* [8] */
Import                    : SchemaImport { $$ = $1; }
                          | ModuleImport { $$ = $1; }
                          ;

/* [9] */
Separator                 : ";"
                          ;

/* [10] */
NamespaceDecl             : "declare namespace" NCName "=" StringLiteral
                            { ($$ = wire1 (p_ns_decl, 
                                           @$,
                                           (c = leaf (p_lit_str, @4),
                                            c->sem.str = $4,
                                            c)))->sem.str = $2;
                            }
                          ;

/* [11] */
XMLSpaceDecl              : "declare xmlspace preserve"
                            { ($$ = leaf (p_xmls_decl, @$))->sem.tru = true;
                              xmlspace_preserve = true;
                            } 
                          | "declare xmlspace strip"
                            { ($$ = leaf (p_xmls_decl, @$))->sem.tru = false;
                              xmlspace_preserve = false;
                            }
                          ;

/* [12] */
DefaultNamespaceDecl      : "declare default element" "namespace" StringLiteral
                            { $$ = wire1 (p_ens_decl,
                                          @$,
                                          (c = leaf (p_lit_str, @3),
                                           c->sem.str = $3,
                                           c));
                            }
                          | "declare default function" "namespace" StringLiteral
                            { $$ = wire1 (p_fns_decl,
                                          @$,
                                          (c = leaf (p_lit_str, @3),
                                           c->sem.str = $3,
                                           c));
                            }
                          ;

/* [13]  !W3C: Done by the lexer */
OrderingModeDecl          : "declare ordering ordered"
                            { ($$ = leaf (p_ordering_mode,
                                          @$))->sem.tru = true;
                            }
                          | "declare ordering unordered"
                            { ($$ = leaf (p_ordering_mode,
                                          @$))->sem.tru = false;
                            }
                          ;

/* [14] */
EmptyOrderingDecl         : "declare default order" "empty greatest"
                            { ($$ = leaf (p_def_order,
                                          @$))->sem.mode.empty = p_greatest;
                            }
                          | "declare default order" "empty least"
                            { ($$ = leaf (p_def_order,
                                          @$))->sem.mode.empty = p_least;
                            }
                          ;

/* [15]  !W3C: Done by the lexer */
InheritNamespacesDecl     : "declare inherit-namespaces yes"
                            { ($$ = leaf (p_inherit_ns, @$))->sem.tru = true; }
                          | "declare inherit-namespaces no"
                            { ($$ = leaf (p_inherit_ns, @$))->sem.tru = false;}
                          ;

/* [16] */
DefaultCollationDecl      : "declare default collation" StringLiteral
                            { $$ = wire1 (p_coll_decl,
                                          @$,
                                          (c = leaf (p_lit_str, @2),
                                           c->sem.str = $2,
                                           c));
                            }
                          ;

/* [17] */
BaseURIDecl               : "declare base-uri" StringLiteral
                            { $$ = wire1 (p_base_uri,
                                          @$,
                                          (c = leaf (p_lit_str, @2),
                                           c->sem.str = $2,
                                           c));
                            }
                          ;

/* [18] / [19] */
SchemaImport              : "import schema" SchemaSrc_ OptAtStringLiterals_
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
                               * ($2[ROOT] == 0 if no namespace decl given)
                               */
                              $$.root = wire2 (p_schm_imp, @$, $2.hole, $3);
                              $$.hole = $2.root;
                            }
                          ;

SchemaSrc_                : StringLiteral
                            { $$.root = NULL;
                              ($$.hole = leaf (p_lit_str, @$))->sem.str = $1;
                            }
                          | "namespace" NCName "=" StringLiteral
                            { ($$.root = wire1 (p_ns_decl, @$,
                                                (c = leaf (p_lit_str, @4),
                                                 c->sem.str = $4,
                                                 c)))->sem.str = $2;
                              ($$.hole = leaf (p_lit_str, @4))->sem.str = $4;
                            }
                          | "default element" "namespace" StringLiteral
                            { $$.root = wire1 (p_ens_decl, @$,
                                               (c = leaf (p_lit_str, @3),
                                                c->sem.str = $3,
                                                c));
                              ($$.hole = leaf (p_lit_str, @3))->sem.str = $3;
                            }
                          ;

OptAtStringLiterals_      : /* empty */
                            { $$ = nil (@$); }
                          | at_StringLiteral StringLiterals_
                            { $$ = wire2 (p_schm_ats, @$,
                                          (c = leaf (p_lit_str, @1),
                                           c->sem.str = $1,
                                           c),
                                          $2);
                            }
                          ;

StringLiterals_           : /* empty */
                            { $$ = nil (@$); }
                          | StringLiteral "," StringLiterals_
                            { $$ = wire2 (p_schm_ats, @$,
                                          (c = leaf (p_lit_str, @1),
                                           c->sem.str = $1,
                                           c),
                                          $3);
                            }
                          ;

/* [20] */
ModuleImport              : "import module" ModuleNS_ OptAtStringLiterals_
                            {
                              PFinfo_loc (OOPS_NOTSUPPORTED, @$,
                                          "Pathfinder does currently not "
                                          "support XQuery modules.");
                              YYERROR;

                              $$.root = wire2 (p_mod_imp, @$, $2.root, $3);
                              $$.hole = $2.root;
                            }
                          ;

ModuleNS_                 : StringLiteral
                            { $$.root = NULL;
                              ($$.hole = leaf (p_lit_str, @$))->sem.str = $1;
                            }
                          | "namespace" NCName "=" StringLiteral
                            { ($$.root = wire1 (p_ns_decl, @$,
                                                (c = leaf (p_lit_str, @4),
                                                 c->sem.str = $4,
                                                 c)))->sem.str = $2;
                              ($$.hole = leaf (p_lit_str, @4))->sem.str = $4;
                            }
                          ;

/* [21] */
VarDecl                   : "declare variable $" VarName OptTypeDeclaration_
                             ":=" ExprSingle
                            { $$ = wire2 (p_var_decl, @$,
                                     wire2 (p_var_type, loc_rng (@2, @3),
                                            $2, $3),
                                     $5);
                            }
                          | "declare variable $" VarName OptTypeDeclaration_
                             "external"
                            { $$ = wire2 (p_var_decl, @$,
                                     wire2 (p_var_type, loc_rng (@2, @3),
                                            $2, $3),
                                     leaf (p_external, @4));
                            }
                          ;

OptTypeDeclaration_       : /* empty */      { $$ = nil (@$); }
                          | TypeDeclaration  { $$ = $1; }
                          ;

/* [22]  !W3C: Done by the lexer */
ConstructionDecl          : "declare construction preserve"
                            { ($$ = leaf (p_constr_decl, @$))->sem.tru = true; } 
                          | "declare construction strip"
                            { ($$ = leaf (p_constr_decl, @$))->sem.tru = false;} 
                          ;

/* [23] */
FunctionDecl              : "declare function" QName_LParen
                            OptParamList_ OptAsSequenceType_ EnclosedExpr
                            { ($$ = wire2 (p_fun_decl, @$,
                                           wire2 (p_fun_sig, loc_rng (@2, @4),
                                                  $3, $4),
                                           $5))->sem.qname = $2;
                            }
                          | "declare function" QName_LParen
                            OptParamList_ OptAsSequenceType_ "external"
                            { ($$ = wire2 (p_fun_decl, @$,
                                           wire2 (p_fun_sig, loc_rng (@2, @4),
                                                  $3, $4),
                                           leaf (p_external, @5)))
                                   ->sem.qname = $2;
                            }
                          ;

OptParamList_             : /* empty */  { $$ = nil (@$); }
                          | ParamList    { $$ = $1; }
                          ;

OptAsSequenceType_        : ")"
                            { /* W3C XQuery Section 4.15:
                               * ``If the result type is omitted from a
                               * function declaration, its default result
                               * type is item*.''
                               */
                              ($$ = wire1 (p_seq_ty, @$,
                                           wire1 (p_item_ty, @$, nil (@$))))
                                ->sem.oci = p_zero_or_more;
                            }
                          | ") as" SequenceType  { $$ = $2; }
                          ;

/* [24] */
ParamList                 : Param
                            { $$ = wire2 (p_params, @$, $1, nil (@$)); }
                          | Param "," ParamList
                            { $$ = wire2 (p_params, @$, $1, $3); }
                          ;

/* [25] */
Param                     : "$" VarName OptParamTypeDeclaration_
                            { $$ = wire2 (p_param, @$, $3, $2); }
                          ;

OptParamTypeDeclaration_  : /* empty */
                            { /* W3C XQuery Section 4.15:
                               * ``If a function parameter is declared using
                               * a name but no type, its default type is
                               * item*.''
                               */
                              ($$ = wire1 (p_seq_ty, @$,
                                           wire1 (p_item_ty, @$, nil (@$))))
                                ->sem.oci = p_zero_or_more;
                            }
                          | TypeDeclaration  { $$ = $1; }
                          ;


/* [26] */
EnclosedExpr              : "{" Expr "}" { $$ = $2; }
                          ;

/* [27] */
QueryBody                 : Expr { $$ = $1; }
                          | /* empty */ { $$ = leaf (p_empty_seq, @$); }
                          ;

/* [28] */
Expr                      : ExprSingle
                            { $$ = wire2 (p_exprseq, @$,
                                          $1,
                                          leaf (p_empty_seq, @1)); }
                          | ExprSingle "," Expr
                            { $$ = wire2 (p_exprseq, @$, $1, $3); }
                          ;

/* [29] */
ExprSingle                : FLWORExpr       { $$ = $1; }
                          | QuantifiedExpr  { $$ = $1; }
                          | TypeswitchExpr  { $$ = $1; }
                          | IfExpr          { $$ = $1; }
                          | OrExpr          { $$ = $1; }
                          ;

/* [30] */
FLWORExpr                 : ForLetClauses_ OptWhereClause_ OptOrderByClause_
                            "return" ExprSingle
                            { $$ = wire2 (p_flwr, @$, $1.root,
                                     wire2 (p_where, loc_rng (@2, @5), $2,
                                       wire2 (p_ord_ret, loc_rng (@3, @5), $3,
                                         $5)));
                            }
                          ;

ForLetClauses_            : ForLetClause_
                            { $$.root = $$.hole = $1; }
                            /* { $$ = $1; } */
                          | ForLetClauses_ ForLetClause_
                            { FIXUP (1, $1, $2);
                              $$.hole = $2; $$.root = $1.root; }
                          ;

ForLetClause_             : ForClause  { $$ = $1; }
                          | LetClause  { $$ = $1; }
                          ;

OptWhereClause_           : /* empty */  { $$ = nil (@$); }
                          | WhereClause  { $$ = $1; }
                          ;

OptOrderByClause_         : /* empty */    { $$ = nil (@$); }
                          | OrderByClause  { $$ = $1; }
                          ;

/* [31] */
ForClause                 : "for $" VarPosBindings_ { $$ = $2; }
                          ;

VarPosBindings_           : VarName OptTypeDeclaration_ OptPositionalVar_
                              "in" ExprSingle
                            { $$ = wire2 (p_binds, @$,
                                     wire2 (p_bind, @$,
                                       wire2 (p_vars, loc_rng (@1, @3),
                                         wire2 (p_var_type, loc_rng (@1, @2),
                                                $1,
                                                $2),
                                         $3),
                                       $5),
                                     nil (@$));
                            }
                          | VarName OptTypeDeclaration_ OptPositionalVar_
                              "in" ExprSingle "," "$" VarPosBindings_
                            { $$ = wire2 (p_binds, @$,
                                     wire2 (p_bind, @$,
                                       wire2 (p_vars, loc_rng (@1, @3),
                                         wire2 (p_var_type, loc_rng (@1, @2),
                                                $1,
                                                $2),
                                         $3),
                                       $5),
                                     $8);
                            }
                          ;

OptPositionalVar_         : /* empty */     { $$ = nil (@$); }
                          | PositionalVar   { $$ = $1; }
                          ;

/* [32] */
PositionalVar             : "at $" VarName  { $$ = $2; }
                          ;

/* [33] */
LetClause                 : "let $" LetBindings_ { $$ = $2; }
                          ;

LetBindings_              : VarName OptTypeDeclaration_ ":=" ExprSingle
                            { $$ = wire2 (p_binds, @$,
                                     wire2 (p_let, @$,
                                       wire2 (p_var_type, loc_rng (@1, @2),
                                         $1, $2),
                                       $4),
                                     nil (@$));
                            }
                          | VarName OptTypeDeclaration_ ":=" ExprSingle
                              "," "$" LetBindings_
                            { $$ = wire2 (p_binds, @$,
                                     wire2 (p_let, @$,
                                       wire2 (p_var_type, loc_rng (@1, @2),
                                         $1, $2),
                                       $4),
                                     $7);
                            }
                          ;

/* [34] */
WhereClause               : "where" ExprSingle { $$ = $2; }
                          ;

/* [35] */
OrderByClause             : "order by" OrderSpecList
                            {($$ = wire1 (p_orderby, @$, $2))->sem.tru = false;}
                          | "stable order by" OrderSpecList
                            {($$ = wire1 (p_orderby, @$, $2))->sem.tru = true;}
                          ;

/* [36] / [37] */
OrderSpecList             : ExprSingle OrderModifier
                            { ($$ = wire2 (p_orderspecs, @$,
                                           $1,
                                           nil (@$)))->sem.mode = $2;
                            }
                          | ExprSingle OrderModifier "," OrderSpecList
                            { ($$ = wire2 (p_orderspecs, @$,
                                           $1,
                                           $4))->sem.mode = $2;
                            }
                          ;

/* [38] */
OrderModifier             : OptAscendingDescending_ OptEmptyGreatestLeast_
                            OptCollationStringLiteral_
                            { $$.dir = $1, $$.empty = $2; $$.coll = $3; }
                          ;

OptAscendingDescending_   : /* empty */   { $$ = p_asc; }
                          | "ascending"   { $$ = p_asc; }
                          | "descending"  { $$ = p_desc; }
                          ;

OptEmptyGreatestLeast_    : /* empty */       { $$ = p_least; }
                          | "empty greatest"  { $$ = p_greatest; }
                          | "empty least"     { $$ = p_least; }
                          ;

OptCollationStringLiteral_: /* empty */                { $$ = NULL; }
                          | "collation" StringLiteral  { $$ = $2; }
                          ;

/* [39] */
QuantifiedExpr            : SomeEvery_ VarBindings_ "satisfies" ExprSingle
                            { $$ = wire2 ($1, @$, $2, $4); }
                          ;

SomeEvery_                : "some $"  { $$ = p_some; }
                          | "every $" { $$ = p_every; }
                          ;

VarBindings_              : VarName OptTypeDeclaration_ "in" ExprSingle
                            { $$ = wire2 (p_binds, @$,
                                     wire2 (p_bind, @$,
                                       wire2 (p_vars, @1,
                                         wire2 (p_var_type, @1, $1, $2),
                                         nil (@2)),
                                       $4),
                                     nil (@$)); }
                          | VarName OptTypeDeclaration_ "in" ExprSingle
                              "," "$" VarBindings_
                            { $$ = wire2 (p_binds, @$,
                                     wire2 (p_bind, @$,
                                       wire2 (p_vars, @1,
                                         wire2 (p_var_type, @1, $1, $2),
                                         nil (@2)),
                                       $4),
                                     $7); }
                          ;

/* [40] */
TypeswitchExpr            : "typeswitch (" Expr ")" CaseClauses_ "default"
                              OptDollarVarName_ "return" ExprSingle
                            { FIXUP (1,
                                     $4,
                                     wire2 (p_cases, loc_rng (@5, @8),
                                       wire2 (p_default, loc_rng (@5, @8),
                                              $6, $8),
                                       nil (@$)));
                              $$ = wire2 (p_typeswitch, @$, $2, $4.root);
                            }
                          ;

CaseClauses_              : CaseClause
                            { $$.root
                                  = $$.hole
                                  = wire2 (p_cases, @$, $1, nil (@$)); }
                          | CaseClause CaseClauses_
                            { $$.root = wire2 (p_cases, @$, $1, $2.root);
                              $$.hole = $2.hole; }
                          ;

OptDollarVarName_         : /* empty */ { $$ = nil (@$); }
                          | "$" VarName { $$ = $2; }
                          ;

/* [41] */
CaseClause                : "case" OptDollarVarNameAs_ SequenceType
                              "return" ExprSingle
                            { $$ = wire2 (p_case, @$,
                                     wire2 (p_var_type, loc_rng (@2, @3),
                                            $2, $3),
                                     $5);
                            }
                          ;

OptDollarVarNameAs_       : /* empty */      { $$ = nil (@$); }
                          | "$" VarName "as" { $$ = $2; }
                          ;

/* [42] */
IfExpr                    : "if (" Expr ")" "then" ExprSingle "else" ExprSingle
                            { $$ = wire2 (p_if, @$,
                                          $2,
                                          wire2 (p_then_else, loc_rng (@4, @7),
                                                 $5, $7));
                            }
                          ;

/* [43] */
OrExpr                    : AndExpr { $$ = $1; }
                          | AndExpr "or" OrExpr
                            { $$ = wire2 (p_or, @$, $1, $3); }
                          ;

/* [44] */
AndExpr                   : ComparisonExpr { $$ = $1; }
                          | ComparisonExpr "and" AndExpr
                            { $$ = wire2 (p_and, @$, $1, $3); }
                          ;

/* [45] */
ComparisonExpr            : RangeExpr { $$ = $1; }
                          | RangeExpr ValueComp RangeExpr
                            { $$ = wire2 ($2, @$, $1, $3); }
                          | RangeExpr GeneralComp RangeExpr
                            { $$ = wire2 ($2, @$, $1, $3); }
                          | RangeExpr NodeComp RangeExpr
                            { $$ = wire2 ($2, @$, $1, $3); }
                          ;

/* [46] */
RangeExpr                 : AdditiveExpr { $$ = $1; }
                          | AdditiveExpr "to" AdditiveExpr
                            { $$ = wire2 (p_range, @$, $1, $3); }
                          ;

/* [47] */
AdditiveExpr              : MultiplicativeExpr { $$ = $1; }
                          | MultiplicativeExpr "+" AdditiveExpr
                            { $$ = wire2 (p_plus, @$, $1, $3); }
                          | MultiplicativeExpr "-" AdditiveExpr
                            { $$ = wire2 (p_minus, @$, $1, $3); }
                          ;

/* [48] */
MultiplicativeExpr        : UnionExpr { $$ = $1; }
                          | UnionExpr DivOp_ MultiplicativeExpr
                            { $$ = wire2 ($2, @$, $1, $3); }
                          ;

DivOp_                    : "*"    { $$ = p_mult; }
                          | "div"  { $$ = p_div; }
                          | "idiv" { $$ = p_idiv; }
                          | "mod"  { $$ = p_mod; }
                          ;

/* [49] */
UnionExpr                 : IntersectExceptExpr { $$ = $1; }
                          | IntersectExceptExpr "union" UnionExpr
                            { $$ = wire2 (p_union, @$, $1, $3); }
                          | IntersectExceptExpr "|" UnionExpr
                            { $$ = wire2 (p_union, @$, $1, $3); }
                          ;

/* [50] */
IntersectExceptExpr       : InstanceofExpr { $$ = $1; }
                          | InstanceofExpr "intersect" IntersectExceptExpr
                            { $$ = wire2 (p_intersect, @$, $1, $3); }
                          | InstanceofExpr "except" IntersectExceptExpr
                            { $$ = wire2 (p_except, @$, $1, $3); }
                          ;

/* [51] */
InstanceofExpr            : TreatExpr { $$ = $1; }
                          | TreatExpr "instance of" SequenceType
                            { $$ = wire2 (p_instof, @$, $1, $3); }
                          ;

/* [52] */
TreatExpr                 : CastableExpr { $$ = $1; }
                          | CastableExpr "treat as" SequenceType
                            { /* FIXME: WARNING: swapped children here to
                                        be consistent with other stuff. */
                              $$ = wire2 (p_treat, @$, $1, $3); }
                          ;

/* [53] */
CastableExpr              : CastExpr { $$ = $1; }
                          | CastExpr "castable as" SingleType
                            { $$ = wire2 (p_castable, @$, $1, $3); }
                          ;

/* [54] */
CastExpr                  : UnaryExpr { $$ = $1; }
                          | UnaryExpr "cast as" SingleType
                            { /* FIXME: WARNING: swapped children here to
                                        be consistent with other stuff. */
                              $$ = wire2 (p_cast, @$, $1, $3); }
                          ;

/* [55]  !W3C */
UnaryExpr                 : ValueExpr     { $$ = $1; }
                          | "-" UnaryExpr { $$ = wire1 (p_uminus, @$, $2); }
                          | "+" UnaryExpr { $$ = wire1 (p_uplus, @$, $2); }
                          ;

/* [56] */
ValueExpr                 : ValidateExpr { $$ = $1; }
                          | PathExpr { $$ = flatten_locpath ($1, NULL); }
                          ;

/* [57] */
GeneralComp               : "="  { $$ = p_eq; }
                          | "!=" { $$ = p_ne; }
                          | "<"  { $$ = p_lt; }
                          | "<=" { $$ = p_le; }
                          | ">"  { $$ = p_gt; }
                          | ">=" { $$ = p_ge; }
                          ;

/* [58] */
ValueComp                 : "eq" { $$ = p_val_eq; }
                          | "ne" { $$ = p_val_ne; }
                          | "lt" { $$ = p_val_lt; }
                          | "le" { $$ = p_val_le; }
                          | "gt" { $$ = p_val_gt; }
                          | "ge" { $$ = p_val_ge; }
                          ;

/* [59] */
NodeComp                  : "is" { $$ = p_is; }
                          | "<<" { $$ = p_ltlt; }
                          | ">>" { $$ = p_gtgt; }
                          ;

/* [60] / [138] */
/* FIXME: Validation nodes changed */
ValidateExpr              : "validate {" Expr "}"
                            { /* No validation mode means `strict'. */
                              ($$ = wire1 (p_validate, @$, $2))
                                ->sem.tru = true; }
                          | "validate lax {" Expr "}"
                            { ($$ = wire1 (p_validate, @$, $2))
                                ->sem.tru = false; }
                          | "validate strict {" Expr "}"
                            { ($$ = wire1 (p_validate, @$, $2))
                                ->sem.tru = true; }
                          ;

/* [61] */
PathExpr                  : "/"
                            { $$ = leaf (p_root, @$); }
                          | "/" RelativePathExpr
                            { $$ = wire2 (p_locpath, @$,
                                          $2, leaf (p_root, @1)); }
                          | "//" RelativePathExpr
                            { $$ = wire2 (
                                    p_locpath,
                                    @$,
                                    wire2 (
                                     p_locpath,
                                     @$,
                                     $2,
                                     wire2 (
                                      p_locpath,
                                      @1,
                                      (c = wire1 (
                                            p_step,
                                            @1,
                                            (c1 = wire1 (p_node_ty,
                                                         @1, nil (@1)),
                                             c1->sem.kind = p_kind_node,
                                             c1)),
                                       c->sem.axis = p_descendant_or_self,
                                       c),
                                      leaf (p_dot, @1))),
                                    leaf (p_root, @1));
                            }
                          | RelativePathExpr { $$ = $1; }
                          ;

/* [62] */
RelativePathExpr          : StepExpr { $$ = $1; }
                          | StepExpr "/" RelativePathExpr
                            { $$ = wire2 (p_locpath, @$, $3, $1); }
                          | StepExpr "//" RelativePathExpr
                            { $$ = wire2 (
                                    p_locpath,
                                    @$,
                                    wire2 (
                                     p_locpath,
                                     @$,
                                     $3,
                                     wire2 (
                                      p_locpath, @2,
                                      (c = wire1 (
                                            p_step,
                                            @2,
                                            (c1 = wire1 (p_node_ty, @2,
                                                         nil (@2)),
                                             c1->sem.kind = p_kind_node,
                                             c1)),
                                       c->sem.axis = p_descendant_or_self,
                                       c),
                                      leaf (p_dot, @2))),
                                    $1);
                            }
                          ;

/* [63] */
StepExpr                  : AxisStep   { $$ = $1; }
                          | FilterExpr { $$ = $1; }
                          ;

/* [64]  !W3C: Factored out attribute step */
AxisStep                  : ForwardStep
                            { $$ = $1; }
                          | ForwardStep PredicateList
                            { FIXUP (0, $2, $1); $$ = $2.root; }
                          | ReverseStep
                            { $$ = $1; }
                          | ReverseStep PredicateList
                            { FIXUP (0, $2, $1); $$ = $2.root; }
                          | AttribStep_
                            { $$ = $1; }
                          | AttribStep_ PredicateList
                            { FIXUP (0, $2, $1); $$ = $2.root; }
                          ;

/* [65] */
ForwardStep               : ForwardAxis NodeTest
                            { ($$ = wire1 (p_step, @$, $2))->sem.axis = $1; }
                          | AbbrevForwardStep
                            { $$ = $1; }
                          ;

/* [66] */
ForwardAxis               : "child::"              { $$ = p_child; }
                          | "descendant::"         { $$ = p_descendant; }
                          | "self::"               { $$ = p_self; }
                          | "descendant-or-self::" { $$ = p_descendant_or_self;}
                          | "following-sibling::"  { $$ = p_following_sibling; }
                          | "following::"          { $$ = p_following; }
                          ;

/* [67] */
AbbrevForwardStep         : NodeTest
                            { ($$ = wire1 (p_step,
                                             @$,
                                             $1))->sem.axis = p_child;
                            }
                          ;

/* [68] */
ReverseStep               : ReverseAxis NodeTest
                            { ($$ = wire1 (p_step, @$, $2))->sem.axis = $1; }
                          | AbbrevReverseStep
                            { $$ = $1; }
                          ;

/* [69] */
ReverseAxis               : "parent::"             { $$ = p_parent; }
                          | "ancestor::"           { $$ = p_ancestor; }
                          | "preceding-sibling::"  { $$ = p_preceding_sibling; }
                          | "preceding::"          { $$ = p_preceding; }
                          | "ancestor-or-self::"   { $$ = p_ancestor_or_self; }
                          ;

/* [70] */
AbbrevReverseStep         : ".."
                            { ($$ = wire1 (p_step,
                                           @$,
                                           (c = wire1 (p_node_ty, @$, nil (@$)),
                                            c->sem.kind = p_kind_node,
                                            c)))->sem.axis = p_parent;
                            }
                          ;

/* !W3C: Attribute step factored out */
AttribStep_               : AttributeAxis_ AttribNodeTest
                            { ($$ = wire1 (p_step, @$, $2))->sem.axis = $1; }
                          | AbbrevAttribStep_
                            { $$ = $1; }
                          ;

/* !W3C: Attribute step factored out */
AttributeAxis_            : "attribute::"          { $$ = p_attribute; }
                          ;

/* !W3C: Attribute step factored out */
AbbrevAttribStep_         : "@" AttribNodeTest
                            { ($$ = wire1 (p_step,
                                           @$,
                                           $2))->sem.axis = p_attribute;
                            }
                          ;

/* [71] */
NodeTest                  : KindTest
                            { $$ = $1; }
                          | NameTest
                            { ($$ = wire1 (p_node_ty, @$, $1))
                                ->sem.kind = p_kind_elem; }
                          ;

/* [72] */
NameTest                  : QName
                            { $$ = wire2 (p_req_ty, @$,
                                          (c = leaf (p_req_name, @$),
                                           c->sem.qname = $1,
                                           c),
                                          nil (@$)); }
                          | Wildcard
                            { $$ = $1; }
                          ;

/* !W3C: Attribute step factored out */
AttribNodeTest            : KindTest
                            { $$ = $1; }
                          | NameTest
                            { ($$ = wire1 (p_node_ty, @$, $1))
                                ->sem.kind = p_kind_attr; }
                          ;

/* [73] */
Wildcard                  : "*"
                            { $$ = wire2 (p_req_ty, @$, nil (@$), nil (@$)); }
                          | NCName_Colon_Star
                            { $$ = wire2 (p_req_ty, @$,
                                          (c = leaf (p_req_name, @$),
                                           c->sem.qname = $1,
                                           c),
                                          nil (@$)); }
                          | Star_Colon_NCName
                            { $$ = wire2 (p_req_ty, @$,
                                          (c = leaf (p_req_name, @$),
                                           c->sem.qname = $1,
                                           c),
                                          nil (@$)); }
                          ;

/* [74] */
FilterExpr                : PrimaryExpr { $$ = $1; }
                          | PrimaryExpr PredicateList
                            { FIXUP (0, $2, $1); $$ = $2.root; }
                          ;

/* [75] */
PredicateList             : Predicate
                            { $$.root = $$.hole = wire2 (p_pred, @1, NULL, $1);}
                          | Predicate PredicateList
                            { $$.hole
                                  = $2.hole->child[0]
                                  = wire2 (p_pred, @1, NULL, $1);
                              $$.root = $2.root;
                            }
                          ;

/* [76] */
Predicate                 : "[" Expr "]" { $$ = $2; }
                          ;

/* [77] */
PrimaryExpr               : Literal           { $$ = $1; }
                          | VarRef            { $$ = $1; }
                          | ParenthesizedExpr { $$ = $1; }
                          | ContextItemExpr   { $$ = $1; }
                          | FunctionCall      { $$ = $1; }
                          | Constructor       { $$ = $1; }
                          | OrderedExpr       { $$ = $1; }
                          | UnorderedExpr     { $$ = $1; }
                          ;

/* [78] */
Literal                   : NumericLiteral { $$ = $1; }
                          | StringLiteral
                            { ($$ = leaf (p_lit_str, @$))->sem.str = $1; }
                          ;

/* [79] */
NumericLiteral            : IntegerLiteral
                            { ($$ = leaf (p_lit_int, @$))->sem.num = $1; }
                          | DecimalLiteral
                            { ($$ = leaf (p_lit_dec, @$))->sem.dec = $1; }
                          | DoubleLiteral
                            { ($$ = leaf (p_lit_dbl, @$))->sem.dbl = $1; }
                          ;

/* [80] */
VarRef                    : "$" VarName { $$ = $2; }
                          ;

/* [81] */
ParenthesizedExpr         : "(" ")"       { $$ = leaf (p_empty_seq, @$); }
                          | "(" Expr ")"  { $$ = $2; }
                          ;

/* [82] */
ContextItemExpr           : "."
                            { $$ = leaf (p_dot, @$); }
                              /*
                               * The `.' was a step in the Nov 2002 draft.
                               * The translation below would be closer to
                               * that old view.
                               *
                              ($$ = wire1 (p_step,
                                           @$,
                                           (c = wire1 (p_node_ty, @$, nil (@$)),
                                            c->sem.kind = p_kind_node,
                                            c)))->sem.axis = p_self;
                              */
                          ;

/* [83] */
OrderedExpr               : "ordered {" Expr "}"
                            { $$ = wire1 (p_ordered, @$, $2); }
                          ;

/* [84] */
UnorderedExpr             : "unordered {" Expr "}"
                            { $$ = wire1 (p_unordered, @$, $2); }
                          ;

/* [85] */
FunctionCall              : QName_LParen OptFuncArgList_ ")"
                            { ($$ = wire1 (p_fun_ref, @$, $2))
                                ->sem.qname = $1; }
                          ;

OptFuncArgList_           : /* empty */   { $$ = nil (@$); }
                          | FuncArgList_  { $$ = $1; }
                          ;

FuncArgList_              : ExprSingle
                            { $$ = wire2 (p_args, @$, $1, nil (@1)); }
                          | FuncArgList_ "," ExprSingle
                            { /* FIXME: For compatibility with the parser that
                               * had followed the Nov 2002 draft, build up
                               * this list in a backward fashion. Core mapping
                               * will reverse this again.
                               * (If you want to change the order here, just
                               * swap FuncArgList_ and ExprSingle in the rule
                               * and the parameters of the below wire2 call.
                               */
                              $$ = wire2 (p_args, @$, $3, $1); }
                          ;

/* [86] */
Constructor               : DirectConstructor   { $$ = $1; }
                          | ComputedConstructor { $$ = $1; }
                          ;

/* [87] */
DirectConstructor         : DirElemConstructor    { $$ = $1; }
                          | DirCommentConstructor { $$ = $1; }
                          | DirPIConstructor      { $$ = $1; }
                          ;

/* [88] */
DirElemConstructor        : "<" QName DirAttributeList "/>"
                            { if ($3.root)
                                $3.hole->child[1] = leaf (p_empty_seq, @3);
                              else
                                $3.root = leaf (p_empty_seq, @3);

                              $$ = wire2 (p_elem,
                                          @$,
                                          (c = leaf (p_tag, @2),
                                           c->sem.qname = $2,
                                           c),
                                          $3.root);
                            }
                          | "<" QName DirAttributeList ">"
                            DirElementContents_
                            "</" QName OptS_ ">"
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
                               if ($3.root)
                                 FIXUP (1, $3, $5);
                               else
                                 $3.root = $5;

                               $$ = wire2 (p_elem,
                                           @$,
                                           (c = leaf (p_tag, @2),
                                            c->sem.qname = $2,
                                            c),
                                           $3.root);
                            }
                          ;

OptS_                     : /* empty */
                          | S
                          ;

DirElementContents_       : /* empty */
                            { $$ = leaf (p_empty_seq, @$); }
                          | DirElemContent DirElementContents_
                            { $$ = wire2 (p_contseq, @$, $1, $2); }
                          ;

/* [89] */
DirAttributeList          : /* empty */
                            { $$.root = $$.hole = NULL; }
                          | S DirAttributeList
                            { $$ = $2; }
                          | S DirAttribute_ DirAttributeList
                            { $$.root
                                  = $$.hole
                                  = wire2 (p_contseq, @$, $2, $3.root);
                              if ($3.root)
                                  $$.hole = $3.hole;
                            }
                          ;

DirAttribute_             : QName OptS_ "=" OptS_ DirAttributeValue
                            { $$ = wire2 (p_attr, @$,
                                          (c = leaf (p_tag, @1),
                                           c->sem.qname = $1,
                                           c), $5);
                            }
                          ;

/* [90] */
DirAttributeValue         : "\"" AttributeValueConts_ "\"" { $$ = $2; }
                          | "'" AttributeValueConts_ "'"   { $$ = $2; }
                          ;

AttributeValueConts_      : /* empty */
                            { $$ = leaf (p_empty_seq, @$); }
                          | AttributeValueCont_ AttributeValueConts_
                            { $$ = wire2 (p_contseq, @$, $1, $2); }
                          ;

AttributeValueCont_       : AttributeValueContTexts_
                            { /* add trailing '\0' to string */
                              *((char *) PFarray_add ($1)) = '\0';

                              $$ = wire2 (p_contseq, @1,
                                          (c = leaf (p_lit_str, @1),
                                           c->sem.str = PFarray_at ($1, 0),
                                           c),
                                          leaf (p_empty_seq, @1)
                                         ); }
                          | EnclosedExpr
                            { $$ = $1; }
                          ;

AttributeValueContTexts_  : AttributeValueContText_
                            { /*
                               * initialize new dynamic array and insert
                               * one character
                               */
                              $$ = PFarray (sizeof (char));
                              *((char *) PFarray_add ($$)) = $1;
                            }
                          | AttributeValueContTexts_ AttributeValueContText_
                            { /* append one charater to array */
                              *((char *) PFarray_add ($1)) = $2;
                            }
                          ;

AttributeValueContText_   : AttrContentChar      { $$ = $1; }
                          | EscapeQuot           { $$ = $1; }
                          | EscapeApos           { $$ = $1; }
                          | PredefinedEntityRef  { $$ = $1; }
                          | CharRef              { $$ = $1; }
                          | "{{"                 { $$ = '{'; }
                          | "}}"                 { $$ = '}'; }
                          ;

/* [93] */
DirElemContent            : DirectConstructor  { $$ = $1; }
                          | ElementContentTexts_
                            { /* add trailing '\0' to string */
                              *((char *) PFarray_add ($1)) = '\0';
                                             
                              /* xmlspace handling */
                              if ((! xmlspace_preserve) &&
                                  (is_whitespace (PFarray_at ($1, 0))))
                                $$ = leaf (p_empty_seq, @1);
                              else
                                $$ = wire1 (p_text, @1,
                                            wire2 (p_exprseq, @1,
                                                   (c = leaf (p_lit_str, @1),
                                                    c->sem.str = 
                                                      PFarray_at ($1, 0),
                                                    c),
                                                   leaf (p_empty_seq, @1)
                                                  )); 
                            }
                          | CDataSection  { $$ = $1; }
                          | EnclosedExpr  { $$ = $1; }
                          ;

ElementContentTexts_      : ElementContentText_
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

ElementContentText_       : ElementContentChar    { $$ = $1; }
                          | PredefinedEntityRef   { $$ = $1; }
                          | CharRef               { $$ = $1; }
                          | "{{"                  { $$ = '{'; }
                          | "}}"                  { $$ = '}'; }
                          ;

/* [95] */
DirCommentConstructor     : "<!--" DirCommentContents "-->"
                            { $$ = wire1 (p_comment, @$, $2); }
                          ;

/* [96] */
DirCommentContents        : Characters_ { $$ = $1; }
                          ;

Characters_               : Chars_
                            { /* add trailing '\0' to string */
                              *((char *) PFarray_add ($1)) = '\0';

                              $$ = leaf (p_lit_str, @1);
                              $$->sem.str = (char *) PFarray_at ($1, 0);
                            }
                          ;

/* collect single characters in a dynamic array */
Chars_                    : /* empty */
                            { /* initialize new dynamic array */
                              $$ = PFarray (sizeof (char)); }
                          | Chars_ PFChar
                            { /* append one charater to array */
                              *((char *) PFarray_add ($1)) = $2;
                              $$ = $1;
                            }
                          ;

/* [97] */
DirPIConstructor          : "<?" PITarget DirPIContents "?>"
                            { /* FIXME: p_pi is now binary. */
                              if (!strcmp ($2, "xml")) {
                                  PFinfo_loc (OOPS_PARSE, @$,
                                              "`xml' is reserved and may not "
                                              "be used as a PI target.");
                                  YYERROR;
                              }

                              $$ = wire2 (p_pi, @$,
                                          (c = leaf (p_lit_str, @$),
                                           c->sem.str = $2,
                                           c),
                                          $3);
                            }
                          ;

/* [98] */
DirPIContents             : S Characters_ { $$ = $2; }
                          ;

/* [99] */
CDataSection              : "<![CDATA[" CDataSectionContents "]]>"
                            { $$ = wire1 (p_text, @$,
                                          wire2 (p_exprseq, @2, $2,
                                                 leaf (p_empty_seq, @2))); }
                          ;

/* [100] */
CDataSectionContents      : Characters_ { $$ = $1; }
                          ;

/* [101] */
ComputedConstructor       : CompDocConstructor      { $$ = $1; }
                          | CompElemConstructor     { $$ = $1; }
                          | CompAttrConstructor     { $$ = $1; }
                          | CompTextConstructor     { $$ = $1; }
                          | CompCommentConstructor  { $$ = $1; }
                          | CompPIConstructor       { $$ = $1; }
                          ;

/* [102] */
CompDocConstructor        : "document {" Expr "}"
                            { $$ = wire1 (p_doc, @$, $2); }
                          ;

/* [103] */
CompElemConstructor       : Element_QName_LBrace OptContentExpr_ "}"
                            { $$ = wire2 (p_elem, @$,
                                          (c = leaf (p_tag, @1),
                                           c->sem.qname = $1,
                                           c),
                                          wire2 (p_contseq, @2, $2,
                                                 leaf (p_empty_seq, @2)));
                            }
                          | "element {" Expr "}" "{" OptContentExpr_ "}"
                            { $$ = wire2 (p_elem, @$, $2,
                                          wire2 (p_contseq, @5, $5,
                                                 leaf (p_empty_seq, @2))); }
                          ;

OptContentExpr_           : /* empty */   { $$ = leaf (p_empty_seq, @$); }
                          | ContentExpr   { $$ = $1; }
                          ;

/* [104] */
ContentExpr               : Expr   { $$ = $1; }
                          ;

/* [105]  !W3C: OptContentExpr_ (symmetric to CompElemConstructor) */
CompAttrConstructor       : Attribute_QName_LBrace OptContentExpr_ "}"
                            { $$ = wire2 (p_attr, @$,
                                          (c = leaf (p_tag, @1),
                                           c->sem.qname = $1,
                                           c),
                                          wire2 (p_contseq, @2, $2,
                                                 leaf (p_empty_seq, @2)));
                            }
                          | "attribute {" Expr "}" "{" OptContentExpr_ "}"
                            { $$ = wire2 (p_attr, @$, $2,
                                          wire2 (p_contseq, @5, $5,
                                                 leaf (p_empty_seq, @5))); }
                          ;

/* [106] */
CompTextConstructor       : "text {" Expr "}"
                            { $$ = wire1 (p_text, @$, $2); }
                          ;

/* [107] */
CompCommentConstructor    : "comment {" Expr "}"
                            { $$ = wire1 (p_comment, @$, $2); }
                          ;

/* [108] */
CompPIConstructor         : PI_NCName_LBrace OptContentExpr_ "}"
                            { $$ = wire2 (p_pi, @$,
                                          (c = leaf (p_lit_str, @2),
                                           c->sem.str = $1,
                                           c),
                                          wire2 (p_contseq, @2, $2,
                                                 leaf (p_empty_seq, @2))); }
                          | "processing-instruction {" Expr "}"
                              "{" OptContentExpr_ "}"
                            { $$ = wire2 (p_pi, @$, $2, $5); }
                          ;

/* [109] */
SingleType                : AtomicType
                            { ($$ = wire1 (p_seq_ty, @$, $1))
                                ->sem.oci = p_one; }
                          | AtomicType "?"
                            { ($$ = wire1 (p_seq_ty, @$, $1))
                                ->sem.oci = p_zero_or_one; }
                          ;

/* [110] */
TypeDeclaration           : "as" SequenceType { $$ = $2; }
                          ;

/* [111] */
SequenceType              : ItemType OccurrenceIndicator
                            { ($$ = wire1 (p_seq_ty, @$, $1))->sem.oci = $2; }
                          | "empty ()"
                            { ($$ = wire1 (p_seq_ty, @$, leaf (p_empty_ty, @$)))
                                ->sem.oci = p_one; }
                          ;

/* [112] */
OccurrenceIndicator       : /* empty */   { $$ = p_one; }
                          | "?"           { $$ = p_zero_or_one; }
                          | "*"           { $$ = p_zero_or_more; }
                          | "+"           { $$ = p_one_or_more; }
                          ;

/* [113] */
ItemType                  : AtomicType { $$ = $1; }
                          | KindTest   { $$ = $1; }
                          | "item ()"  { $$ = wire1 (p_item_ty, @$, nil (@$)); }
                          ;

/* [114] */
AtomicType                : QName
                            { ($$ = wire1 (p_atom_ty, @$, nil (@$)))
                                ->sem.qname = $1; }
                          ;

/* [115] */
KindTest                  : DocumentTest         { $$ = $1; }
                          | ElementTest          { $$ = $1; }
                          | AttributeTest        { $$ = $1; }
                          | SchemaElementTest    { $$ = $1; }
                          | SchemaAttributeTest  { $$ = $1; }
                          | PITest               { $$ = $1; }
                          | CommentTest          { $$ = $1; }
                          | TextTest             { $$ = $1; }
                          | AnyKindTest          { $$ = $1; }
                          ;

/* [116] */
AnyKindTest               : "node ()"
                            { ($$ = wire1 (p_node_ty, @$, nil (@$)))
                                ->sem.kind = p_kind_node; }
                          ;

/* [117] */
DocumentTest              : "document-node (" ")"
                            { ($$ = wire1 (p_node_ty, @$, nil (@$)))
                                ->sem.kind = p_kind_doc; }
                          | "document-node (" ElementTest ")"
                            { ($$ = wire1 (p_node_ty, @$, $2))
                                ->sem.kind = p_kind_doc; }
                          | "document-node (" SchemaElementTest ")"
                            { ($$ = wire1 (p_node_ty, @$, $2))
                                ->sem.kind = p_kind_doc; }
                          ;

/* [118] */
TextTest                  : "text (" ")"
                            { ($$ = wire1 (p_node_ty, @$, nil (@$)))
                                ->sem.kind = p_kind_text; }
                          ;

/* [119] */
CommentTest               : "comment (" ")"
                            { ($$ = wire1 (p_node_ty, @$, nil (@$)))
                                ->sem.kind = p_kind_comment; }
                          ;

/* [120] */
PITest                    : "processing-instruction (" ")"
                            { ($$ = wire1 (p_node_ty, @$, nil (@$)))
                                ->sem.kind = p_kind_pi; }
                          | "processing-instruction (" NCName ")"
                            { /* semantics for p-i(NCName) and
                               * p-i(StringLiteral) are identical. */
                              ($$ = wire1 (p_node_ty, @$,
                                           (c = leaf (p_lit_str, @2),
                                            c->sem.str = $2,
                                            c)))
                                ->sem.kind = p_kind_pi; }
                          | "processing-instruction (" StringLiteral ")"
                            { ($$ = wire1 (p_node_ty, @$,
                                           (c = leaf (p_lit_str, @2),
                                            c->sem.str = $2,
                                            c)))
                                ->sem.kind = p_kind_pi; }
                          ;

/* [121] */
AttributeTest             : "attribute (" ")"
                            { ($$ = wire1 (p_node_ty, @$, nil (@$)))
                                ->sem.kind = p_kind_attr; }
                          | "attribute (" AttribNameOrWildcard ")"
                            { ($$ = wire1 (p_node_ty, @$,
                                           wire2 (p_req_ty, @$, $2, nil (@$))))
                                ->sem.kind = p_kind_attr; }
                          | "attribute (" AttribNameOrWildcard "," TypeName ")"
                            { ($$ = wire1 (p_node_ty, @$,
                                           wire2 (p_req_ty, @$,
                                                  $2,
                                                  (c = leaf (p_named_ty, @4),
                                                   c->sem.qname = $4,
                                                   c))))
                                ->sem.kind = p_kind_attr; }
                          ;

/* [122] */
AttribNameOrWildcard      : AttributeName
                            { ($$ = leaf (p_req_name, @$))->sem.qname = $1; }
                          | "*"
                            { $$ = nil (@$); }
                          ;

/* [123] */
SchemaAttributeTest       : "schema-attribute (" AttributeDeclaration ")"
                            { ($$ = leaf (p_schm_attr, @$))->sem.qname = $2; }
                          ;

/* [124] */
AttributeDeclaration      : AttributeName { $$ = $1; }
                          ;

/* [125] */
ElementTest               : "element (" ")"
                            { ($$ = wire1 (p_node_ty, @$, nil (@$)))
                                ->sem.kind = p_kind_elem; }
                          | "element (" ElementNameOrWildcard ")"
                            { ($$ = wire1 (p_node_ty, @$,
                                           wire2 (p_req_ty, @$, $2, nil (@$))))
                                ->sem.kind = p_kind_elem; }
                          | "element (" ElementNameOrWildcard "," TypeName ")"
                            { ($$ = wire1 (p_node_ty, @$,
                                           wire2 (p_req_ty, @$,
                                                  $2,
                                                  (c = leaf (p_named_ty, @4),
                                                   c->sem.qname = $4,
                                                   c))))
                                ->sem.kind = p_kind_elem; }
                          | "element (" ElementNameOrWildcard ","
                              TypeName "?" ")"
                            { /* FIXME: We currently ignore the '?'.
                               *
                               * Semantics of the '?': Identical to semantics
                               * without, but additionally allows elements that
                               * have their `nilled' property set. (We don't
                               * implement `nilled' anyway.)
                               */
                              PFinfo_loc (OOPS_NOTICE, @5,
                                          "`?' modifier in element type test "
                                          "is not supported and will be "
                                          "ignored.");
                              ($$ = wire1 (p_node_ty, @$,
                                           wire2 (p_req_ty, @$,
                                                  $2,
                                                  (c = leaf (p_named_ty, @4),
                                                   c->sem.qname = $4,
                                                   c))))
                                ->sem.kind = p_kind_elem;
                            }
                          ;

/* [126] */
ElementNameOrWildcard     : ElementName
                            { ($$ = leaf (p_req_name, @$))->sem.qname = $1; }
                          | "*"
                            { $$ = nil (@$); }
                          ;

/* [127] */
SchemaElementTest         : "schema-element (" ElementDeclaration ")"
                            { ($$ = leaf (p_schm_elem, @$))->sem.qname = $2; }
                          ;

/* [128] */
ElementDeclaration        : ElementName { $$ = $1; }
                          ;

/* [129] */
AttributeName             : QName { $$ = $1; }
                          ;

/* [130] */
ElementName               : QName { $$ = $1; }
                          ;

/* [131] */
TypeName                  : QName { $$ = $1; }
                          ;

/* [132]  !W3C: IntegerLiteral is a terminal */
/* [133]  !W3C: DecimalLiteral is a terminal */
/* [134]  !W3C: DoubleLiteral is a terminal */
/* [135]  !W3C: StringLiteral is a terminal */
/* [136]  !W3C: PITarget is a terminal */

/* [137] */
VarName                   : QName
                            { ($$ = leaf (p_varref, @$))->sem.qname = $1; }
                          ;

/* [138]  !W3C: in Rule [60] */

/* [139]  !W3C: Don't need Digits: Is in ...Literal terminals */

/* [140]  !W3C: Predefined entities handled by scanner */

/* [141]  !W3C: CharRef is a terminal */

/* [142] */
EscapeQuot                : "\"\""  { $$ = '"'; }
                          ;
/* [143] */
EscapeApos                : "''"    { $$ = '\''; }
                          ;

/* [144]  !W3C: ElementContentChar is a terminal */
/* [145]  !W3C: QuotAttrContentChar is a terminal */
/* [146]  !W3C: AposAttrContentChar is a terminal */

/* !W3C: We don't support pragmas [147] [148] [149] */

/* !W3C: Comments are handled by the scanner [150] [151] */

/* [152]  !W3C: QName is a terminal */
/* [153]  !W3C: NCName is a terminal */
/* [154]  !W3C: S is a terminal */
/* [155]  !W3C: PFChar is a terminal */

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
                    p->child[1] = wire2 (p_locpath,
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
void pferror (const char *s)
{
    if (pftext && *pftext)
        PFlog ("%s on line %d (next token is `%s')",
                s, pflineno,
                pftext);
    else
        PFlog ("%s on line %d",
                s, pflineno);
}

char* pfinput = NULL; /* standard input of scanner, used by flex */
YYLTYPE pflloc; /* why ? */
extern void pfStart(char*);

/**
 * Parse an XQuery coming in on stdin (or whatever stdin might have
 * been dup'ed to)
 */
void
PFparse (char* input, PFpnode_t **r)
{
    pfStart(input);
#if YYDEBUG
    pfdebug = 1;
#endif

    /* initialisation of yylloc */
    pflloc.first_row = pflloc.last_row = 1;
    pflloc.first_col = pflloc.last_col = 0;

    if (pfparse ())
        PFoops (OOPS_PARSE, "XQuery parsing failed");

    *r = root;
}

/* vim:set shiftwidth=4 expandtab: */

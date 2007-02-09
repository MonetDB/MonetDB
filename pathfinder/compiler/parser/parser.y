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
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

%{
#include "pathfinder.h"

#include "parser.h"
#include "compile_interface.h"

/* PFarray_t */
#include "array.h"
#include "oops.h"

/* isspace */
#include <ctype.h>
/* strcmp */
#include <string.h>

#include "scanner.h"

/* PFstrdup() */
#include "mem.h"

/* include libxml2 library to parse module definitions from an URI */
#include "libxml/xmlIO.h"

/** root node of the parse tree */
static PFpnode_t *root;

/* temporay node memory */
static PFpnode_t *c, *c1;

static void add_to_module_wl (char* id, char *ns, char *uri);

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
 * fix-up the hole of an abstract syntax tree t and 
 * replace its c-th leaf by e:
 *
 *               t.root
 *                 / \
 *      t.hole --> -e--
 */
#define FIXUP(c,t,e) ((t).hole)->child[c] = (e)


/* scanner information to provide better error messages */
extern char         *pftext;
extern unsigned int  cur_col;
extern unsigned int  cur_row;

/**
 * True if we have been invoked via parse_module().
 *
 * For queries that contain "import module" instructions, we re-invoke
 * the parser for each imported module.  In that case we _only_ allow
 * modules to be read and refuse query scripts.
 */
static bool module_only = false;

#ifdef ENABLE_MILPRINT_SUMMER

/*
 * quasi-random number determined by module; used as base for scope 
 * and variable numbers in milprint_summer (so scope numbers of 
 * different modules that are compiled separately don't collide).
 */
static unsigned int module_base = 0;

/*
 * the number of functions declared in a query. Used in milprint_summer to
 * be able to suppress code generation for all functions defined
 * in modules;
 */
static int num_fun = 0;

#endif

/**
 * Module namespace we accept during parsing.
 *
 * If a module is imported, its associated namespace must match the
 * namespace given in the "import module" statement.  If this variable
 * is != NULL, we only accept modules with the given namespace.
 */
static char *req_module_ns = NULL;

/**
 * URI of the module that we are currently parsing.
 *
 * Only set for imported modules, NULL otherwise.
 */
static char *current_uri = NULL;

/**
 * Work list of modules that we have to load.
 *
 * Whenever we encounter an "import module" statement, we add an URI to
 * the list (if it is not in there already).  We process the work list
 * after parsing the main query file.
 */
static PFarray_t *modules = NULL;

/**
 * Each item in the work list is a id/namespace/URI triple.  The URI
 * denotes the URI to load the module from; the namespace is the
 * namespace that the module should be defined for (abort parsing
 * otherwise).
 */
typedef struct module_t {
    char *id;
    char *ns;
    char *uri;
} module_t;

/*
 * Check if the input string consists of whitespace only.
 * If this is the case and pres_boundary_space is set to false, the 
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
    long long int   num;
    dec             dec;
    double          dbl;
    char           *str;
    bool            bit;
    PFqname_raw_t   qname_raw;
    PFpnode_t      *pnode;
    struct phole_t  phole;
    PFptype_t       ptype;
    PFpaxis_t       axis;
    PFsort_t        mode;
    PFpoci_t        oci;
    PFarray_t      *buf;
    PFinsertmod_t   insert;
}

%token encoding_StringLiteral
%token StringLiteral

%token after                           "after"
%token ancestor_colon_colon            "ancestor::"
%token ancestor_or_self_colon_colon    "ancestor-or-self::"
%token and                             "and"
%token apos                            "'"
%token as                              "as"
%token ascending                       "ascending"
%token as_first_into                   "as first into"
%token as_last_into                    "as last into"
%token at_dollar                       "at $"
%token atsign                          "@"
%token attribute_colon_colon           "attribute::"
%token attribute_lbrace                "attribute {"
%token attribute_lparen                "attribute ("
%token before                          "before"
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
%token copy_ns_inherit                 "inherit"
%token copy_ns_noinherit               "no-inherit"
%token copy_ns_nopreserve              "no-preserve"
%token copy_ns_preserve                "preserve"
%token declare_base_uri                "declare base-uri"
%token declare_construction_preserve   "declare construction preserve"
%token declare_construction_strip      "declare construction strip"
%token declare_default_collation       "declare default collation"
%token declare_default_element         "declare default element"
%token declare_default_function        "declare default function"
%token declare_default_order           "declare default order"
%token declare_function                "declare function"
%token declare_updating_function       "declare updating function"
%token declare_copy_namespaces         "declare copy-namespaces"
%token declare_namespace               "declare namespace"
%token declare_ordering_ordered        "declare ordering ordered"
%token declare_ordering_unordered      "declare ordering unordered"
%token declare_variable_dollar         "declare variable $"
%token declare_boundary_space_preserve "declare boundary-space preserve"
%token declare_boundary_space_strip    "declare boundary-space strip"
%token declare_option                  "declare option"
%token declare_revalidation_lax        "declare revalidation lax"
%token declare_revalidation_skip       "declare revalidation skip"
%token declare_revalidation_strict     "declare revalidation strict"
%token default_                        "default"
%token default_element                 "default element"
%token descendant_colon_colon          "descendant::"
%token descendant_or_self_colon_colon  "descendant-or-self::"
%token descending                      "descending"
%token div_                            "div"
%token do_delete                       "do delete"
%token do_insert                       "do insert"
%token do_rename                       "do rename"
%token do_replace                      "do replace"
%token do_replace_value_of             "do replace value of"
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
%token empty_sequence                  "empty-sequence ()"
%token eq                              "eq"
%token equals                          "="
%token escape_apos                     "''"
%token escape_quot                     "\"\""
%token every_dollar                    "every $"
%token except                          "except"
%token excl_equals                     "!="
%token execute_at                      "execute at"
%token external_                       "external"
%token following_colon_colon           "following::"
%token following_sibling_colon_colon   "following-sibling::"
%token for_dollar                      "for $"
%token ge                              "ge"
%token greater_than                    ">"
%token greater_than_equal              ">="
%token gt                              "gt"
%token gt_gt                           ">>"
%token hash_paren                      "#)"
%token idiv                            "idiv"
%token if_lparen                       "if ("
%token import_module                   "import module"
%token import_schema                   "import schema"
%token in_                             "in"
%token instance_of                     "instance of"
%token intersect                       "intersect"
%token into                            "into"
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
%token modify                          "modify"
%token module_namespace                "module namespace"
%token namespace                       "namespace"
%token ne                              "ne"
%token node_lrparens                   "node ()"
%token or                              "or"
%token order_by                        "order by"
%token ordered_lbrace                  "ordered {"
%token paren_hash                      "(#"
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
/* Pathfinder extension: recursion */
%token recurse                         "recurse"
/* [BURKOWSKI] */
%token reject_narrow_colon_colon       "reject-narrow::"
%token reject_wide_colon_colon         "reject-wide::"
/* [/BURKOWKSI] */
%token return_                         "return"
%token rparen                          ")"
/* %token rparen_as                       ") as" */
%token satisfies                       "satisfies"
%token schema_attribute_lparen         "schema-attribute ("
%token schema_element_lparen           "schema-element ("
/* Pathfinder extension: recursion */
%token seeded_by                       "seeded by"
/* [BURKOWKSI] */
%token select_narrow_colon_colon       "select-narrow::"
%token select_wide_colon_colon         "select-wide::"
/* [/BURKOWKSI] */
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
%token to_                             "to"
%token transform_copy_dollar           "transform copy $"
%token treat_as                        "treat as"
%token typeswitch_lparen               "typeswitch ("
%token union_                          "union"
%token unordered_lbrace                "unordered {"
%token validate_lax_lbrace             "validate lax {"
%token validate_lbrace                 "validate {"
%token validate_strict_lbrace          "validate strict {"
%token where                           "where"
%token with                            "with"
/* Pathfinder extension: recursion */
%token with_dollar                     "with $"
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
%token EscapeApos
%token EscapeQuot
%token IntegerLiteral
%token NCName
%token NCName_Colon_Star
%token PITarget
%token PI_NCName_LBrace
%token PragmaContents
%token PredefinedEntityRef
%token QName
%token QName_LParen
%token S
%token Star_Colon_NCName
%token at_URILiteral

/*
 * The lexer sends an error token if it reads something that shouldn't
 * appear. (We replace flex' default action with this.)
 */
%token invalid_character

%type <str>
               "{{"
               "}}"
               at_URILiteral
               AttrContentChar
               AttributeValueContText_
               CharRef
               ElementContentChar
               ElementContentText_
               encoding_StringLiteral
               EscapeQuot
               EscapeApos
               NCName
               OptCollationStringLiteral_
               OptEncoding_
               PFChar
               PITarget
               PI_NCName_LBrace
               PragmaContents
               PredefinedEntityRef
               StringLiteral
               URILiteral

%type <qname_raw>
               AttributeDeclaration
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

%type <num>    
               IntegerLiteral
               OptAscendingDescending_
               OptEmptyGreatestLeast_

%type <dec>
               DecimalLiteral

%type <dbl>
               DoubleLiteral

%type <mode>
               OrderModifier

%type <axis>  
               AttributeAxis_
               BurkAxis_
               ForwardAxis
               ReverseAxis

%type <buf>  
               AttributeValueContTexts_
               Chars_
               ElementContentTexts_

%type <ptype>  
               DivOp_
               GeneralComp
               NodeComp
               SomeEvery_
               ValueComp

%type <pnode> 
               AbbrevAttribStep_
               AbbrevForwardStep
               AbbrevReverseStep
               AdditiveExpr
               AndExpr
               AnyKindTest
               AtomicType
               AttribNameOrWildcard
               AttribNodeTest
               AttribStep_
               AttributeTest
               AttributeValueCont_
               AttributeValueConts_
               AxisStep
               BaseURIDecl
               BoundarySpaceDecl
               BurkStep_
               CaseClause
               CastableExpr
               CastExpr
               CDataSection
               CDataSectionContents
               Characters_
               CommentTest
               ComparisonExpr
               CompAttrConstructor
               CompCommentConstructor
               CompDocConstructor
               CompElemConstructor
               CompPIConstructor
               CompTextConstructor
               ComputedConstructor
               ConstructionDecl
               Constructor
               ContentExpr
               ContextItemExpr
               CopyNamespacesDecl
               DefaultCollationDecl
               DefaultNamespaceDecl
               DeleteExpr
               DirAttribute_
               DirAttributeValue
               DirCommentConstructor
               DirCommentContents
               DirectConstructor
               DirElemConstructor
               DirElemContent
               DirElementContents_
               DirPIConstructor
               DirPIContents
               DocumentTest
               ElementNameOrWildcard
               ElementTest
               EmptyOrderDecl
               EnclosedExpr
               Expr
               ExprSingle
               ExtensionExpr
               FilterExpr
               FLWORExpr
               ForwardStep
               FuncArgList_
               FunctionCall
               FunctionDecl
               IfExpr
               InsertExpr
               InstanceofExpr
               IntersectExceptExpr
               ItemType
               KindTest
               LibraryModule
               Literal
               MainModule
               Module
               ModuleDecl
               MultiplicativeExpr
               NamespaceDecl
               NameTest
               NewNameExpr
               NodeTest
               NumericLiteral
               OptAsSequenceType_
               OptAtURILiterals_
               OptContentExpr_
               OptDollarVarName_
               OptDollarVarNameAs_
               OptFuncArgList_
               OptionDecl
               OptOrderByClause_
               OptParamList_
               OptParamTypeDeclaration_
               OptPositionalVar_
               OptPragmaExpr_
               OptTypeDeclaration_
               OptWhereClause_
               OrderByClause
               OrderedExpr
               OrderingModeDecl
               OrderSpecList
               OrExpr
               Param
               ParamList
               ParenthesizedExpr
               PathExpr
               PITest
               PositionalVar
               Pragma
               Pragmas_
               Predicate
               PrimaryExpr
               Prolog
               QuantifiedExpr
               QueryBody 
               RangeExpr
               RecursiveExpr                /* PF extension */
               RelativePathExpr
               RenameExpr
               ReplaceExpr
               RevalidationDecl
               ReverseStep
               SchemaAttributeTest
               SchemaElementTest
               SeedVar                      /* PF extension */
               SequenceType
               Setter
               SingleType
               SourceExpr
               StepExpr
               TargetExpr
               TextTest
               TransformBinding_
               TransformBindings_
               TransformExpr
               TreatExpr
               TypeDeclaration
               TypeswitchExpr
               UnaryExpr
               UnionExpr
               UnorderedExpr
               ValidateExpr
               ValueExpr
               VarBindings_
               VarDecl
               VarName
               VarRef
               WhereClause
               Wildcard
               XRPCCall               /* Pathfinder extension */

%type <phole> 
               CaseClauses_
               DirAttributeList
               ForClause
               ForLetClauses_
               ForLetClause_
               Import
               LetBindings_
               LetClause
               ModuleImport
               ModuleNS_
               ModuleNSWithoutPrefix
               ModuleNSWithPrefix
               PredicateList
               SchemaImport
               SchemaSrc_
               SetterImportAndNSDecls_
               URILiterals_
               VarFunDecls_
               VarPosBindings_

%type <oci>   
               OptOccurrenceIndicator_

%type <bit>   
               PreserveMode
               InheritMode

%type <insert>
               InsertLoc_

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
                              PFquery.encoding = "UTF-8"; }
                          | "xquery version" StringLiteral OptEncoding_
                            Separator
                            { PFquery.version = $2; PFquery.encoding = $3; 
                              if (strcmp (PFquery.version, "1.0")) {
                                  PFinfo_loc (OOPS_PARSE, @$,
                                      "only XQuery version '1.0' is supported");
                                  YYERROR;
                              }
                              if (strcmp (PFquery.encoding, "UTF-8")
                                  && strcmp (PFquery.encoding, "utf-8")) {
                                  PFinfo_loc (OOPS_PARSE, @$,
                                      "only XQueries in UTF-8 encoding are "
                                      "supported, not in '%s' encoding",
                                      PFquery.encoding);
                                  YYERROR;
                              }
                            }
                          ;

OptEncoding_              : /* empty */
                            { $$ = "UTF-8"; }
                          | encoding_StringLiteral
                            { $$ = $1; }
                          ;

/* [3] */
MainModule                : Prolog QueryBody
                            {
                              if (module_only)
                                  PFoops (OOPS_MODULEIMPORT,
                                          "\"import module\" references a "
                                          "query, not a module");
                              $$ = wire2 (p_main_mod, @$, $1, $2); }
                          ;

/* [4] */
LibraryModule             : ModuleDecl Prolog 
                            { $$ = wire2 (p_lib_mod, @$, $1, $2);
                              if (current_uri)
                                  $$->sem.str = PFstrdup (current_uri);
                            }
                          ;

/* [5] */
ModuleDecl                : "module namespace" NCName
                              "=" URILiteral Separator
                            {
                              if (req_module_ns && strcmp (req_module_ns, $4))
                                  PFoops (OOPS_MODULEIMPORT,
                                          "module namespace does not match "
                                          "import statement (`%s' vs. `%s')",
                                          $4, req_module_ns);

#ifdef ENABLE_MILPRINT_SUMMER
                              if (!module_only)
                                  module_base = 1;
#endif

                              ($$ = wire1 (p_mod_ns,
                                           @$,
                                           (c = leaf (p_lit_str, @4),
                                            c->sem.str = $4,
                                            c)))->sem.str = $2;
                            }
                          ;

/* [6] */
Prolog                    : SetterImportAndNSDecls_ VarFunDecls_
                            { $$ = $1.root;
                              $1.hole->child[1] = $2.root; }
                          ;

SetterImportAndNSDecls_   : /* empty */
                            { $$.root
                                  = $$.hole
                                  = wire2 (p_decl_imps, @$, nil (@$), nil (@$));
                            }
                          | DefaultNamespaceDecl Separator
                            SetterImportAndNSDecls_
                            { $$.root = wire2 (p_decl_imps, @$, $1, $3.root);
                              $$.hole = $3.hole; }
                          | Setter Separator SetterImportAndNSDecls_
                            { $$.root = wire2 (p_decl_imps, @$, $1, $3.root);
                              $$.hole = $3.hole; }
                          | NamespaceDecl Separator SetterImportAndNSDecls_
                            { $$.root = wire2 (p_decl_imps, @$, $1, $3.root);
                              $$.hole = $3.hole; }
                          | Import Separator SetterImportAndNSDecls_
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
                          | OptionDecl Separator VarFunDecls_
                            { $$.root = wire2 (p_decl_imps, @$, $1, $3.root);
                              $$.hole = $3.hole; }
                          ;

/* [7] */
Setter                    : BoundarySpaceDecl    { $$ = $1; }
                          | DefaultCollationDecl { $$ = $1; }
                          | BaseURIDecl          { $$ = $1; }
                          | ConstructionDecl     { $$ = $1; }
                          | OrderingModeDecl     { $$ = $1; }
                          | EmptyOrderDecl       { $$ = $1; }
                          | RevalidationDecl     { $$ = $1; }  /* XQUpdt */
                          | CopyNamespacesDecl   { $$ = $1; }
                          ;

/* [8] */
Import                    : SchemaImport     { $$ = $1; }
                          | ModuleImport     { $$ = $1; }
                          ;

/* [9] */
Separator                 : ";"
                          ;

/* [10] */
NamespaceDecl             : "declare namespace" NCName "=" URILiteral
                            { ($$ = wire1 (p_ns_decl, 
                                           @$,
                                           (c = leaf (p_lit_str, @4),
                                            c->sem.str = $4,
                                            c)))->sem.str = $2;
                            }
                          ;

/* [11] */
BoundarySpaceDecl         : "declare boundary-space preserve"
                            { ($$ = leaf (p_boundspc_decl, @$))->sem.tru = true;
                              PFquery.pres_boundary_space = true;
                            } 
                          | "declare boundary-space strip"
                            { ($$ = leaf (p_boundspc_decl,@$))->sem.tru = false;
                              PFquery.pres_boundary_space = false;
                            }
                          ;

/* [12] */
DefaultNamespaceDecl      : "declare default element" "namespace" URILiteral
                            { $$ = wire1 (p_ens_decl,
                                          @$,
                                          (c = leaf (p_lit_str, @3),
                                           c->sem.str = $3,
                                           c));
                            }
                          | "declare default function" "namespace" URILiteral
                            { $$ = wire1 (p_fns_decl,
                                          @$,
                                          (c = leaf (p_lit_str, @3),
                                           c->sem.str = $3,
                                           c));
                            }
                          ;

/* [13] */
OptionDecl                : "declare option" QName StringLiteral
                            {
                              /* FIXME:
                                 What could we do with options, actually? */
                              PFinfo (OOPS_NOTICE,
                                      "option %s will be ignored",
                                      PFqname_raw_str ($2));
                              $$ = nil (@$);
                            }
                          ;

/* [14]  !W3C: Done by the lexer */
OrderingModeDecl          : "declare ordering ordered"
                            { ($$ = leaf (p_ordering_mode,
                                          @$))->sem.tru = true;
                            }
                          | "declare ordering unordered"
                            { ($$ = leaf (p_ordering_mode,
                                          @$))->sem.tru = false;
                            }
                          ;

/* [15] */
EmptyOrderDecl            : "declare default order" "empty greatest"
                            { ($$ = leaf (p_def_order,
                                          @$))->sem.mode.empty = p_greatest;
                            }
                          | "declare default order" "empty least"
                            { ($$ = leaf (p_def_order,
                                          @$))->sem.mode.empty = p_least;
                            }
                          ;

/* [16]  !W3C: Done by the lexer */
CopyNamespacesDecl        : "declare copy-namespaces" PreserveMode ","
                            InheritMode
                            { $$ = leaf (p_copy_ns, @$);
                              $$->sem.copy_ns.preserve = $2;
                              $$->sem.copy_ns.inherit  = $4;
                            }
                          ;

/* [17] */
PreserveMode              : "preserve"    { $$ = true; }
                          | "no-preserve" { $$ = false; }
                          ;

/* [18] */
InheritMode               : "inherit"     { $$ = true; }
                          | "no-inherit"  { $$ = false; }
                          ;
                            
/* [19] */
DefaultCollationDecl      : "declare default collation" URILiteral
                            { $$ = wire1 (p_coll_decl,
                                          @$,
                                          (c = leaf (p_lit_str, @2),
                                           c->sem.str = $2,
                                           c));
                            }
                          ;

/* [20] */
BaseURIDecl               : "declare base-uri" URILiteral
                            { $$ = wire1 (p_base_uri,
                                          @$,
                                          (c = leaf (p_lit_str, @2),
                                           c->sem.str = $2,
                                           c));
                            }
                          ;

/* [21] / [22] */
SchemaImport              : "import schema" SchemaSrc_ OptAtURILiterals_
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

SchemaSrc_                : URILiteral
                            { $$.root = NULL;
                              ($$.hole = leaf (p_lit_str, @$))->sem.str = $1;
                            }
                          | "namespace" NCName "=" URILiteral
                            { ($$.root = wire1 (p_ns_decl, @$,
                                                (c = leaf (p_lit_str, @4),
                                                 c->sem.str = $4,
                                                 c)))->sem.str = $2;
                              ($$.hole = leaf (p_lit_str, @4))->sem.str = $4;
                            }
                          | "default element" "namespace" URILiteral
                            { $$.root = wire1 (p_ens_decl, @$,
                                               (c = leaf (p_lit_str, @3),
                                                c->sem.str = $3,
                                                c));
                              ($$.hole = leaf (p_lit_str, @3))->sem.str = $3;
                            }
                          ;

OptAtURILiterals_         : /* empty */
                            { $$ = nil (@$); }
                          | at_URILiteral URILiterals_
                            { $$ = wire2 (p_schm_ats, @$,
                                          (c = leaf (p_lit_str, @1),
                                           c->sem.str = $1,
                                           c),
                                          $2.root ? $2.root : nil (@$));
                            }
                          ;

URILiterals_              : /* empty */
                            { $$.root = $$.hole = NULL; }
                          | URILiterals_ "," URILiteral
                            {
                              if ($1.root) {
                                  $$.root = $1.root;
                                  FIXUP (1, $1,
                                         wire2 (p_schm_ats, @$,
                                                (c = leaf (p_lit_str, @3),
                                                 c->sem.str = $3,
                                                 c),
                                                nil (@3)));
                              }
                              else {
                                  $$.root = $$.hole
                                          = wire2 (p_schm_ats, @$,
                                                   (c = leaf (p_lit_str, @3),
                                                    c->sem.str = $3,
                                                    c),
                                                   nil (@$));
                              }
                            }
                          ;

/* [23] */
ModuleImport              : "import module" ModuleNS_ OptAtURILiterals_
                            { /* XQuery allows to merge a module import
                               * and an associated namespace declaration:
                               *
                               * import module namespace ns = "ns" [at "url"]
                               *
                               * which is equivalent to
                               *
                               * import module "ns" [at "url"]
                               * namespace ns = "ns" 
                               *
                               * We thus return the module import in $$.root
                               * and the namespace declaration in $$.hole
                               * ($2.root == NULL if no namespace decl given)
                               */

                              /* FIXME:
                               *   Does this code make sense?  We add the
                               *   module to the work list once for each
                               *   "at" URI given in the query.  This means
                               *   we load the module from each of the URIs
                               *   in turn?  Shouldn't we rather try each
                               *   of the URIs until we can successfully
                               *   load *one* of them?
                               */
                              for (c = $3;
                                   c->kind == p_schm_ats;
                                   c = c->child[1])
                                  add_to_module_wl (
                                      $2.root
                                          ? $2.root->sem.str : "", /* prefix */
                                      $2.hole->sem.str,            /* namespc */
                                      c->child[0]->sem.str);       /* at URI */

                              $$.root = wire2 (p_mod_imp, @$, $2.hole, $3);
                              $$.hole = $2.root;
                            }
                          ;

ModuleNS_                 : ModuleNSWithPrefix      { $$ = $1; }
                          | ModuleNSWithoutPrefix   { $$ = $1; }
                          ;

ModuleNSWithoutPrefix     : URILiteral
                            { $$.root = NULL;
                              ($$.hole = leaf (p_lit_str, @$))->sem.str = $1;
                            }
                          ;

ModuleNSWithPrefix        : "namespace" NCName "=" URILiteral
                            { ($$.root = wire1 (p_ns_decl, @$,
                                                (c = leaf (p_lit_str, @4),
                                                 c->sem.str = $4,
                                                 c)))->sem.str = $2;
                              ($$.hole = leaf (p_lit_str, @4))->sem.str = $4;
                            }
                          ;


/* [24] */
/* !W3C: Use the VarName non-terminal (not QName) */
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

/* [25]  !W3C: Done by the lexer */
ConstructionDecl          : "declare construction preserve"
                            { ($$ = leaf (p_constr_decl, @$))->sem.tru = true; }
                          | "declare construction strip"
                            { ($$ = leaf (p_constr_decl, @$))->sem.tru = false;}
                          ;

/* [26] */
FunctionDecl              : "declare function" QName_LParen
                            OptParamList_ OptAsSequenceType_ EnclosedExpr
                            { c = wire2 (p_fun_decl, @$,
                                         wire2 (p_fun_sig, loc_rng (@2, @4),
                                                $3, $4),
                                         $5);
                              c->sem.qname_raw = $2;
                              $$ = c;

#ifdef ENABLE_MILPRINT_SUMMER
                              num_fun++;
                              if (module_base)
                                  module_base
                                      = 1 | (module_base ^ (unsigned long) $$);
#endif
                            }
                          | "declare function" QName_LParen
                            OptParamList_ OptAsSequenceType_ "external"
                            { ($$ = wire2 (p_fun_decl, @$,
                                           wire2 (p_fun_sig, loc_rng (@2, @4),
                                                  $3, $4),
                                           leaf (p_external, @5)))
                                   ->sem.qname_raw = $2;
#ifdef ENABLE_MILPRINT_SUMMER
                              if (module_base)
                                  module_base
                                      = 1 | (module_base ^ (unsigned long) $$);
#endif
                            }
/* W3C Update Facility */ | "declare updating function" QName_LParen
                            OptParamList_ ")" EnclosedExpr
                            { c = wire2 (p_fun_decl, @$,
                                         wire2 (p_fun_sig, loc_rng (@2, @4),
                                                $3, 
                                                (c1 = wire1 (
                                                       p_seq_ty, @$,
                                                       wire1 (p_stmt_ty, @$,
                                                              nil (@4))),
                                                 c1->sem.oci = p_zero_or_more,
                                                 c1)),
                                         $5);
                              c->sem.qname_raw = $2;
                              $$ = c;

#ifdef ENABLE_MILPRINT_SUMMER
                              num_fun++;
                              if (module_base)
                                  module_base
                                      = 1 | (module_base ^ (unsigned long) $$);
#endif
                            }
                          | "declare updating function" QName_LParen
                            OptParamList_ ")" "external"
                            { ($$ = wire2 (p_fun_decl, @$,
                                           wire2 (p_fun_sig, loc_rng (@2, @4),
                                                  $3,
                                                  (c = wire1 (
                                                         p_seq_ty, @$,
                                                         wire1 (p_stmt_ty, @$,
                                                                nil (@4))),
                                                   c->sem.oci = p_zero_or_more,
                                                   c)),
                                           leaf (p_external, @5)))
                                   ->sem.qname_raw = $2;

#ifdef ENABLE_MILPRINT_SUMMER
                              if (module_base)
                                  module_base
                                      = 1 | (module_base ^ (unsigned long) $$);
#endif
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
                          | ")" "as" SequenceType  { $$ = $3; }
                          ;

/* [27] */
ParamList                 : Param
                            { $$ = wire2 (p_params, @$, $1, nil (@$)); }
                          | Param "," ParamList
                            { $$ = wire2 (p_params, @$, $1, $3); }
                          ;

/* [28] */
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


/* [29] */
EnclosedExpr              : "{" Expr "}" { $$ = $2; }
                          ;

/* [30] */
QueryBody                 : Expr { $$ = $1; }
                          ;

/* [31] */
Expr                      : ExprSingle
                            { $$ = wire2 (p_exprseq, @$,
                                          $1,
                                          leaf (p_empty_seq, @1)); }
                          | ExprSingle "," Expr
                            { $$ = wire2 (p_exprseq, @$, $1, $3); }
                          ;

/* [32] */
ExprSingle                : FLWORExpr       { $$ = $1; }
                          | QuantifiedExpr  { $$ = $1; }
                          | TypeswitchExpr  { $$ = $1; }
                          | IfExpr          { $$ = $1; }
                          | InsertExpr      { $$ = $1; }
                          | DeleteExpr      { $$ = $1; }
                          | RenameExpr      { $$ = $1; }
                          | ReplaceExpr     { $$ = $1; }
                          | TransformExpr   { $$ = $1; }
                          | OrExpr          { $$ = $1; }
                          | RecursiveExpr   { $$ = $1; }
                          ;

/* [33] */
FLWORExpr                 : ForLetClauses_ OptWhereClause_ OptOrderByClause_
                            "return" ExprSingle
                            { $$ = wire2 (p_flwr, @$, $1.root,
                                     wire2 (p_where, loc_rng (@2, @5), $2,
                                       wire2 (p_ord_ret, loc_rng (@3, @5), $3,
                                         $5)));
                            }
                          ;

ForLetClauses_            : ForLetClause_
                            { $$ = $1; }
                            /* { $$ = $1; } */
                          | ForLetClauses_ ForLetClause_
                            { FIXUP (1, $1, $2.root);
                              $$.hole = $2.hole; $$.root = $1.root; }
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

/* [34] */
ForClause                 : "for $" VarPosBindings_ { $$ = $2; }
                          ;

VarPosBindings_           : VarName OptTypeDeclaration_ OptPositionalVar_
                              "in" ExprSingle
                            { $$.root = $$.hole = 
                                wire2 (p_binds, @$,
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
                            { $$.hole = $8.hole;
                              $$.root = 
                                wire2 (p_binds, @$,
                                  wire2 (p_bind, @$,
                                    wire2 (p_vars, loc_rng (@1, @3),
                                      wire2 (p_var_type, loc_rng (@1, @2),
                                             $1,
                                             $2),
                                      $3),
                                    $5),
                                  $8.root);
                            }
                          ;

OptPositionalVar_         : /* empty */     { $$ = nil (@$); }
                          | PositionalVar   { $$ = $1; }
                          ;

/* [35] */
PositionalVar             : "at $" VarName  { $$ = $2; }
                          ;

/* [36] */
LetClause                 : "let $" LetBindings_ { $$ = $2; }
                          ;

LetBindings_              : VarName OptTypeDeclaration_ ":=" ExprSingle
                            { $$.root = $$.hole = 
                                wire2 (p_binds, @$,
                                  wire2 (p_let, @$,
                                    wire2 (p_var_type, loc_rng (@1, @2),
                                      $1, $2),
                                    $4),
                                  nil (@$));
                            }
                          | VarName OptTypeDeclaration_ ":=" ExprSingle
                              "," "$" LetBindings_
                            { $$.hole = $7.hole;
                              $$.root = 
                                wire2 (p_binds, @$,
                                  wire2 (p_let, @$,
                                    wire2 (p_var_type, loc_rng (@1, @2),
                                      $1, $2),
                                    $4),
                                  $7.root);
                            }
                          ;

/* [37] */
WhereClause               : "where" ExprSingle { $$ = $2; }
                          ;

/* [38] */
OrderByClause             : "order by" OrderSpecList
                            {($$ = wire1 (p_orderby, @$, $2))->sem.tru = false;}
                          | "stable order by" OrderSpecList
                            {($$ = wire1 (p_orderby, @$, $2))->sem.tru = true;}
                          ;

/* [39] / [40] */
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

/* [41] */
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

/* [42] */
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

/* [43] */
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

/* [44] */
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

/* [45] */
IfExpr                    : "if (" Expr ")" "then" ExprSingle "else" ExprSingle
                            { $$ = wire2 (p_if, @$,
                                          $2,
                                          wire2 (p_then_else, loc_rng (@4, @7),
                                                 $5, $7));
                            }
                          ;

/* [46] */
OrExpr                    : AndExpr { $$ = $1; }
                          | AndExpr "or" OrExpr
                            { $$ = wire2 (p_or, @$, $1, $3); }
                          ;

/* [47] */
AndExpr                   : ComparisonExpr { $$ = $1; }
                          | ComparisonExpr "and" AndExpr
                            { $$ = wire2 (p_and, @$, $1, $3); }
                          ;

/* [48] */
ComparisonExpr            : RangeExpr { $$ = $1; }
                          | RangeExpr ValueComp RangeExpr
                            { $$ = wire2 ($2, @$, $1, $3); }
                          | RangeExpr GeneralComp RangeExpr
                            { $$ = wire2 ($2, @$, $1, $3); }
                          | RangeExpr NodeComp RangeExpr
                            { $$ = wire2 ($2, @$, $1, $3); }
                          ;

/* [49] */
RangeExpr                 : AdditiveExpr { $$ = $1; }
                          | AdditiveExpr "to" AdditiveExpr
                            { $$ = wire2 (p_range, @$, $1, $3); }
                          ;

/* [50] */
AdditiveExpr              : MultiplicativeExpr { $$ = $1; }
                          | MultiplicativeExpr "+" AdditiveExpr
                            { $$ = wire2 (p_plus, @$, $1, $3); }
                          | MultiplicativeExpr "-" AdditiveExpr
                            { $$ = wire2 (p_minus, @$, $1, $3); }
                          ;

/* [51] */
MultiplicativeExpr        : UnionExpr { $$ = $1; }
                          | UnionExpr DivOp_ MultiplicativeExpr
                            { $$ = wire2 ($2, @$, $1, $3); }
                          ;

DivOp_                    : "*"    { $$ = p_mult; }
                          | "div"  { $$ = p_div; }
                          | "idiv" { $$ = p_idiv; }
                          | "mod"  { $$ = p_mod; }
                          ;

/* [52] */
UnionExpr                 : IntersectExceptExpr { $$ = $1; }
                          | IntersectExceptExpr "union" UnionExpr
                            { $$ = wire2 (p_union, @$, $1, $3); }
                          | IntersectExceptExpr "|" UnionExpr
                            { $$ = wire2 (p_union, @$, $1, $3); }
                          ;

/* [53] */
IntersectExceptExpr       : InstanceofExpr { $$ = $1; }
                          | InstanceofExpr "intersect" IntersectExceptExpr
                            { $$ = wire2 (p_intersect, @$, $1, $3); }
                          | InstanceofExpr "except" IntersectExceptExpr
                            { $$ = wire2 (p_except, @$, $1, $3); }
                          ;

/* [54] */
InstanceofExpr            : TreatExpr { $$ = $1; }
                          | TreatExpr "instance of" SequenceType
                            { $$ = wire2 (p_instof, @$, $1, $3); }
                          ;

/* [55] */
TreatExpr                 : CastableExpr { $$ = $1; }
                          | CastableExpr "treat as" SequenceType
                            { $$ = wire2 (p_treat, @$, $1, $3); }
                          ;

/* [56] */
CastableExpr              : CastExpr { $$ = $1; }
                          | CastExpr "castable as" SingleType
                            { $$ = wire2 (p_castable, @$, $1, $3); }
                          ;

/* [57] */
CastExpr                  : UnaryExpr { $$ = $1; }
                          | UnaryExpr "cast as" SingleType
                            { $$ = wire2 (p_cast, @$, $1, $3); }
                          ;

/* [58] */
UnaryExpr                 : ValueExpr     { $$ = $1; }
                          | "-" UnaryExpr { $$ = wire1 (p_uminus, @$, $2); }
                          | "+" UnaryExpr { $$ = wire1 (p_uplus, @$, $2); }
                          ;

/* [59] */
ValueExpr                 : ValidateExpr   { $$ = $1; }
                          | PathExpr       { $$ = flatten_locpath ($1, NULL); }
                          | ExtensionExpr  { $$ = $1; }
                          ;

/* [60] */
GeneralComp               : "="  { $$ = p_eq; }
                          | "!=" { $$ = p_ne; }
                          | "<"  { $$ = p_lt; }
                          | "<=" { $$ = p_le; }
                          | ">"  { $$ = p_gt; }
                          | ">=" { $$ = p_ge; }
                          ;

/* [61] */
ValueComp                 : "eq" { $$ = p_val_eq; }
                          | "ne" { $$ = p_val_ne; }
                          | "lt" { $$ = p_val_lt; }
                          | "le" { $$ = p_val_le; }
                          | "gt" { $$ = p_val_gt; }
                          | "ge" { $$ = p_val_ge; }
                          ;

/* [62] */
NodeComp                  : "is" { $$ = p_is; }
                          | "<<" { $$ = p_ltlt; }
                          | ">>" { $$ = p_gtgt; }
                          ;

/* [63] / [64] */
ValidateExpr              : "validate {" Expr "}"
                            { /* No validation mode means `strict'
                                 (W3C XQuery Sect. 3.13). */
                              ($$ = wire1 (p_validate, @$, $2))
                                ->sem.tru = true; }
                          | "validate lax {" Expr "}"
                            { ($$ = wire1 (p_validate, @$, $2))
                                ->sem.tru = false; }
                          | "validate strict {" Expr "}"
                            { ($$ = wire1 (p_validate, @$, $2))
                                ->sem.tru = true; }
                          ;

/* [65] */
ExtensionExpr             : Pragmas_ "{" OptPragmaExpr_ "}"
                            { $$ = p_wire2 (p_ext_expr, @$, $1, $3); }
                          ;

Pragmas_                  : Pragma
                            { $$ = p_wire2 (p_pragmas, @$, $1, nil (@$)); }
                          | Pragma Pragmas_
                            { $$ = p_wire2 (p_pragmas, @$, $1, $2); }
                          ;

OptPragmaExpr_            : /* empty */  { $$ = nil(@$); }
                          | Expr         { $$ = $1; }
                          ;

/* [66] */
Pragma                    : "(#" S QName PragmaContents "#)"
                            { $$ = p_leaf (p_pragma, @$);
                              $$->sem.pragma.qn.qname_raw = $3;
                              $$->sem.pragma.content   = $4;
                            }
                          ;

/* [67] is a terminal in Pathfinder */

/* [68] */
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

/* [69] */
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

/* [70] */
StepExpr                  : FilterExpr  { $$ = $1; }
                          | AxisStep    { $$ = $1; }
                          ;

/* [71]  !W3C: Factored out attribute step */
AxisStep                  : ReverseStep
                            { $$ = $1; }
                          | ReverseStep PredicateList
                            { FIXUP (0, $2, $1); $$ = $2.root; }
                          | ForwardStep
                            { $$ = $1; }
                          | ForwardStep PredicateList
                            { FIXUP (0, $2, $1); $$ = $2.root; }
                          | AttribStep_
                            { $$ = $1; }
                          | AttribStep_ PredicateList
                            { FIXUP (0, $2, $1); $$ = $2.root; }
/* [BURKOWSKI] */
                          | BurkStep_
                            { $$ = $1; }
                          | BurkStep_ PredicateList
                            { FIXUP (0, $2, $1); $$ = $2.root; }
/* [/BURKOWSKI] */
                          ;

/* [72] */
ForwardStep               : ForwardAxis NodeTest
                            { ($$ = wire1 (p_step, @$, $2))->sem.axis = $1; }
                          | AbbrevForwardStep
                            { $$ = $1; }
                          ;

/* [73] */
/* !W3C: attribute factored out */
ForwardAxis               : "child::"              { $$ = p_child; }
                          | "descendant::"         { $$ = p_descendant; }
                          | "self::"               { $$ = p_self; }
                          | "descendant-or-self::" { $$ = p_descendant_or_self;}
                          | "following-sibling::"  { $$ = p_following_sibling; }
                          | "following::"          { $$ = p_following; }
                          ;

/* [74] */
/* !W3C: attribute factored out */
AbbrevForwardStep         : NodeTest
                            { ($$ = wire1 (p_step,
                                             @$,
                                             $1))->sem.axis = p_child;
                            }
                          ;

/* [75] */
ReverseStep               : ReverseAxis NodeTest
                            { ($$ = wire1 (p_step, @$, $2))->sem.axis = $1; }
                          | AbbrevReverseStep
                            { $$ = $1; }
                          ;

/* [76] */
ReverseAxis               : "parent::"             { $$ = p_parent; }
                          | "ancestor::"           { $$ = p_ancestor; }
                          | "preceding-sibling::"  { $$ = p_preceding_sibling; }
                          | "preceding::"          { $$ = p_preceding; }
                          | "ancestor-or-self::"   { $$ = p_ancestor_or_self; }
                          ;

/* [77] */
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

/* !W3C: Burkowski steps are a Pathfinder extension */
BurkStep_                 : BurkAxis_ NodeTest
                            { /* Burkowski Axis (can only be used if
                                 corresponding compiler flag is set) */
                              ($$ = wire1 (p_step, @$, $2))->sem.axis = $1; }
                          ;

BurkAxis_                 : "select-narrow::"
                            { 
                              if (!PFstate.standoff_axis_steps) {
                                PFoops (OOPS_PARSE,
                                      "invalid character "
                                      "(StandOff was not enabled)");
                              } else {
                                $$ = p_select_narrow; 
                              }
                            }
                          | "select-wide::"
                            { 
                              if (!PFstate.standoff_axis_steps) {
                                PFoops (OOPS_PARSE,
                                      "invalid character "
                                      "(StandOff was not enabled)");
                              } else {
                                $$ = p_select_wide; 
                              }
                            }
                          | "reject-narrow::"
                            { 
                              if (!PFstate.standoff_axis_steps) {
                                PFoops (OOPS_PARSE,
                                      "invalid character "
                                      "(StandOff was not enabled)");
                              } else {
                                $$ = p_reject_narrow; 
                              }
                            }
                          | "reject-wide::"
                            { 
                              if (!PFstate.standoff_axis_steps) {
                                PFoops (OOPS_PARSE,
                                      "invalid character "
                                      "(StandOff was not enabled)");
                              } else {
                                $$ = p_reject_wide; 
                              }
                            }
                          ;


/* [78] */
NodeTest                  : KindTest
                            { $$ = $1; }
                          | NameTest
                            { ($$ = wire1 (p_node_ty, @$, $1))
                                ->sem.kind = p_kind_elem; }
                          ;

/* [79] */
NameTest                  : QName
                            { $$ = wire2 (p_req_ty, @$,
                                          (c = leaf (p_req_name, @$),
                                           c->sem.qname_raw = $1,
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

/* [80] */
Wildcard                  : "*"
                            { $$ = wire2 (p_req_ty, @$, nil (@$), nil (@$)); }
                          | NCName_Colon_Star
                            { $$ = wire2 (p_req_ty, @$,
                                          (c = leaf (p_req_name, @$),
                                           c->sem.qname_raw = $1,
                                           c),
                                          nil (@$)); }
                          | Star_Colon_NCName
                            { $$ = wire2 (p_req_ty, @$,
                                          (c = leaf (p_req_name, @$),
                                           c->sem.qname_raw = $1,
                                           c),
                                          nil (@$)); }
                          ;

/* [81] */
FilterExpr                : PrimaryExpr { $$ = $1; }
                          | PrimaryExpr PredicateList
                            { FIXUP (0, $2, $1); $$ = $2.root; }
                          ;

/* [82] */
PredicateList             : Predicate
                            { $$.root = $$.hole = wire2 (p_pred, @1, NULL, $1);}
                          | Predicate PredicateList
                            { $$.hole
                                  = $2.hole->child[0]
                                  = wire2 (p_pred, @1, NULL, $1);
                              $$.root = $2.root;
                            }
                          ;

/* [83] */
Predicate                 : "[" Expr "]"      { $$ = $2; }
                          ;

/* [84] */
PrimaryExpr               : Literal           { $$ = $1; }
                          | VarRef            { $$ = $1; }
                          | ParenthesizedExpr { $$ = $1; }
                          | ContextItemExpr   { $$ = $1; }
                          | FunctionCall      { $$ = $1; }
                          | OrderedExpr       { $$ = $1; }
                          | UnorderedExpr     { $$ = $1; }
                          | Constructor       { $$ = $1; }
                          | XRPCCall          { $$ = $1; }  /* PF extension */
                          ;

/* [85] */
Literal                   : NumericLiteral { $$ = $1; }
                          | StringLiteral
                            { ($$ = leaf (p_lit_str, @$))->sem.str = $1; }
                          ;

/* [86] */
NumericLiteral            : IntegerLiteral
                            { ($$ = leaf (p_lit_int, @$))->sem.num = $1; }
                          | DecimalLiteral
                            { ($$ = leaf (p_lit_dec, @$))->sem.dec = $1; }
                          | DoubleLiteral
                            { ($$ = leaf (p_lit_dbl, @$))->sem.dbl = $1; }
                          ;

/* [87] */
VarRef                    : "$" VarName   { $$ = $2; }
                          ;

/* [88] */
VarName                   : QName
                            { ($$ = leaf (p_varref, @$))->sem.qname_raw = $1; }
                          ;

/* [89] */
ParenthesizedExpr         : "(" ")"       { $$ = leaf (p_empty_seq, @$); }
                          | "(" Expr ")"  { $$ = $2; }
                          ;

/* [90] */
ContextItemExpr           : "." { $$ = leaf (p_dot, @$); }
                          ;

/* [91] */
OrderedExpr               : "ordered {" Expr "}"
                            { $$ = wire1 (p_ordered, @$, $2); }
                          ;

/* [92] */
UnorderedExpr             : "unordered {" Expr "}"
                            { $$ = wire1 (p_unordered, @$, $2); }
                          ;

/* [93] */
FunctionCall              : QName_LParen OptFuncArgList_ ")"
                            { c = wire1 (p_fun_ref, @$, $2);
                              /* FIXME:
                               *   This is not the parser's job!
                               *   Do this during function resolution!
                               */
                              c->sem.qname_raw = $1;
                              $$ = c;
                            }
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

/* [94] */
Constructor               : DirectConstructor   { $$ = $1; }
                          | ComputedConstructor { $$ = $1; }
                          ;

/* [95] */
DirectConstructor         : DirElemConstructor    { $$ = $1; }
                          | DirCommentConstructor { $$ = $1; }
                          | DirPIConstructor      { $$ = $1; }
                          ;

/* [96] */
DirElemConstructor        : "<" QName DirAttributeList "/>"
                            { if ($3.root)
                                $3.hole->child[1] = leaf (p_empty_seq, @3);
                              else
                                $3.root = leaf (p_empty_seq, @3);

                              $$ = wire2 (p_elem,
                                          @$,
                                          (c = leaf (p_tag, @2),
                                           c->sem.qname_raw = $2,
                                           c),
                                          $3.root);
                            }
                          | "<" QName DirAttributeList ">"
                            DirElementContents_
                            "</" QName OptS_ ">"
                            { /* XML well-formedness check:
                               * start and end tag must match
                               */ 
                              if (PFqname_raw_eq ($2, $7)) {
                                PFinfo_loc (OOPS_TAGMISMATCH, @$,
                                            "<%s> and </%s>",
                                            PFqname_raw_str ($2),
                                            PFqname_raw_str ($7));
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
                                            c->sem.qname_raw = $2,
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

/* [97] */
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
                                           c->sem.qname_raw = $1,
                                           c), $5);
                            }
                          ;

/* [98] */
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
                            {   /*
                                 * initialize new dynamic array and insert
                                 * one (UTF-8) character
                                 */
                                $$ = PFarray (sizeof (char));
                                while (*($1)) {
                                    *((char *) PFarray_add ($$)) = *($1);
                                    ($1)++;
                                }
                            }
                          | AttributeValueContTexts_ AttributeValueContText_
                            {   /* append one (UTF-8) charater to array */
                                while (*($2)) {
                                    *((char *) PFarray_add ($1)) = *($2);
                                    ($2)++;
                                }
                            }
                          ;

AttributeValueContText_   : AttrContentChar      { $$ = $1; }
                          | EscapeQuot           { $$ = "\""; }
                          | EscapeApos           { $$ = "'"; }
                          | PredefinedEntityRef  { $$ = $1; }
                          | CharRef              { $$ = $1; }
                          | "{{"                 { $$ = "{"; }
                          | "}}"                 { $$ = "}"; }
                          ;

/* [101] */
/* !W3C: We don't have a CommonContent non-terminal. */
DirElemContent            : DirectConstructor      { $$ = $1; }
                          | CDataSection           { $$ = $1; }
                          | ElementContentTexts_
                            { /* add trailing '\0' to string */
                              *((char *) PFarray_add ($1)) = '\0';
                                             
                              /* xmlspace handling */
                              if ((! PFquery.pres_boundary_space) &&
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
                          | EnclosedExpr  { $$ = $1; }
                          ;

ElementContentTexts_      : ElementContentText_
                            {   /*
                                 * initialize new dynamic array and insert
                                 * one (UTF-8) character
                                 */
                                $$ = PFarray (sizeof (char));
                                while (*($1)) {
                                    *((char *) PFarray_add ($$)) = *($1);
                                    ($1)++;
                                }
                            }
                          | ElementContentTexts_ ElementContentText_
                            {   /* append one (UTF-8) charater to array */
                                while (*($2)) {
                                    *((char *) PFarray_add ($1)) = *($2);
                                    ($2)++;
                                }
                                $$ = $1;
                            }
                          ;

ElementContentText_       : ElementContentChar    { $$ = $1; }
                          | PredefinedEntityRef   { $$ = $1; }
                          | CharRef               { $$ = $1; }
                          | "{{"                  { $$ = "{"; }
                          | "}}"                  { $$ = "}"; }
                          ;

/* [103] */
DirCommentConstructor     : "<!--" DirCommentContents "-->"
                            { $$ = wire1 (p_comment, @$, $2); }
                          ;

/* [104] */
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
                            {   /* append one (UTF-8) charater to array */
                                while (*($2)) {
                                    *((char *) PFarray_add ($1)) = *($2);
                                    ($2)++;
                                }
                                $$ = $1;
                            }
                          ;

/* [105] */
DirPIConstructor          : "<?" PITarget DirPIContents "?>"
                            { 
                              if ((strlen ($2) == 3)
                                   && ($2[0] == 'x' || $2[0] == 'X')
                                   && ($2[1] == 'm' || $2[1] == 'M')
                                   && ($2[2] == 'l' || $2[2] == 'L')) {
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

/* [106] */
DirPIContents             : /* empty */   { $$ = nil (@$); }
                          | S Characters_ { $$ = $2; }
                          ;

/* [107] */
CDataSection              : "<![CDATA[" CDataSectionContents "]]>"
                            { $$ = wire1 (p_text, @$,
                                          wire2 (p_exprseq, @2, $2,
                                                 leaf (p_empty_seq, @2))); }
                          ;

/* [108] */
CDataSectionContents      : Characters_ { $$ = $1; }
                          ;

/* [109] */
ComputedConstructor       : CompDocConstructor      { $$ = $1; }
                          | CompElemConstructor     { $$ = $1; }
                          | CompAttrConstructor     { $$ = $1; }
                          | CompTextConstructor     { $$ = $1; }
                          | CompCommentConstructor  { $$ = $1; }
                          | CompPIConstructor       { $$ = $1; }
                          ;

/* [110] */
CompDocConstructor        : "document {" Expr "}"
                            { $$ = wire1 (p_doc, @$, $2); }
                          ;

/* [111] */
CompElemConstructor       : Element_QName_LBrace OptContentExpr_ "}"
                            { $$ = wire2 (p_elem, @$,
                                          (c = leaf (p_tag, @1),
                                           c->sem.qname_raw = $1,
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

/* [112] */
ContentExpr               : Expr   { $$ = $1; }
                          ;

/* [113]  !W3C: OptContentExpr_ (symmetric to CompElemConstructor) */
CompAttrConstructor       : Attribute_QName_LBrace OptContentExpr_ "}"
                            { $$ = wire2 (p_attr, @$,
                                          (c = leaf (p_tag, @1),
                                           c->sem.qname_raw = $1,
                                           c),
                                          wire2 (p_contseq, @2, $2,
                                                 leaf (p_empty_seq, @2)));
                            }
                          | "attribute {" Expr "}" "{" OptContentExpr_ "}"
                            { $$ = wire2 (p_attr, @$, $2,
                                          wire2 (p_contseq, @5, $5,
                                                 leaf (p_empty_seq, @5))); }
                          ;

/* [114] */
CompTextConstructor       : "text {" Expr "}"
                            { $$ = wire1 (p_text, @$, $2); }
                          ;

/* [115] */
CompCommentConstructor    : "comment {" Expr "}"
                            { $$ = wire1 (p_comment, @$, $2); }
                          ;

/* [116] */
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

/* [117] */
SingleType                : AtomicType
                            { ($$ = wire1 (p_seq_ty, @$, $1))
                                ->sem.oci = p_one; }
                          | AtomicType "?"
                            { ($$ = wire1 (p_seq_ty, @$, $1))
                                ->sem.oci = p_zero_or_one; }
                          ;

/* [118] */
TypeDeclaration           : "as" SequenceType { $$ = $2; }
                          ;

/* [119] */
SequenceType              : "empty-sequence ()"
                            { ($$ = wire1 (p_seq_ty, @$, leaf (p_empty_ty, @$)))
                                ->sem.oci = p_one; }
                          | ItemType OptOccurrenceIndicator_
                            { ($$ = wire1 (p_seq_ty, @$, $1))->sem.oci = $2; }
                          ;

/* [120] */
/* !W3C: Drafts use OccurrenceIndicator? in Rule [119] */
OptOccurrenceIndicator_   : /* empty */   { $$ = p_one; }
                          | "?"           { $$ = p_zero_or_one; }
                          | "*"           { $$ = p_zero_or_more; }
                          | "+"           { $$ = p_one_or_more; }
                          ;

/* [121] */
ItemType                  : KindTest   { $$ = $1; }
                          | "item ()"  { $$ = wire1 (p_item_ty, @$, nil (@$)); }
                          | AtomicType { $$ = $1; }
                          ;

/* [122] */
AtomicType                : QName
                            { ($$ = wire1 (p_atom_ty, @$, nil (@$)))
                                ->sem.qname_raw = $1; }
                          ;

/* [123] */
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

/* [124] */
AnyKindTest               : "node ()"
                            { ($$ = wire1 (p_node_ty, @$, nil (@$)))
                                ->sem.kind = p_kind_node; }
                          ;

/* [125] */
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

/* [126] */
TextTest                  : "text (" ")"
                            { ($$ = wire1 (p_node_ty, @$, nil (@$)))
                                ->sem.kind = p_kind_text; }
                          ;

/* [127] */
CommentTest               : "comment (" ")"
                            { ($$ = wire1 (p_node_ty, @$, nil (@$)))
                                ->sem.kind = p_kind_comment; }
                          ;

/* [128] */
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

/* [129] */
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
                                                   c->sem.qname_raw = $4,
                                                   c))))
                                ->sem.kind = p_kind_attr; }
                          ;

/* [130] */
AttribNameOrWildcard      : AttributeName
                            { ($$ = leaf (p_req_name,@$))->sem.qname_raw = $1; }
                          | "*"
                            { $$ = nil (@$); }
                          ;

/* [131] */
SchemaAttributeTest       : "schema-attribute (" AttributeDeclaration ")"
                            { ($$ = leaf (p_schm_attr,@$))->sem.qname_raw = $2;}
                          ;

/* [132] */
AttributeDeclaration      : AttributeName { $$ = $1; }
                          ;

/* [133] */
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
                                                   c->sem.qname_raw = $4,
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
                                                   c->sem.qname_raw = $4,
                                                   c))))
                                ->sem.kind = p_kind_elem;
                            }
                          ;

/* [134] */
ElementNameOrWildcard     : ElementName
                            { ($$ = leaf (p_req_name,@$))->sem.qname_raw = $1; }
                          | "*"
                            { $$ = nil (@$); }
                          ;

/* [135] */
SchemaElementTest         : "schema-element (" ElementDeclaration ")"
                            { ($$ = leaf (p_schm_elem,@$))->sem.qname_raw = $2;}
                          ;

/* [136] */
ElementDeclaration        : ElementName { $$ = $1; }
                          ;

/* [137] */
AttributeName             : QName { $$ = $1; }
                          ;

/* [138] */
ElementName               : QName { $$ = $1; }
                          ;

/* [139] */
TypeName                  : QName { $$ = $1; }
                          ;

/* [140] */
URILiteral                : StringLiteral { $$ = $1; }
                          ;

/* Update extensions follow */

/* [141] XQUpdt */
RevalidationDecl          : "declare revalidation strict"
                            { ($$ = p_leaf (p_revalid, @$))
                                  ->sem.revalid = revalid_strict; }
                          | "declare revalidation lax"
                            { ($$ = p_leaf (p_revalid, @$))
                                  ->sem.revalid = revalid_lax; }
                          | "declare revalidation skip"
                            { ($$ = p_leaf (p_revalid, @$))
                                  ->sem.revalid = revalid_skip; }
                          ;

/* [142] XQUpdt */
InsertExpr                : "do insert" SourceExpr InsertLoc_ TargetExpr
                            { ($$ = p_wire2 (p_insert, @$, $2, $4))
                                  ->sem.insert = $3; }
                          ;

InsertLoc_                : "as first into"  { $$ = p_first_into; }
                          | "as last into"   { $$ = p_last_into; }
                          | "into"           { $$ = p_into; }
                          | "after"          { $$ = p_after; }
                          | "before"         { $$ = p_before; }
                          ;

/* [143] */
DeleteExpr                : "do delete" TargetExpr
                            { $$ = p_wire1 (p_delete, @$, $2); }
                          ;

/* [144] */
ReplaceExpr               : "do replace" TargetExpr "with" ExprSingle
                            { ($$ = p_wire2 (p_replace, @$, $2, $4))
                                  ->sem.tru = false; }
                          | "do replace value of" TargetExpr "with" ExprSingle
                            { ($$ = p_wire2 (p_replace, @$, $2, $4))
                                  ->sem.tru = true; }
                          ;

/* [145] */
/* FIXME: "into" is wrong syntax here.  But we get problems with our
          lexical state machine if we use "as". 
   it has also been noted a bug at W3C: http://www.w3.org/Bugs/Public/show_bug.cgi?id=4176
 */
RenameExpr                : "do rename" TargetExpr "into" NewNameExpr
                            { $$ = p_wire2 (p_rename, @$, $2, $4); }
                          ;

/* [146] */
SourceExpr                : ExprSingle    { $$ = $1; }
                          ;

/* [147] */
TargetExpr                : ExprSingle    { $$ = $1; }
                          ;

/* [148] */
NewNameExpr               : ExprSingle    { $$ = $1; }
                          ;

/* [149] */
TransformExpr             : "transform copy $" TransformBindings_
                            "modify" ExprSingle "return" ExprSingle
                            { $$ = p_wire2 (p_transform, @$,
                                     $2,
                                     p_wire2 (p_modify, loc_rng (@3, @6),
                                              $4, $6)); }
                          ;

TransformBindings_        : TransformBinding_
                            { $$ = p_wire2 (p_transbinds, @$, $1, nil (@$)); }
                          | TransformBinding_ "," TransformBindings_
                            { $$ = p_wire2 (p_transbinds, @$, $1, $3); }
                          ;

TransformBinding_         : VarName ":=" ExprSingle
                            { $$ = p_wire2 (p_let, @$,
                                     p_wire2 (p_var_type, @1, $1, nil (@1)),
                                     $3); }
                          ;

/* Pathfinder extension: recursion */
RecursiveExpr             : "with $" SeedVar "seeded by" ExprSingle
                            "recurse" ExprSingle
                            { $$ = wire2 (p_recursion, @$,
                                     $2,
                                     wire2 (p_seed, @$, $4, $6)); }
                          ;

SeedVar                   : VarName OptTypeDeclaration_
                            { $$ = wire2 (p_var_type, @$, $1, $2); }
                          ;
/* end of the Pathfinder recursion extension */

/* Pathfinder extension: XRPC */
XRPCCall                  : "execute at" "{" ExprSingle "}" "{" FunctionCall "}"
                            { $$ = wire2 (p_xrpc, @$, $3, $6); }
                          | "execute at" URILiteral "{" FunctionCall "}"
                            { $$ = wire2 (p_xrpc, 
                                          @$, 
                                          (c = leaf (p_lit_str, @2),
                                           c->sem.str = $2,
                                           c), 
                                          $4); 
                            }
                          ;
/* End Pathfinder extension */

%%

/** 
 * Check if the input string consists of whitespace only.
 * If this is the case and @c PFquery.pres_boundary_space is set to false, the 
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
 * Add an item to the work list of modules to import.  If it is
 * already in there, do nothing.
 *
 * @param id   namespace id
 * @param ns   namespace associated with this module
 * @param uri  URI where we will find the module definition.
 *
 *  import module [prefix =] ns at uri, uri, uri...
 */
static void
add_to_module_wl (char*id, char *ns, char *uri)
{
    for (unsigned int i = 0; i < PFarray_last (modules); i++)
        if ( !strcmp (((module_t *) PFarray_at (modules, i))->uri, uri) )
            return;

    *(module_t *) PFarray_add (modules)
        = (module_t) { .id = PFstrdup(id), .ns = PFstrdup (ns),
                       .uri = PFstrdup (uri) };
}

/**
 * Invoked by bison whenever a parsing error occurs.
 */
void pferror (const char *s)
{
    if (pftext && *pftext)
        PFinfo (OOPS_PARSE,
                "%s on line %d, column %d (next token is `%s')",
                s, cur_row, cur_col,
                pftext);
    else
        PFinfo (OOPS_PARSE,
                "%s on line %d, column %d",
                s, cur_row, cur_col);
}

char* pfinput = NULL; /* standard input of scanner, used by flex */
YYLTYPE pflloc; /* why ? */

/**
 * Parse an XQuery from the main-memory buffer pointed to by @a input.
 */
#ifdef ENABLE_MILPRINT_SUMMER
int
#else
void
#endif
PFparse (char *input, PFpnode_t **r)
{
#if YYDEBUG
    pfdebug = 1;
#endif

    /* initialize lexical scanner */
    PFscanner_init (input);

    /* initialize work list of modules to load */
    modules = PFarray (sizeof (module_t));
    
    /* we don't explicitly ask for modules */
    module_only = false;
    current_uri = NULL;

#ifdef ENABLE_MILPRINT_SUMMER
    module_base = 0;
    num_fun = 0;
#endif

    /* initialisation of yylloc */
    pflloc.first_row = pflloc.last_row = 1;
    pflloc.first_col = pflloc.last_col = 0;

    if (pfparse ())
        PFoops (OOPS_PARSE, "XQuery parsing failed");

    *r = root;

#ifdef ENABLE_MILPRINT_SUMMER
    return num_fun;
#endif
}

/**
 * Load and parse the module located at @a uri (has to be associated
 * with namespace @a ns).
 */
static PFpnode_t *
parse_module (char *ns, char *uri)
{
    char *buf = PFurlcache (uri, 1);

    if (buf == NULL)
        PFoops (OOPS_MODULEIMPORT, "error loading module from %s", uri);

    /*
     * remember the URI that we are currently parsing.  We will leave
     * this information in p_lib_mod nodes.
     */
    current_uri = uri;

    /* file is now loaded into main-memory */
    PFscanner_init (buf);

    /* initialisation of yylloc */
    pflloc.first_row = pflloc.last_row = 1;
    pflloc.first_col = pflloc.last_col = 0;
    
    req_module_ns = ns;

    if (pfparse ())
        PFoops (OOPS_PARSE, "error parsing module from %s", uri);

    return root;
}

/**
 * Load and parse modules listed in working list and put them into
 * the parse tree @a r.
 */
#ifdef ENABLE_MILPRINT_SUMMER
int
#else
void
#endif
PFparse_modules (PFpnode_t *r)
{
    PFpnode_t *module;
    PFloc_t    noloc = (PFloc_t) { .first_row = 0, .first_col = 0,
                                   .last_row  = 0, .last_col  = 0};

    /* only accept module from now on */
    module_only = true;

    for (unsigned int i = 0; i < PFarray_last (modules); i++) {

        module = parse_module (((module_t *) PFarray_at (modules, i))->ns,
                               ((module_t *) PFarray_at (modules, i))->uri);
        req_module_ns = NULL;

        /*
         * declarations are the left child of the root for queries,
         * right child otherwise.
         */
        if (r->kind == p_main_mod)
            r->child[0]
                = wire2 (p_decl_imps, noloc, module, r->child[0]);
        else
            r->child[1]
                = wire2 (p_decl_imps, noloc, module, r->child[1]);
    }

#ifdef ENABLE_MILPRINT_SUMMER
    return (module_base%2000000)*1000;
#endif
}

/* vim:set shiftwidth=4 expandtab: */

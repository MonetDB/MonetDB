/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

prologue { 

/*
 * XQuery Formal Semantics: mapping XQuery to XQuery Core.
 *
 * In this file, a reference to `W3C XQuery' refers to the W3C WD
 * `XQuery 1.0 and XPath 2.0 Formal Semantics', Draft Nov 15, 2002
 * http://www.w3.org/TR/2002/WD-query-semantics-20021115/
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

/*
 * NOTE (Revision Information):
 *
 * Changes in the Core2MIL_Summer2004 branch have been merged into
 * this file on July 15, 2004. I have tagged this file in the
 * Core2MIL_Summer2004 branch with `merged-into-main-15-07-2004'.
 *
 * For later merges from the Core2MIL_Summer2004, please only merge
 * the changes since this tag.
 *
 * Jens
 */
  
/* Auxiliary routines related to the formal semantics are located in
 * this separate included file to facilitate automated documentation
 * via doxygen.
 */	
#include "fs_impl.h"

/*
 * We use the Unix `sed' tool to make `[[ e ]]' a synonym for
 * `(e)->core' (the core equivalent of e). The following sed expressions
 * will do the replacement.
 *
 * (The following lines contain the special marker that is used
 * in the build process. The build process will search the file
 * for these markers, extract the sed expressions and feed the file
 * with these expressions through sed. Write sed expressions in
 * _exactly_ this style!)
 *
 *!sed 's/\[\[/(/g'
 *!sed 's/\]\]/)->core/g'
 *
 * (First line translates all `[[' into `(', second line translates all
 * `]]' into `)->core'.)
 */

/* element/attribute constructor and content
 */
static PFty_t (*elem_attr) (PFqname_t, PFty_t);
static PFty_t any;

};

node  plus         /* binary + */
      minus        /* binary - */
      mult         /* * (multiplication) */
      div_         /* div (division) */
      idiv         /* idiv (integer division) */
      mod          /* mod */
      and          /* and */
      or           /* or */
      lt           /* < (less than) */
      le           /* <= (less than or equal) */
      gt           /* > (greater than) */
      ge           /* >= (greater than or equal) */
      eq           /* = (equality) */
      ne           /* != (inequality) */
      val_lt       /* lt (value less than) */
      val_le       /* le (value less than or equal) */
      val_gt       /* gt (value greater than) */
      val_ge       /* ge (value greter than or equal) */
      val_eq       /* eq (value equality) */
      val_ne       /* ne (value inequality) */
      uplus        /* unary + */
      uminus       /* unary - */
      lit_int      /* integer literal */
      lit_dec      /* decimal literal */
      lit_dbl      /* double literal */
      lit_str      /* string literal */
      is           /* is (node identity) */
      nis          /* isnot (negated node identity) *grin* */
      step         /* axis step */
      var          /* ``real'' scoped variable */
      namet        /* name test */
      kindt        /* kind test */
      locpath      /* location path */
      root_        /* / (document root) */
      dot          /* current context node */
      ltlt         /* << (less than in doc order) */
      gtgt         /* >> (greater in doc order) */
      flwr         /* for-let-where-return */
      binds        /* sequence of variable bindings */
      nil          /* end-of-sequence marker */
      empty_seq    /* the empty sequence */
      bind         /* for/some/every variable binding */
      let          /* let binding */
      exprseq      /* e1, e2 (expression sequence) */
      range        /* to (range) */
      union_       /* union */
      intersect    /* intersect */
      except       /* except */
      pred         /* e1[e2] (predicate) */
      if_          /* if-then-else */
      some         /* some (existential quantifier) */
      every        /* every (universal quantifier) */
      orderby      /* order by */
      orderspecs   /* order criteria */
      instof       /* instance of */
      seq_ty       /* sequence type */
      empty_ty     /* empty type */
      node_ty      /* node type */
      item_ty      /* item type */
      atom_ty      /* named atomic type */
      atomval_ty   /* atomic value type */
      named_ty     /* named type */ 
      req_ty       /* required type */
      req_name     /* required name */
      typeswitch   /* typeswitch */
      cases        /* list of case branches */
      case_        /* a case branch */
      schm_path    /* path of schema context steps */
      schm_step    /* schema context step */
      glob_schm    /* global schema */
      glob_schm_ty /* global schema type */
      castable     /* castable */
      cast         /* cast as */
      treat        /* treat as */
      validate     /* validate */
      apply        /* e1 (e2, ...) (function application) */
      args         /* function argument list (actuals) */
      char_        /* character content */
      doc          /* document constructor (document { }) */
      elem         /* XML element constructor */
      attr         /* XML attribute constructor */
      text         /* XML text node constructor */
      tag          /* (fixed) tag name */
      pi           /* <?...?> content */
      comment      /* <!--...--> content */
      contseq      /* constructor content sequence */
      xquery       /* root of the query parse tree */
      prolog       /* query prolog */
      decl_imps    /* list of declarations and imports */
      xmls_decl    /* xmlspace declaration */
      coll_decl    /* default collation declaration */
      ns_decl      /* namespace declaration */
      fun_decls    /* list of function declarations */
      fun          /* function declaration */
      ens_decl     /* default element namespace declaration */
      fns_decl     /* default function namespace declaration */
      schm_imp     /* schema import */
      params       /* list of (formal) function parameters */
      param;       /* (formal) function parameter */

label Query
      QueryProlog
      QueryBody
      OptExprSequence_
      ExprSequence
      EmptySequence_
      Expr
      OrderSpecList
      OrExpr
      AndExpr
      FLWRExpr
      OptPositionalVar_
      PositionalVar
      OptTypeDeclaration_
      TypeDeclaration
      OptWhereClause_
      WhereClause
      OrderByClause
      QuantifiedExpr
      TypeswitchExpr
      OptCaseVar_
      SingleType
      SequenceType
      ItemType
      ElemOrAttrType
      SchemaType
      AtomType
      IfExpr
      InstanceofExpr
      ComparisonExpr
      RangeExpr
      AdditiveExpr
      MultiplicativeExpr
      UnionExpr
      IntersectExceptExpr
      UnaryExpr
      ValueExpr
      ValidateExpr
      OptSchemaContext_
      SchemaContext
      SchemaGlobalContext
      SchemaContextSteps_
      SchemaContextStep
      CastableExpr
      CastExpr
      TreatExpr
      ParenthesizedExpr
      Constructor
      ElementConstructor
      ElementContent
      TagName
      XmlComment
      XmlProcessingInstruction
      DocumentConstructor
      AttributeConstructor          
      TextConstructor
      AttributeValue
      OptElemContSequence_
      ElemContSequence
      OptAttrContSequence_
      AttrContSequence
      AttrEnclosedExpr
      PathExpr
      LocationStep_
      LocationPath_
      StepExpr
      NodeTest
      KindTest
      NameTest
      PrimaryExpr
      Literal
      NumericLiteral
      StringLiteral
      IntegerLiteral
      DecimalLiteral
      DoubleLiteral 
      FunctionCall
      FuncArgList_
      DeclsImports_
      NamespaceDecl
      XMLSpaceDecl
      DefaultNamespaceDecl 
      DefaultCollationDecl
      SchemaImport
      OptSchemaLoc_
      FunctionDefns_
      FunctionDefn
      OptParamList_
      ParamList
      Param
      OptAs_
      Var_
      Nil_;

Query:		    	xquery (QueryProlog, QueryBody)
                        { assert ($$);  /* avoid `root unused' warning */ }
    =
    { 
        /* FIXME: this ignores the QueryProlog */
        [[ $$ ]] = [[ $2$ ]];
    }
    ;

QueryProlog:            prolog (DeclsImports_, FunctionDefns_);

DeclsImports_:          Nil_;
DeclsImports_:          decl_imps (NamespaceDecl, DeclsImports_);
DeclsImports_:          decl_imps (XMLSpaceDecl, DeclsImports_);
DeclsImports_:          decl_imps (DefaultNamespaceDecl, DeclsImports_);
DeclsImports_:          decl_imps (DefaultCollationDecl, DeclsImports_);
DeclsImports_:          decl_imps (SchemaImport, DeclsImports_);

FunctionDefns_:         Nil_;
FunctionDefns_:         fun_decls (FunctionDefn, FunctionDefns_);

QueryBody:              OptExprSequence_;

OptExprSequence_:       EmptySequence_; 
OptExprSequence_:       ExprSequence;

ExprSequence:           exprseq (Expr, EmptySequence_)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        
        [[ $$ ]] =
        let (var (v1), [[ $1$ ]],
             let (var (v2), [[ $2$ ]],
                  seq (var (v1), var (v2))));
    }
    ;
ExprSequence:           exprseq (Expr, ExprSequence)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        
        [[ $$ ]] =
        let (var (v1), [[ $1$ ]],
             let (var (v2), [[ $2$ ]],
                  seq (var (v1), var (v2))));
    }
    ;

EmptySequence_:         empty_seq
    =
    {
        [[ $$ ]] = empty ();
    }
    ;

Expr:                   OrExpr;

OrExpr:                 AndExpr;
OrExpr:                 or (OrExpr, AndExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_or = function (PFqname (PFns_op, "or"));

        [[ $$ ]] = let (var (v1), ebv ([[ $1$ ]]),
                        let (var (v2), ebv ([[ $2$ ]]),
                             APPLY (op_or, var (v1), var (v2))));
    }
    ;                           

AndExpr:                FLWRExpr;
AndExpr:                and (AndExpr, FLWRExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_or = function (PFqname (PFns_op, "and"));

        [[ $$ ]] = let (var (v1), ebv ([[ $1$ ]]),
                        let (var (v2), ebv ([[ $2$ ]]),
                             APPLY (op_or, var (v1), var (v2))));
    }
    ;                           

FLWRExpr:               QuantifiedExpr;
FLWRExpr:               flwr (binds (let (Nil_, Var_, Expr), 
                                     Nil_), 
			      OptWhereClause_, 
			      Nil_,
			      FLWRExpr)
    =
    {
        PFvar_t *v = new_var (0);
        
        [[ $$ ]] =
        let ([[ $1.1.2$ ]], [[ $1.1.3$ ]],
             let (var (v), [[ $2$ ]],
                  ifthenelse (var (v), 
                              [[ $4$ ]], empty ())));
    }
    ;
FLWRExpr:               flwr (binds (let (TypeDeclaration, Var_, Expr), 
                                     Nil_), 
			      OptWhereClause_, 
			      Nil_,
			      FLWRExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);

        [[ $$ ]] = let (var (v1), [[ $1.1.3$ ]],
                        let ([[ $1.1.2$ ]], 
                             proof (var (v1), [[ $1.1.1$ ]], 
                                    seqcast ([[ $1.1.1$ ]], var (v1))),
                             let (var (v2), [[ $2$ ]],
                                  ifthenelse (var (v2),
                                              [[ $4$ ]],
                                              empty ()))));
    }
    ;
FLWRExpr:               flwr (binds (let (Nil_, Var_, Expr), 
                                     Nil_), 
			      OptWhereClause_, 
			      OrderByClause,
			      FLWRExpr)
    =
    {
        /* FIXME: OrderByClause ignored for now */
        
        PFvar_t *v = new_var (0);
        
        [[ $$ ]] =
        let ([[ $1.1.2$ ]], [[ $1.1.3$ ]],
             let (var (v), [[ $2$ ]],
                  ifthenelse (var (v), 
                              [[ $4$ ]], empty ())));
        
        PFinfo_loc (OOPS_WARN_NOTSUPPORTED,
                    ($3$)->loc,
                    "`order by' (ignored)");
    }
    ;
FLWRExpr:               flwr (binds (let (TypeDeclaration, Var_, Expr), 
                                     Nil_), 
			      OptWhereClause_, 
			      OrderByClause,
			      FLWRExpr)
    =
    {
        /* FIXME: OrderByClause ignored for now */
        
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);

        [[ $$ ]] = let (var (v1), [[ $1.1.3$ ]],
                        let ([[ $1.1.2$ ]], 
                             proof (var (v1), [[ $1.1.1$ ]], 
                                    seqcast ([[ $1.1.1$ ]], var (v1))),
                             let (var (v2), [[ $2$ ]],
                                  ifthenelse (var (v2),
                                              [[ $4$ ]],
                                              empty ()))));
        
        PFinfo_loc (OOPS_WARN_NOTSUPPORTED,
                    ($3$)->loc,
                    "``order by'' (ignored)");
    }
    ;
FLWRExpr:               flwr (binds (bind (Nil_, OptPositionalVar_, 
                                           Var_, Expr), 
                                     Nil_), 
			      OptWhereClause_, 
			      Nil_,
			      FLWRExpr) 
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *v3 = new_var (0);
        
        [[ $$ ]] = let (var (v1), [[ $1.1.4$ ]],
                        let (var (v2), seq (var (v1), empty ()),
                             for_ ([[ $1.1.3$ ]], [[ $1.1.2$ ]], var (v2),
                                   let (var (v3), [[ $2$ ]],
                                        ifthenelse (var (v3), 
                                                    [[ $4$ ]], empty ())))));
    }
    ;
FLWRExpr:               flwr (binds (bind (TypeDeclaration, OptPositionalVar_,
                                           Var_, Expr), 
                                     Nil_), 
			      OptWhereClause_, 
			      Nil_,
			      FLWRExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *v3 = new_var (0);
        PFvar_t *v4 = new_var (0);

        [[ $$ ]] =
        let (var (v1), [[ $1.1.4$ ]],
             let (var (v2), seq (var (v1), empty ()),
                  for_ (var (v3), [[ $1.1.2$ ]], var (v2),
                        let ([[ $1.1.3$ ]],
                             proof (var (v3), [[ $1.1.1$ ]],
                                    seqcast ([[ $1.1.1$ ]], var (v3))),
                             let (var (v4), [[ $2$ ]],
                                  ifthenelse (var (v4), 
                                              [[ $4$ ]], empty ()))))));
    }
    ;
FLWRExpr:               flwr (binds (bind (Nil_, OptPositionalVar_, 
                                           Var_, Expr), 
                                     Nil_), 
                              OptWhereClause_,
                              OrderByClause, 			    
			      FLWRExpr)
    =
    {
        /* FIXME: OrderByClause ignored for now */
        
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *v3 = new_var (0);
        
        [[ $$ ]] = let (var (v1), [[ $1.1.4$ ]],
                        let (var (v2), seq (var (v1), empty ()),
                             for_ ([[ $1.1.3$ ]], [[ $1.1.2$ ]], var (v1),
                                   let (var (v3), [[ $2$ ]],
                                        ifthenelse (var (v3), 
                                                    [[ $4$ ]], empty ())))));
        
        PFinfo_loc (OOPS_WARN_NOTSUPPORTED,
                    ($3$)->loc,
                    "``order by'' (ignored)");
    }
    ;
FLWRExpr:               flwr (binds (bind (TypeDeclaration, OptPositionalVar_,
                                           Var_, Expr), 
                                     Nil_), 
			      OptWhereClause_, 
			      OrderByClause,
			      FLWRExpr)
    =
    {
        /* FIXME: OrderByClause ignored for now */
        
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *v3 = new_var (0);
        PFvar_t *v4 = new_var (0);
        
        [[ $$ ]] =
        let (var (v1), [[ $1.1.4$ ]],
             let (var (v2), seq (var (v1), empty ()),
                  for_ (var (v3), [[ $1.1.2$ ]], var (v2),
                        let ([[ $1.1.3$ ]],
                             proof (var (v3), [[ $1.1.1$ ]],
                                    seqcast ([[ $1.1.1$ ]], var (v3))),
                             let (var (v4), [[ $2$ ]],
                                  ifthenelse (var (v4), 
                                              [[ $4$ ]], empty ()))))));

        PFinfo_loc (OOPS_WARN_NOTSUPPORTED,
                    ($3$)->loc,
                    "``order by'' (ignored)");
    }
    ;


OptPositionalVar_:      Nil_;
OptPositionalVar_:      PositionalVar;

PositionalVar:          Var_;

OptTypeDeclaration_:    Nil_;
OptTypeDeclaration_:    TypeDeclaration;

TypeDeclaration:        SequenceType;

OptWhereClause_:        Nil_
    =
    {
        [[ $$ ]] = true_ ();
    }
    ;
OptWhereClause_:        WhereClause
    =
    {
        [[ $$ ]] = ebv ([[ $$ ]]);
    }
    ;

OrderByClause:          orderby (OrderSpecList);

OrderSpecList:          orderspecs (Expr, Nil_);
OrderSpecList:          orderspecs (Expr, OrderSpecList);

WhereClause:            Expr;

QuantifiedExpr:         TypeswitchExpr;
QuantifiedExpr:         some (binds (bind (Nil_, Nil_, Var_, Expr), 
                                     Nil_),
			      QuantifiedExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *v3 = new_var (0);
        PFvar_t *v4 = new_var (0);
        PFvar_t *v5 = new_var (0);
        PFfun_t *fn_not   = function (PFqname (PFns_fn, "not"));
        PFfun_t *fn_empty = function (PFqname (PFns_fn, "empty"));

        [[ $$ ]] = let (var (v1), [[ $1.1.4$ ]],
                        let (var (v2), 
                             for_ ([[ $1.1.3$ ]], nil (), 
                                   var (v1), 
                                   let (var (v3), ebv ([[ $2$ ]]),
                                        ifthenelse (var (v3),
                                                    num (1),
                                                    empty ()))),
                             let (var (v4), APPLY (fn_empty, var (v2)),
                                  let (var (v5), APPLY (fn_not, var (v4)),
                                       var (v5)))));
    }
    ;
QuantifiedExpr:         some (binds (bind (TypeDeclaration, Nil_, Var_, Expr), 
                                     Nil_),
                              QuantifiedExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *v3 = new_var (0);
        PFvar_t *v4 = new_var (0);
        PFvar_t *v5 = new_var (0);
        PFfun_t *fn_not   = function (PFqname (PFns_fn, "not"));
        PFfun_t *fn_empty = function (PFqname (PFns_fn, "empty"));

        [[ $$ ]] = 
        let (var (v1), [[ $1.1.4$ ]],
             let (var (v2), 
                  for_ ([[ $1.1.3$ ]], nil (), var (v1), 
                        typeswitch ([[ $1.1.3$ ]],
                            cases (case_ ([[ $1.1.1$ ]],
                                          let (var (v3), ebv ([[ $2$ ]]),
                                               ifthenelse (var (v3),
                                                           num (1),
                                                           empty ()))),
                                   nil ()),
                            error_loc (($$)->loc, 
                                       "$%s does not match required type %s",
                                       PFqname_str (($1.1.3$)->sem.var->qname),
                                       PFty_str ([[ $1.1.1$ ]]->sem.type)))),
                  let (var (v4), APPLY (fn_empty, var (v2)),
                       let (var (v5), APPLY (fn_not, var (v4)),
                            var (v5)))));
    }
    ;
QuantifiedExpr:         every (binds (bind (Nil_, Nil_, Var_, Expr), 
                                      Nil_),
                               QuantifiedExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *v3 = new_var (0);
        PFvar_t *v4 = new_var (0);
        PFvar_t *v5 = new_var (0);
        PFfun_t *fn_not   = function (PFqname (PFns_fn, "not"));
        PFfun_t *fn_empty = function (PFqname (PFns_fn, "empty"));

        [[ $$ ]] = let (var (v1), [[ $1.1.4$ ]],
                        let (var (v2), 
                             for_ ([[ $1.1.3$ ]], nil (), 
                                   var (v1), 
                                   let (var (v3), ebv ([[ $2$ ]]),
                                        let (var (v4), 
                                             APPLY (fn_not, var (v3)),
                                             ifthenelse (var (v4),
                                                         num (1),
                                                         empty ())))),
                                   let (var (v5), APPLY (fn_empty, var (v2)),
                                        var (v5))));
    }
    ;
QuantifiedExpr:         every (binds (bind (TypeDeclaration, Nil_, Var_, Expr),
                                      Nil_),
                               QuantifiedExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *v3 = new_var (0);
        PFvar_t *v4 = new_var (0);
        PFvar_t *v5 = new_var (0);
        PFfun_t *fn_not   = function (PFqname (PFns_fn, "not"));
        PFfun_t *fn_empty = function (PFqname (PFns_fn, "empty"));

        [[ $$ ]] = 
        let (var (v1), [[ $1.1.4$ ]],
             let (var (v2), 
                  for_ ([[ $1.1.3$ ]], nil (), var (v1), 
                        typeswitch ([[ $1.1.3$ ]],
                            cases (case_ ([[ $1.1.1$ ]],
                                          let (var (v3), ebv ([[ $2$ ]]),
                                               let (var (v4), 
                                                    APPLY (fn_not, var (v3)),
                                                    ifthenelse (var (v4),
                                                                num (1),
                                                                empty ())))),
                                   nil ()),
                            error_loc (($$)->loc, 
                                       "$%s does not match required type %s",
                                       PFqname_str (($1.1.3$)->sem.var->qname),
                                       PFty_str ([[ $1.1.1$ ]]->sem.type)))),
                  let (var (v5), APPLY (fn_empty, var (v2)),
                       var (v5))));
    }
    ;

TypeswitchExpr:         IfExpr;
TypeswitchExpr:         typeswitch (Expr,
                                    cases (case_ (SequenceType, 
                                                  OptCaseVar_, Expr), 
                                           Nil_),
                                    OptCaseVar_,
                                    TypeswitchExpr)
    =
    {
        PFvar_t *v = new_var (0);

        [[ $$ ]] = let (var (v), [[ $1$ ]],
                        typeswitch (var (v),
                                    cases (case_ ([[ $2.1.1$ ]], 
                                                  let ([[ $2.1.2$ ]], 
                                                       seqcast ([[ $2.1.1$ ]],
                                                                var (v)),
                                                       [[ $2.1.3$ ]])),
                                           nil ()),
                                    let ([[ $3$ ]], var (v),
                                         [[ $4$ ]])));
    }
    ;

OptCaseVar_:            Nil_
    =
    {
        /* generate new variable $fs_new in case no variable
         * has been specified for case/default branch of typeswitch
         * (see W3C XQuery, 4.12.2)
         */
        PFvar_t *fs_new = new_var (0);

        [[ $$ ]] = var (fs_new);
    }
    ;
OptCaseVar_:            Var_;

SingleType:             seq_ty (AtomType)
    =
    {
        switch (($$)->sem.oci) {
        case p_one:
            [[ $$ ]] = [[ $1$ ]];
            break;
        case p_zero_or_one:
            [[ $$ ]] = 
            seqtype (PFty_opt ([[ $1$ ]]->sem.type));
            break;
        default:
            PFoops (OOPS_FATAL, 
                    "illegal occurrence indicator (%d) in single type",
                    ($$)->sem.oci);        
        }
    }  
    ;
SequenceType:           seq_ty (ItemType)
    =
    {
        switch (($$)->sem.oci) {
        case p_one:
            [[ $$ ]] = [[ $1$ ]];
            break;
        case p_zero_or_one:
            [[ $$ ]] = 
            seqtype (PFty_opt ([[ $1$ ]]->sem.type));
            break;
        case p_zero_or_more:
            [[ $$ ]] = 
            seqtype (PFty_star ([[ $1$ ]]->sem.type));
            break;
        case p_one_or_more:
            [[ $$ ]] = 
            seqtype (PFty_plus ([[ $1$ ]]->sem.type));
            break;
        default:
            PFoops (OOPS_FATAL, 
                    "illegal occurrence indicator (%d) in sequence type",
                    ($$)->sem.oci);
        };
    }
    ;
SequenceType:           seq_ty (empty_ty)
    =
    {
        [[ $$ ]] = seqtype (PFty_empty ());
    }
    ;

ItemType:               node_ty (Nil_)
    =
    {
        switch (($$)->sem.kind) {
        case p_kind_node:
            /* node */
            [[ $$ ]] = seqtype (PFty_xs_anyNode ());
            break;
        case p_kind_comment:
            /* comment */
            [[ $$ ]] = seqtype (PFty_comm ());
            break;
        case p_kind_text:
            /* text */
            [[ $$ ]] = seqtype (PFty_text ());
            break;
        case p_kind_pi:
            /* processing-instruction */
            [[ $$ ]] = seqtype (PFty_pi ());
            break;
        case p_kind_doc:
            /* document */
            [[ $$ ]] = seqtype (PFty_doc (PFty_xs_anyType ()));
            break;
        case p_kind_elem:
            /* element */
            [[ $$ ]] =
            seqtype (PFty_elem (PFqname (PFns_wild, 0), 
                                PFty_xs_anyType ()));
            break;
        case p_kind_attr:
            /* attribute */
            [[ $$ ]] = 
            seqtype (PFty_attr (PFqname (PFns_wild, 0), 
                                PFty_xs_anySimpleType ()));
            break;
        default:
            PFoops (OOPS_FATAL,
                    "illegal node kind ``%s'' in node type",
                    p_id[($$)->sem.kind]);
        };
    }  
    ;
ItemType:               node_ty (ElemOrAttrType)
    { TOPDOWN; }
    =
    {
        switch (($$)->sem.kind) {
        case p_kind_elem:
            elem_attr = PFty_elem;
            any = PFty_xs_anyType ();
            break;
        case p_kind_attr:
            elem_attr = PFty_attr;
            any = PFty_xs_anySimpleType ();
            break;
        default:
            PFoops (OOPS_FATAL, "illegal node kind ``%s'' in type",
                    p_id[($$)->sem.kind]);
        }

        tDO ($%1$);

        [[ $$ ]] = [[ $1$ ]];
    }
    ;
ItemType:               item_ty (Nil_)
    =
    {
        /* item */
        [[ $$ ]] = seqtype (PFty_xs_anyItem ()); 
    }
    ;
ItemType:               AtomType;
ItemType:               atomval_ty (Nil_)
    =
    {
        /* atomic value */
        [[ $$ ]] = seqtype (PFty_xs_anySimpleType ()); 
    }
    ;

ElemOrAttrType:         req_ty (req_name, SchemaType)
    =
    {   /* element/attribute qn of type qn */
        [[ $$ ]] = seqtype (elem_attr ($1$->sem.qname,
                                       [[ $2$ ]]->sem.type));
    }
    ;
ElemOrAttrType:         req_ty (Nil_, SchemaType)
    =
    {   /* element/attribute of type qn */
        [[ $$ ]] = seqtype (elem_attr (PFqname (PFns_wild, 0),
                                       [[ $2$ ]]->sem.type));
    }
    ;
ElemOrAttrType:         req_ty (req_name, Nil_)
    =
    {   /* element/attribute qn */
        [[ $$ ]] = seqtype (elem_attr ($1$->sem.qname, 
                                       any));
    }
    ;
ElemOrAttrType:         req_ty (req_name, SchemaContext)
    =
    {   /* FIXME: we do not implement schema contexts for now */

        /* element/attribute qn context ... */
        [[ $$ ]] = seqtype (elem_attr ($1$->sem.qname, 
                                       any));

        PFinfo_loc (OOPS_WARN_NOTSUPPORTED,
                    ($2$)->loc,
                    "schema context (assuming content type ``%s'')",
                    PFty_str (any));
    }
    ;

SchemaType:             named_ty
    =
    {
        /* is the referenced type a known schema type definition? */
        if (! PFty_schema (PFty_named (($$)->sem.qname)))
            PFoops_loc (OOPS_TYPENOTDEF, ($$)->loc,
                        "``%s''",
                        PFqname_str (($$)->sem.qname));

        [[ $$ ]] = seqtype (PFty_named (($$)->sem.qname));
    }
    ;

AtomType:               atom_ty (Nil_)
    =
    {
        /* is the referenced type a known schema type? */
        if (! PFty_schema (PFty_named (($$)->sem.qname)))
            PFoops_loc (OOPS_TYPENOTDEF, ($$)->loc,
                        "``%s''",
                        PFqname_str (($$)->sem.qname));

        [[ $$ ]] = seqtype (PFty_named (($$)->sem.qname));
    }
    ;

IfExpr:                 InstanceofExpr;
IfExpr:                 if_ (Expr, Expr, Expr)
    =
    {
        PFvar_t *v = new_var (0);

        [[ $$ ]] = let (var (v), ebv ([[ $1$ ]]),
                        ifthenelse (var (v), [[ $2$ ]], [[ $3$ ]]));
                                 
    }
    ;

InstanceofExpr:         CastableExpr;
InstanceofExpr:         instof (CastableExpr, SequenceType)
    =
    {
        PFvar_t *v = new_var (0);

        [[ $$ ]] = let (var (v), [[ $1$ ]],
                        typeswitch (var (v),
                                    cases (case_ ([[ $2$ ]], true_ ()),
                                           nil ()),
                                    false_ ()));
    }
    ;

CastableExpr:           ComparisonExpr;
CastableExpr:           castable (ComparisonExpr, SingleType);

ComparisonExpr:         RangeExpr;
ComparisonExpr:         eq (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *u1 = new_var (0);
        PFvar_t *u2 = new_var (0);
        PFfun_t *op_eq = function (PFqname (PFns_op, "eq"));

        [[ $$ ]] = some (var (v1), fn_data ([[ $1$ ]]), 
                         some (var (v2), fn_data ([[ $2$ ]]),
                               let (var (u1),
                                    fs_convert_op_by_type( 
                                               fs_convert_op_by_expr (var (v1),
                                                                      var (v2)),
                                               PFty_xs_string ()),
                                    let (var (u2), 
                                         fs_convert_op_by_type (
                                                fs_convert_op_by_expr (var (v2), 
                                                                       var (v1)),
                                                PFty_xs_string ()),
                                         APPLY (op_eq, var (u1), var (u2))))
                              )
                        );
    }
    ;
                           
ComparisonExpr:         ne (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *u1 = new_var (0);
        PFvar_t *u2 = new_var (0);
        PFfun_t *op_ne = function (PFqname (PFns_op, "ne"));

        [[ $$ ]] = some (var (v1), fn_data ([[ $1$ ]]), 
                         some (var (v2), fn_data ([[ $2$ ]]),
                               let (var (u1),
                                    fs_convert_op_by_type( 
                                               fs_convert_op_by_expr (var (v1),
                                                                      var (v2)),
                                               PFty_xs_string ()),
                                    let (var (u2), 
                                         fs_convert_op_by_type (
                                                fs_convert_op_by_expr (var (v2), 
                                                                       var (v1)),
                                                PFty_xs_string ()),
                                         APPLY (op_ne, var (u1), var (u2))))
                              )
                        );
    }
    ;
                           
ComparisonExpr:         lt (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *u1 = new_var (0);
        PFvar_t *u2 = new_var (0);
        PFfun_t *op_lt = function (PFqname (PFns_op, "lt"));

        [[ $$ ]] = some (var (v1), fn_data ([[ $1$ ]]), 
                         some (var (v2), fn_data ([[ $2$ ]]),
                               let (var (u1),
                                    fs_convert_op_by_type( 
                                               fs_convert_op_by_expr (var (v1),
                                                                      var (v2)),
                                               PFty_xs_string ()),
                                    let (var (u2), 
                                         fs_convert_op_by_type (
                                                fs_convert_op_by_expr (var (v2), 
                                                                       var (v1)),
                                                PFty_xs_string ()),
                                         APPLY (op_lt, var (u1), var (u2))))
                              )
                        );
    }
    ;
                           
ComparisonExpr:         le (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *u1 = new_var (0);
        PFvar_t *u2 = new_var (0);
        PFfun_t *op_le = function (PFqname (PFns_op, "le"));

        [[ $$ ]] = some (var (v1), fn_data ([[ $1$ ]]), 
                         some (var (v2), fn_data ([[ $2$ ]]),
                               let (var (u1),
                                    fs_convert_op_by_type( 
                                               fs_convert_op_by_expr (var (v1),
                                                                      var (v2)),
                                               PFty_xs_string ()),
                                    let (var (u2), 
                                         fs_convert_op_by_type (
                                                fs_convert_op_by_expr (var (v2), 
                                                                       var (v1)),
                                                PFty_xs_string ()),
                                         APPLY (op_le, var (u1), var (u2))))
                              )
                        );
    }
    ;
                           
ComparisonExpr:         gt (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *u1 = new_var (0);
        PFvar_t *u2 = new_var (0);
        PFfun_t *op_gt = function (PFqname (PFns_op, "gt"));

        [[ $$ ]] = some (var (v1), fn_data ([[ $1$ ]]), 
                         some (var (v2), fn_data ([[ $2$ ]]),
                               let (var (u1),
                                    fs_convert_op_by_type( 
                                               fs_convert_op_by_expr (var (v1),
                                                                      var (v2)),
                                               PFty_xs_string ()),
                                    let (var (u2), 
                                         fs_convert_op_by_type (
                                                fs_convert_op_by_expr (var (v2), 
                                                                       var (v1)),
                                                PFty_xs_string ()),
                                         APPLY (op_gt, var (u1), var (u2))))
                              )
                        );
    }
    ;
                           
ComparisonExpr:         ge (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *u1 = new_var (0);
        PFvar_t *u2 = new_var (0);
        PFfun_t *op_ge = function (PFqname (PFns_op, "ge"));

        [[ $$ ]] = some (var (v1), fn_data ([[ $1$ ]]), 
                         some (var (v2), fn_data ([[ $2$ ]]),
                               let (var (u1),
                                    fs_convert_op_by_type( 
                                               fs_convert_op_by_expr (var (v1),
                                                                      var (v2)),
                                               PFty_xs_string ()),
                                    let (var (u2), 
                                         fs_convert_op_by_type (
                                                fs_convert_op_by_expr (var (v2), 
                                                                       var (v1)),
                                                PFty_xs_string ()),
                                         APPLY (op_ge, var (u1), var (u2))))
                              )
                        );
    }
    ;
                           
ComparisonExpr:         val_eq (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_eq = function (PFqname (PFns_op, "eq"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_string ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_string ()),
                             APPLY (op_eq, var (v1), var (v2))));
    }
    ;
                           
ComparisonExpr:         val_ne (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_ne = function (PFqname (PFns_op, "ne"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_string ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_string ()),
                             APPLY (op_ne, var (v1), var (v2))));
    }
    ;
                           
ComparisonExpr:         val_lt (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_lt = function (PFqname (PFns_op, "lt"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_string ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_string ()),
                             APPLY (op_lt, var (v1), var (v2))));
    }
    ;
                           
ComparisonExpr:         val_le (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_le = function (PFqname (PFns_op, "le"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_string ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_string ()),
                             APPLY (op_le, var (v1), var (v2))));
    }
    ;
                           
ComparisonExpr:         val_gt (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_gt = function (PFqname (PFns_op, "gt"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_string ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_string ()),
                             APPLY (op_gt, var (v1), var (v2))));
    }
    ;
                           
ComparisonExpr:         val_ge (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_ge = function (PFqname (PFns_op, "ge"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_string ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_string ()),
                             APPLY (op_ge, var (v1), var (v2))));
    }
    ;
                           
ComparisonExpr:         is (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_is = function (PFqname (PFns_op, "is-same-node"));

        [[ $$ ]] = let (var (v1), [[ $1$ ]],
                        let (var (v2), [[ $2$ ]],
                             APPLY (op_is, var (v1), var (v2))));
    }
    ;

ComparisonExpr:         nis (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *v3 = new_var (0);
        PFvar_t *v4 = new_var (0);
        PFvar_t *v5 = new_var (0);
        PFfun_t *op_is = function (PFqname (PFns_op, "is-same-node"));
        PFfun_t *fn_not = function (PFqname (PFns_fn, "not"));

        [[ $$ ]] = let (var (v1), [[ $1$ ]],
                        let (var (v2), [[ $2$ ]],
                             let (var (v3),
                                  APPLY (op_is, var (v1), var (v2)),
                                  let (var (v4),
                                       APPLY (fn_not, PFcore_var (v3)),
                                       PFcore_var (v5)))));
    }
    ;

ComparisonExpr:         ltlt (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_ltlt = function (PFqname (PFns_op, "node-before"));

        [[ $$ ]] = let (var (v1), [[ $1$ ]],
                        let (var (v2), [[ $2$ ]],
                             APPLY (op_ltlt, var (v1), var (v2))));
    }
    ;

ComparisonExpr:         gtgt (RangeExpr, RangeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_gtgt = function (PFqname (PFns_op, "node-after"));

        [[ $$ ]] = let (var (v1), [[ $1$ ]],
                        let (var (v2), [[ $2$ ]],
                             APPLY (op_gtgt, var (v1), var (v2))));
    }
    ;


RangeExpr:              AdditiveExpr;
RangeExpr:              range (AdditiveExpr, AdditiveExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_to = function (PFqname (PFns_op, "to"));

        [[ $$ ]] = let (var (v1), [[ $1$ ]],
                        let (var (v2), [[ $2$ ]],
                             APPLY (op_to, var (v1), var (v2))));
    }
    ;


AdditiveExpr:           MultiplicativeExpr;
AdditiveExpr:           plus (AdditiveExpr, MultiplicativeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_plus = function (PFqname (PFns_op, "plus"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_double ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_double ()),
                             APPLY (op_plus, var (v1), var (v2))));
    }
    ;
                           
AdditiveExpr:           minus (AdditiveExpr, MultiplicativeExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_minus = function (PFqname (PFns_op, "minus"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_double ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_double ()),
                             APPLY (op_minus, var (v1), var (v2))));
    }
    ;                           

MultiplicativeExpr:     UnaryExpr;
MultiplicativeExpr:     mult (MultiplicativeExpr, UnaryExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_times = function (PFqname (PFns_op, "times"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_double ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_double ()),
                             APPLY (op_times, var (v1), var (v2))));
    }
    ;                           

MultiplicativeExpr:     div_ (MultiplicativeExpr, UnaryExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_div = function (PFqname (PFns_op, "div"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_double ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_double ()),
                             APPLY (op_div, var (v1), var (v2))));
    }
    ;                           

MultiplicativeExpr:     idiv (MultiplicativeExpr, UnaryExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_idiv = function (PFqname (PFns_op, "idiv"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_integer ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_integer ()),
                             APPLY (op_idiv, var (v1), var (v2))));
    }
    ;                           

MultiplicativeExpr:     mod (MultiplicativeExpr, UnaryExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_mod = function (PFqname (PFns_op, "mod"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_double ()),
                        let (var (v2),
                             fs_convert_op_by_type (fn_data ([[ $2$ ]]),
                                                    PFty_xs_double ()),
                             APPLY (op_mod, var (v1), var (v2))));
    }
    ;                           


UnaryExpr:              UnionExpr;
UnaryExpr:              uminus (UnionExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFfun_t *op_minus = function (PFqname (PFns_op, "minus"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_double ()),
                        APPLY (op_minus, num (0), var (v1)));
    }
    ;                           

UnaryExpr:              uplus (UnionExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFfun_t *op_plus = function (PFqname (PFns_op, "plus"));

        [[ $$ ]] = let (var (v1), fs_convert_op_by_type (fn_data ([[ $1$ ]]),
                                                         PFty_xs_double ()),
                        APPLY (op_plus, num (0), var (v1)));
    }
    ;                           


UnionExpr:              IntersectExceptExpr;
UnionExpr:              union_ (UnionExpr, IntersectExceptExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_union = function (PFqname (PFns_op, "union"));

        [[ $$ ]] = let (var (v1), [[ $1$ ]],
                        let (var (v2), [[ $2$ ]],
                             APPLY (op_union, var (v1), var (v2))));
    }
    ;                           


IntersectExceptExpr:    ValueExpr;
IntersectExceptExpr:    intersect (IntersectExceptExpr, UnaryExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_intersect = function (PFqname (PFns_op, "intersect"));

        [[ $$ ]] = let (var (v1), [[ $1$ ]],
                        let (var (v2), [[ $2$ ]],
                             APPLY (op_intersect, var (v1), var (v2))));
    }
    ;                           

IntersectExceptExpr:    except (IntersectExceptExpr, UnaryExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *op_except = function (PFqname (PFns_op, "except"));

        [[ $$ ]] = let (var (v1), [[ $1$ ]],
                        let (var (v2), [[ $2$ ]],
                             APPLY (op_except, var (v1), var (v2))));
    }
    ;                           


ValueExpr:              ValidateExpr;
ValueExpr:              CastExpr;
ValueExpr:              TreatExpr;
ValueExpr:              Constructor;
ValueExpr:              PathExpr;                       

ValidateExpr:           validate (OptSchemaContext_, Expr);

OptSchemaContext_:      Nil_;
OptSchemaContext_:      SchemaContext;

SchemaContext:          schm_path (SchemaGlobalContext, SchemaContextSteps_);

SchemaContextSteps_:    Nil_;
SchemaContextSteps_:    schm_path (SchemaContextStep, SchemaContextSteps_);

SchemaGlobalContext:    glob_schm;
SchemaGlobalContext:    glob_schm_ty;

SchemaContextStep:      schm_step;

CastExpr:               cast (SingleType, ParenthesizedExpr)
    =
    {
        PFvar_t *v1 = new_var (0);

        [[ $$ ]] = let (var (v1), [[ $2$ ]], 
                        seqcast ([[ $1$ ]], var (v1)));
    }
    ;

TreatExpr:              treat (SingleType, ParenthesizedExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFfun_t *fn_error = function (PFqname (PFns_fn, "error"));

        [[ $$ ]] = let (var (v1), [[ $2$ ]], 
                        typeswitch (var (v1),
                                    cases (case_ ([[ $1$ ]],
                                                  var (v1)),
                                           nil ()),
                /* FIXME: fn_error works only with more then 1 argument */
                                    APPLY (fn_error, var(v1)))); 
    }
    ;

ParenthesizedExpr:      OptExprSequence_;

Constructor:            ElementConstructor;
Constructor:            XmlComment;
Constructor:            XmlProcessingInstruction;
Constructor:            DocumentConstructor;
Constructor:            AttributeConstructor;
Constructor:            TextConstructor;

ElementConstructor:     elem (TagName, ElementContent)
    =
    {
        /*
         * FIXME:
         *   The code that has been generated for $2$ has piecewise gone
         *   through item-sequence-to-node-sequence() and is thus a
         *   sequence of nodes.
         *   Additionally, the W3C specification [XQ, 3.7.1.3] demands
         *    -- adjacent text nodes to be merged into one, and
         *    -- empty text nodes to be removed.
         *   I'd recommend to introduce something like
         *   ``merge-adjacent-text-nodes()'' here and wrap it around $2$.
         *     Jens, 27.08.04 - updated 21.09.04 JR
         */
        PFfun_t *pf_matn = function (PFqname (PFns_pf, "merge-adjacent-text-nodes"));

        [[ $$ ]] = constr_elem ([[ $1$ ]], APPLY (pf_matn, [[ $2$ ]]));
    }
    ;

AttributeConstructor:   attr (TagName, AttributeValue)
    =
    {
        /*
         * The translation rules rooted at AttributeValue compile
         * the attribute value into a single string. The W3C specification
         * converts this string into an xdt:untypedAtomic afterwards.
         * However, I can't really see sense in this cast, so I left
         * the attribute value as it is. After the attribute has been
         * constructed, its value will have the type xdt:untypedAtomic
         * anyway.
         *   Jens, 27.08.04
         */
        [[ $$ ]] = constr_attr ([[ $1$ ]], [[ $2$ ]]);
    }
    ;

TextConstructor:        text (OptExprSequence_)
    =
    {
        PFfun_t *is2uA = 
                function (PFqname (PFns_pf, "item-sequence-to-untypedAtomic"));

        [[ $$ ]] = constr (($$)->kind, APPLY (is2uA, [[ $1$ ]]));
    }
    ;

DocumentConstructor:    doc (OptExprSequence_)
    =
    {
        PFfun_t *is2ns = 
                function (PFqname (PFns_pf, "item-sequence-to-node-sequence"));

        [[ $$ ]] = constr (($$)->kind, APPLY (is2ns, [[ $1$ ]]));
    }
    ;

XmlComment:             comment (StringLiteral)
    =
    {
        PFfun_t *is2uA = 
                function (PFqname (PFns_pf, "item-sequence-to-untypedAtomic"));

        [[ $$ ]] = constr (($$)->kind, APPLY (is2uA, [[ $1$ ]]));
    }
    ;

XmlProcessingInstruction: pi (StringLiteral)
    =
    {
        PFfun_t *is2uA = 
                function (PFqname (PFns_pf, "item-sequence-to-untypedAtomic"));

        [[ $$ ]] = constr (($$)->kind, APPLY (is2uA, [[ $1$ ]]));
    }
    ;
                        
TagName:                tag
    =
    {
        [[ $$ ]] = constr_tag (($$)->sem.qname);
    }
    ;

TagName:                Expr
    =
    {
        /* in theory this should be translated into
           'fn:resolve-QName' but we don't know the
           element argument - therefore internally
           the string is translated into a qname */
        [[ $$ ]] = fs_convert_op_by_type (fn_data ([[ $$ ]]), PFty_string ());
    }
    ;

ElementContent:         OptElemContSequence_;

AttributeValue:         OptAttrContSequence_;

OptElemContSequence_:   EmptySequence_;
OptElemContSequence_:   ElemContSequence;

ElemContSequence:       contseq (Expr, EmptySequence_)
    =
    {
        /*
         * let $v1 := [[ e1 ]] return
         *     let $v2 := fs:item-sequence-to-node-sequence($v1) return
         *         $v2
         */
        PFvar_t *v1 = new_var (NULL);
        PFvar_t *v2 = new_var (NULL);
        PFfun_t *is2ns = 
                function (PFqname (PFns_pf, "item-sequence-to-node-sequence"));

        [[ $$ ]] =
            let (var (v1), [[ $1$ ]],
                 let (var (v2), APPLY (is2ns, var (v1)),
                           var (v2)));
    }
    ;
ElemContSequence:       contseq (Expr, ElemContSequence)
    =
    {
        /*
         * let $v1 := [[ e1 ]] return
         *   let $v2 := [[ e2 ]] return
         *     let $v3 := fs:item-sequence-to-node-sequence($v1) return
         *       ( $v3, $v2 )
         */
        PFvar_t *v1 = new_var (NULL);
        PFvar_t *v2 = new_var (NULL);
        PFvar_t *v3 = new_var (NULL);
        PFfun_t *is2ns = 
                function (PFqname (PFns_pf, "item-sequence-to-node-sequence"));

        [[ $$ ]] =
            let (var (v1), [[ $1$ ]],
                 let (var (v2), [[ $2$ ]],
                      let (var (v3), APPLY (is2ns, var (v1)),
                           seq (var (v3), var (v2)))));
    }
    ;

OptAttrContSequence_:   empty_seq
    =
    {
        /*
         * An empty attribute value shall result in an empty string.
         * [XQ, 3.7.1.1].
         */
        [[ $$ ]] = str ("");
    }
    ;

OptAttrContSequence_:   AttrContSequence;

AttrContSequence:       contseq (AttrEnclosedExpr, empty_seq)
    =
    {
        /*
         * The first argument is already a string (after rule
         * AttrEnclosedExpr). So we can just return that.
         */
        [[ $$ ]] = [[ $1$ ]];
    }
    ;
AttrContSequence:       contseq (AttrEnclosedExpr, AttrContSequence)
    =
    {
        /*
         * Both arguments have type xs:string after they had been
         * converted in the rule AttrEnclosedExpr. All that is left
         * to do is to concatenate them.
         *
         * let $v1 := [[ e1 ]] return
         *   let $v2 := [[ e2 ]] return
         *     fn:concat ($v1, $v2)
         */
        PFvar_t *v1 = new_var (NULL);
        PFvar_t *v2 = new_var (NULL);
        PFfun_t *concat = function (PFqname (PFns_fn, "concat"));

        [[ $$ ]] =
            let (var (v1), [[ $1$ ]],
                 let (var (v2), [[ $2$ ]],
                     APPLY (concat, var (v1), var (v2))));
    }
    ;

AttrEnclosedExpr:       Expr
    =
    {
        /*
         * Convert enclosed expression in attribute values as described in
         * [XQ, Section 3.7.1.1]:
         *
         *  a. Apply atomization (fn:data) to the enclosed expression.
         *  b. Cast each atomic value in the resulting sequence to a string
         *  c. Merge all the resulting strings with a space character
         *     inbetween.
         *
         * let $v1 := [[ $1$ ]] return
         *    let $v2 := for $v3 in $v1 return fn:string($v3)
         *       return
         *       fn:string-join( $v2, " " )
         */
        PFcnode_t *ret = NULL;
        PFvar_t *v1 = new_var (NULL);
        PFvar_t *v2 = new_var (NULL);
        PFvar_t *v3 = new_var (NULL);
        PFfun_t *string = function (PFqname (PFns_fn, "string"));
        PFfun_t *string_join = function (PFqname (PFns_fn, "string-join"));

        ret =
            let (var (v1), [[ $$ ]],
                 let (var (v2),
                      for_ (var (v3), nil (), var (v1),
                         APPLY (string, var (v3))),
                      APPLY (string_join, var (v2), str (" "))));

        [[ $$ ]] = ret;
    }
    ;

PathExpr:               StepExpr;
PathExpr:               LocationPath_;

LocationPath_:          root_
    =
    {
        PFfun_t *_root = function (PFqname (PFns_fn, "root"));
        if (fs_dot)
        {
            [[ $$ ]] = APPLY (_root, var (fs_dot));
        }
        else
        {
            PFoops_loc (OOPS_NOCONTEXT, ($$)->loc,
                        "``.'' is unbound");
        }
    }
    ;
LocationPath_:          dot
    =
    {
        if (fs_dot)
            [[ $$ ]] = var (fs_dot);
        else
            PFoops_loc (OOPS_NOCONTEXT, ($$)->loc,
                        "``.'' is unbound");
    }
    ;
LocationPath_:          locpath (LocationStep_, LocationPath_)
    =
    {
        PFvar_t *v = new_var (0);

        /* This is used to isolate ``location step only'' path
         * expressions, i.e., path expressions of the form
         *
         *       s0/s1/.../sn
         *
         * with the si (i = 0...n) being location steps of the
         * form axis::node-test (see `LocationStep' label).
         * Such paths are subject to (chains of) staircase joins.
         */
        [[ $$ ]] = let (var (v), [[ $2$ ]], 
                        locsteps ([[ $1$ ]], var (v)));
    }
    ;
LocationPath_:          locpath (LocationStep_, StepExpr)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFfun_t *pf_distinct_doc_order = 
            function (PFqname (PFns_pf, 
                               "distinct-doc-order"));
        
        [[ $$ ]] =
        let (var (v1), [[ $2$ ]],
             let (var (v2), APPLY (pf_distinct_doc_order, var (v1)),
                  locsteps ([[ $1$ ]], var (v2))));
    }
    ;
LocationPath_:          locpath (StepExpr, LocationPath_)
    { TOPDOWN; }
    =
    {
        PFvar_t *v = new_var (0);
        PFvar_t *dot;
        PFvar_t *position;
        PFvar_t *last;

        PFfun_t *count = function (PFqname (PFns_fn, "count"));
        
        tDO ($%2$);

        /* save context items $fs:dot, $fs:position, $fs:last
           and establish new context item */
        dot = fs_dot;
        fs_dot = new_var ("dot");
        position = fs_position;
        fs_position = new_var ("pos");
        last = fs_last;
        fs_last = new_var ("lst");
        
        tDO ($%1$);
        
        [[ $$ ]] =
        let (var (v), [[ $2$ ]],
             let (var(fs_last), APPLY (count, var(v)),
                  for_ (var (fs_dot),
                        var (fs_position),
                        var (v),
                        [[ $1$ ]])));
             
        /* restore context items $fs:dot, $fs:position, and $fs:last */
        fs_dot = dot;
        fs_position = position;
        fs_last = last;
    }
    ;
LocationPath_:          locpath (StepExpr, StepExpr)
    { TOPDOWN; }
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *dot;
        PFvar_t *position;
        PFvar_t *last;
        PFfun_t *pf_distinct_doc_order = 
            function (PFqname (PFns_pf, 
                               "distinct-doc-order"));
        
        tDO ($%2$);

        /* save context items $fs:dot, $fs:position, $fs:last
           and establish new context item */
        dot = fs_dot;
        fs_dot = new_var ("dot");
        position = fs_position;
        fs_position = new_var ("pos");
        last = fs_last;
        fs_last = new_var ("lst");

        PFfun_t *count = function (PFqname (PFns_fn, "count"));
        
        tDO ($%1$);
        
        [[ $$ ]] =
        let (var (v1), [[ $2$ ]],
             let (var(fs_last), APPLY (count, var(v1)),
                  let (var (v2), APPLY (pf_distinct_doc_order, var (v1)),
                       for_ (var (fs_dot),
                             var (fs_position),
                             var (v2),
                             [[ $1$ ]]))));
             
        /* restore context items $fs:dot, $fs:position, and $fs:last */
        fs_dot = dot;
        fs_position = position;
        fs_last = last;
    }
    ;

StepExpr:               PrimaryExpr;
StepExpr:               pred (PathExpr, Expr)
    { TOPDOWN; }
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *v3 = new_var (0);
        PFvar_t *v4 = new_var (0);
        PFvar_t *v5 = new_var (0);
        PFvar_t *dot;
        PFvar_t *position;
        PFvar_t *last;

        PFfun_t *fn_eq = function (PFqname (PFns_op, "eq"));
        PFfun_t *count = function (PFqname (PFns_fn, "count"));

        tDO ($%1$);

        /* save context items $fs:dot, $fs:position, $fs:last
           and establish new context item */
        dot = fs_dot;
        fs_dot = new_var ("dot");
        position = fs_position;
        fs_position = new_var ("pos");
        last = fs_last;
        fs_last = new_var ("lst");

        tDO ($%2$);

        [[ $$ ]] = 
        let (var (v1), [[ $1$ ]],
             let (var(fs_last), APPLY (count, var(v1)),
                  for_ (var(fs_dot), var (fs_position), var(v1),
                        let (var (v2), [[ $2$ ]],
                             let (var (v3), 
                                  typeswitch 
                                      (var (v2),
                                       cases
                                         (case_
                                            (seqtype (PFty_numeric ()),
             /* instead of numeric-eq ()  */ let (var (v5),
             /* proposed by the W3C a     */      seqcast (seqtype (PFty_integer ()),
             /* cast to integer is chosen */               var (v2)),
                                                  let (var (v4),
                                                       apply (fn_eq,
                                                              arg (var (v5),
                                                                   arg (var (fs_position),
                                                                        nil ()))),
                                                       var (v4)))),
                                          nil ()),
                                       ebv (var (v2))),
                                  ifthenelse (var (v3), 
                                              var (fs_dot), empty ()))))));

        /* restore context items $fs:dot, $fs:position, and $fs:last */
        fs_dot = dot;
        fs_position = position;
        fs_last = last;
    }
    ;

LocationStep_:          step (NodeTest)
    =
    {
        [[ $$ ]] = step (($$)->sem.axis, [[ $1$ ]]);
    }
    ;

NodeTest:               KindTest;
NodeTest:               NameTest;

KindTest:               kindt (Nil_)
    =
    {
        [[ $$ ]] = kindt (($$)->sem.kind, [[ $1$ ]]);
        
    }
    ;
KindTest:               kindt (StringLiteral)
    =
    {
        [[ $$ ]] = kindt (($$)->sem.kind, [[ $1$ ]]);
        
    }
    ;

NameTest:               namet
    =
    {
        [[ $$ ]] = namet (($$)->sem.qname);
    } 
    ;

PrimaryExpr:            Literal;
PrimaryExpr:            FunctionCall;
PrimaryExpr:            Var_;

PrimaryExpr:            ParenthesizedExpr;

Literal:                NumericLiteral;
Literal:                StringLiteral;

NumericLiteral:         IntegerLiteral;
NumericLiteral:         DecimalLiteral;
NumericLiteral:         DoubleLiteral;

IntegerLiteral:         lit_int
    =
    {
        [[ $$ ]] = num (($$)->sem.num);
    }
    ; 
DecimalLiteral:         lit_dec
    =
    {
        [[ $$ ]] = dec (($$)->sem.dec);
    }
    ; 
DoubleLiteral:          lit_dbl
    =
    {
        [[ $$ ]] = dbl (($$)->sem.dbl);
    }
    ; 

StringLiteral:          lit_str
    =
    {
        [[ $$ ]] = str (($$)->sem.str);
    }
    ; 

/* special rule to translate built-in functions (fn:root(), fn:last(),
   and fn:position()) according to the current context set
   (here is the only place where we have the context set information
   available) */
FunctionCall:           apply (Nil_)
    { TOPDOWN; }
    =
    {
        PFfun_t *fun = ($$)->sem.fun;

        /* apply context item $fs:dot to retrieve context roots 
           or string values */
        if (!PFqname_eq(fun->qname,PFqname (PFns_fn,"root")) ||
            !PFqname_eq(fun->qname,PFqname (PFns_fn,"string")))
        {
            if (fs_dot)
            {
                [[ $$ ]] = APPLY (fun, var (fs_dot));
            }
            /* handle undefined context set */
            else
            {
                PFoops_loc (OOPS_NOCONTEXT, ($$)->loc,
                            "``.'' is unbound");
            }
        }
        /* replace function call fn:last() by context item $fs_last */
        else if (!PFqname_eq(fun->qname,PFqname (PFns_fn,"last")))
        {
            if (fs_last)
            {
                [[ $$ ]] = var (fs_last);
            }
            /* handle undefined context set */
            else
            {
                PFoops_loc (OOPS_NOCONTEXT, ($$)->loc,
                            "``.'' is unbound");
            }
        }
        /* replace function call fn:position() by context item $fs_postion */
        else if(!PFqname_eq(fun->qname,PFqname (PFns_fn,"position")))
        {
            if (fs_position)
            {
                [[ $$ ]] = var (fs_position);
            }
            /* handle undefined context set */
            else
            {
                PFoops_loc (OOPS_NOCONTEXT, ($$)->loc,
                            "``.'' is unbound");
            }
        }
        else
        {
            [[ $$ ]] = apply (fun, nil ());
        }
    }
    ;

FunctionCall:           apply (FuncArgList_)
    { TOPDOWN; }
    =
    {
        *(PFfun_t **) PFarray_add (funs)   = ($$)->sem.fun;
        *(PFcnode_t **) PFarray_add (args) = nil ();

        tDO ($%1$);

        PFarray_del (funs);
        PFarray_del (args);

        [[ $$ ]] = [[ $1$ ]];
    }
    ;

FuncArgList_:           args (Expr, FuncArgList_)
    { TOPDOWN; }
    =
    {
        /* NB: we are ``casting'' the function argument to `none' here,
         * the type checker will replace this with the expected argument
         * type.
         *
         * See W3C XQuery, 5.1.4
         */
        PFvar_t *v = new_var (0);

        tDO ($%1$);

        *(PFcnode_t **) PFarray_top (args) =
            arg (var (v),
                 *(PFcnode_t **) PFarray_top (args));

        tDO ($%2$);
       
        [[ $$ ]] = let (var (v), [[ $1$ ]], [[ $2$ ]]);
    }
    ;
FuncArgList_:           Nil_
    =
    {
        [[ $$ ]] = apply (*(PFfun_t **) PFarray_top (funs),
                          *(PFcnode_t **) PFarray_top (args));
    }
    ;

XMLSpaceDecl:           xmls_decl;

DefaultCollationDecl:   coll_decl (StringLiteral);

NamespaceDecl:          ns_decl (StringLiteral);

DefaultNamespaceDecl:   ens_decl (StringLiteral);
DefaultNamespaceDecl:   fns_decl (StringLiteral);

FunctionDefn:           fun (OptParamList_, OptAs_, ExprSequence);

OptParamList_:          Nil_;
OptParamList_:          ParamList;

ParamList:              params (Param, Nil_);
ParamList:              params (Param, ParamList);

Param:                  param (OptTypeDeclaration_, Var_);

OptAs_:                 Nil_;
OptAs_:                 SequenceType;

SchemaImport:           schm_imp (StringLiteral, OptSchemaLoc_);

OptSchemaLoc_:          Nil_;
OptSchemaLoc_:          StringLiteral;

Var_:                   var
    =
    {
        [[ $$ ]] = var (($$)->sem.var);
    }
    ;

Nil_:                   nil
    =
    {
        [[ $$ ]] = nil ();
    }
    ;

/* vim:set shiftwidth=4 expandtab filetype=c: */

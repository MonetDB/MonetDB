/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

prologue { 

/*
 * XQuery Formal Semantics: mapping XQuery to XQuery Core.
 *
 * In this file, a reference to W3C XQuery refers to the W3C WD
 * XQuery 1.0 and XPath 2.0 Formal Semantics, Draft Nov 15, 2002
 * http://www.w3.org/TR/2002/WD-query-semantics-20021115/
 *
 * $Id$
 */
  
/* Auxiliary routines related to the formal semantics are located in
 * this separate included file to facilitate automated documentation
 * via doxygen.
 */	
#include "fs_impl.c"

/* m4: make __core__( e ) a synonym for (e)->core (core equivalent of e),
 * see PFpnode_t in include/abssyn.h
 */


/* element/attribute constructor and content
 */
PFty_t (*elem_attr) (PFqname_t, PFty_t);
PFty_t any;

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
      var          /* `real' scoped variable */
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
      untyped_ty   /* untyped type */
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
                        { assert ($$);  /* avoid root unused warning */ }
    =
    { 
        /* FIXME: this ignores the QueryProlog */
        ($$ )->core = ($2$ )->core;
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
        
        ($$ )->core =
        let (var (v1), ($1$ )->core,
             let (var (v2), ($2$ )->core,
                  seq (var (v1), var (v2))));
    }
    ;
ExprSequence:           exprseq (Expr, ExprSequence)
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        
        ($$ )->core =
        let (var (v1), ($1$ )->core,
             let (var (v2), ($2$ )->core,
                  seq (var (v1), var (v2))));
    }
    ;

EmptySequence_:         empty_seq
    =
    {
        ($$ )->core = empty ();
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

        ($$ )->core = let (var (v1), ebv (($1$ )->core),
                        let (var (v2), ebv (($2$ )->core),
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

        ($$ )->core = let (var (v1), ebv (($1$ )->core),
                        let (var (v2), ebv (($2$ )->core),
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
        
        ($$ )->core =
        let (($1.1.2$ )->core, ($1.1.3$ )->core,
             let (var (v), ($2$ )->core,
                  ifthenelse (var (v), 
                              ($4$ )->core, empty ())));
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

        ($$ )->core = let (var (v1), ($1.1.3$ )->core,
                        let (($1.1.2$ )->core, 
                             proof (var (v1), ($1.1.1$ )->core, 
                                    seqcast (($1.1.1$ )->core, var (v1))),
                             let (var (v2), ($2$ )->core,
                                  ifthenelse (var (v2),
                                              ($4$ )->core,
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
        
        ($$ )->core =
        let (($1.1.2$ )->core, ($1.1.3$ )->core,
             let (var (v), ($2$ )->core,
                  ifthenelse (var (v), 
                              ($4$ )->core, empty ())));
        
        PFinfo_loc (OOPS_WARN_NOTSUPPORTED,
                    ($3$)->loc,
                    "order by (ignored)");
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

        ($$ )->core = let (var (v1), ($1.1.3$ )->core,
                        let (($1.1.2$ )->core, 
                             proof (var (v1), ($1.1.1$ )->core, 
                                    seqcast (($1.1.1$ )->core, var (v1))),
                             let (var (v2), ($2$ )->core,
                                  ifthenelse (var (v2),
                                              ($4$ )->core,
                                              empty ()))));
        
        PFinfo_loc (OOPS_WARN_NOTSUPPORTED,
                    ($3$)->loc,
                    "`order by' (ignored)");
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
        
        ($$ )->core = let (var (v1), ($1.1.4$ )->core,
                        let (var (v2), seq (var (v1), empty ()),
                             for_ (($1.1.3$ )->core, ($1.1.2$ )->core, var (v2),
                                   let (var (v3), ($2$ )->core,
                                        ifthenelse (var (v3), 
                                                    ($4$ )->core, empty ())))));
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

        ($$ )->core =
        let (var (v1), ($1.1.4$ )->core,
             let (var (v2), seq (var (v1), empty ()),
                  for_ (var (v3), ($1.1.2$ )->core, var (v2),
                        let (($1.1.3$ )->core,
                             proof (var (v3), ($1.1.1$ )->core,
                                    seqcast (($1.1.1$ )->core, var (v3))),
                             let (var (v4), ($2$ )->core,
                                  ifthenelse (var (v4), 
                                              ($4$ )->core, empty ()))))));
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
        
        ($$ )->core = let (var (v1), ($1.1.4$ )->core,
                        let (var (v2), seq (var (v1), empty ()),
                             for_ (($1.1.3$ )->core, ($1.1.2$ )->core, var (v1),
                                   let (var (v3), ($2$ )->core,
                                        ifthenelse (var (v3), 
                                                    ($4$ )->core, empty ())))));
        
        PFinfo_loc (OOPS_WARN_NOTSUPPORTED,
                    ($3$)->loc,
                    "`order by' (ignored)");
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
        
        ($$ )->core =
        let (var (v1), ($1.1.4$ )->core,
             let (var (v2), seq (var (v1), empty ()),
                  for_ (var (v3), ($1.1.2$ )->core, var (v2),
                        let (($1.1.3$ )->core,
                             proof (var (v3), ($1.1.1$ )->core,
                                    seqcast (($1.1.1$ )->core, var (v3))),
                             let (var (v4), ($2$ )->core,
                                  ifthenelse (var (v4), 
                                              ($4$ )->core, empty ()))))));

        PFinfo_loc (OOPS_WARN_NOTSUPPORTED,
                    ($3$)->loc,
                    "`order by' (ignored)");
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
        ($$ )->core = true_ ();
    }
    ;
OptWhereClause_:        WhereClause
    =
    {
        ($$ )->core = ebv (($$ )->core);
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

        ($$ )->core = let (var (v1), ($1.1.4$ )->core,
                        let (var (v2), 
                             for_ (($1.1.3$ )->core, nil (), 
                                   var (v1), 
                                   let (var (v3), ebv (($2$ )->core),
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

        ($$ )->core = 
        let (var (v1), ($1.1.4$ )->core,
             let (var (v2), 
                  for_ (($1.1.3$ )->core, nil (), var (v1), 
                        typeswitch (($1.1.3$ )->core,
                            cases (case_ (($1.1.1$ )->core,
                                          let (var (v3), ebv (($2$ )->core),
                                               ifthenelse (var (v3),
                                                           num (1),
                                                           empty ()))),
                                   nil ()),
                            error_loc (($$)->loc, 
                                       "$%s does not match required type %s",
                                       PFqname_str (($1.1.3$)->sem.var->qname),
                                       PFty_str (($1.1.1$ )->core->sem.type)))),
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

        ($$ )->core = let (var (v1), ($1.1.4$ )->core,
                        let (var (v2), 
                             for_ (($1.1.3$ )->core, nil (), 
                                   var (v1), 
                                   let (var (v3), ebv (($2$ )->core),
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

        ($$ )->core = 
        let (var (v1), ($1.1.4$ )->core,
             let (var (v2), 
                  for_ (($1.1.3$ )->core, nil (), var (v1), 
                        typeswitch (($1.1.3$ )->core,
                            cases (case_ (($1.1.1$ )->core,
                                          let (var (v3), ebv (($2$ )->core),
                                               let (var (v4), 
                                                    APPLY (fn_not, var (v3)),
                                                    ifthenelse (var (v4),
                                                                num (1),
                                                                empty ())))),
                                   nil ()),
                            error_loc (($$)->loc, 
                                       "$%s does not match required type %s",
                                       PFqname_str (($1.1.3$)->sem.var->qname),
                                       PFty_str (($1.1.1$ )->core->sem.type)))),
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

        ($$ )->core = let (var (v), ($1$ )->core,
                        typeswitch (var (v),
                                    cases (case_ (($2.1.1$ )->core, 
                                                  let (($2.1.2$ )->core, 
                                                       seqcast (($2.1.1$ )->core,
                                                                var (v)),
                                                       ($2.1.3$ )->core)),
                                           nil ()),
                                    let (($3$ )->core, var (v),
                                         ($4$ )->core)));
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

        ($$ )->core = var (fs_new);
    }
    ;
OptCaseVar_:            Var_;

SingleType:             seq_ty (AtomType)
    =
    {
        switch (($$)->sem.oci) {
        case p_one:
            ($$ )->core = ($1$ )->core;
            break;
        case p_zero_or_one:
            ($$ )->core = 
            seqtype (PFty_opt (($1$ )->core->sem.type));
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
            ($$ )->core = ($1$ )->core;
            break;
        case p_zero_or_one:
            ($$ )->core = 
            seqtype (PFty_opt (($1$ )->core->sem.type));
            break;
        case p_zero_or_more:
            ($$ )->core = 
            seqtype (PFty_star (($1$ )->core->sem.type));
            break;
        case p_one_or_more:
            ($$ )->core = 
            seqtype (PFty_plus (($1$ )->core->sem.type));
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
        ($$ )->core = seqtype (PFty_empty ());
    }
    ;

ItemType:               node_ty (Nil_)
    =
    {
        switch (($$)->sem.kind) {
        case p_kind_node:
            /* node */
            ($$ )->core = seqtype (PFty_xs_anyNode ());
            break;
        case p_kind_comment:
            /* comment */
            ($$ )->core = seqtype (PFty_comm ());
            break;
        case p_kind_text:
            /* text */
            ($$ )->core = seqtype (PFty_text ());
            break;
        case p_kind_pi:
            /* processing-instruction */
            ($$ )->core = seqtype (PFty_pi ());
            break;
        case p_kind_doc:
            /* document */
            ($$ )->core = seqtype (PFty_doc (PFty_xs_anyType ()));
            break;
        case p_kind_elem:
            /* element */
            ($$ )->core =
            seqtype (PFty_elem (PFqname (PFns_wild, 0), 
                                PFty_xs_anyType ()));
            break;
        case p_kind_attr:
            /* attribute */
            ($$ )->core = 
            seqtype (PFty_attr (PFqname (PFns_wild, 0), 
                                PFty_xs_anySimpleType ()));
            break;
        default:
            PFoops (OOPS_FATAL,
                    "illegal node kind `%s' in node type",
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
            PFoops (OOPS_FATAL, "illegal node kind `%s' in type",
                    p_id[($$)->sem.kind]);
        }

        tDO ($%1$);

        ($$ )->core = ($1$ )->core;
    }
    ;
ItemType:               item_ty (Nil_)
    =
    {
        /* item */
        ($$ )->core = seqtype (PFty_xs_anyItem ()); 
    }
    ;
ItemType:               AtomType;
ItemType:               untyped_ty (Nil_)
    =
    {
        /* untyped */
        ($$ )->core = seqtype (PFty_untyped ()); 
    }
    ;
ItemType:               atomval_ty (Nil_)
    =
    {
        /* atomic value */
        ($$ )->core = seqtype (PFty_xs_anySimpleType ()); 
    }
    ;

ElemOrAttrType:         req_ty (req_name, SchemaType)
    =
    {   /* element/attribute qn of type qn */
        ($$ )->core = seqtype (elem_attr ($1$->sem.qname,
                                       ($2$ )->core->sem.type));
    }
    ;
ElemOrAttrType:         req_ty (Nil_, SchemaType)
    =
    {   /* element/attribute of type qn */
        ($$ )->core = seqtype (elem_attr (PFqname (PFns_wild, 0),
                                       ($2$ )->core->sem.type));
    }
    ;
ElemOrAttrType:         req_ty (req_name, Nil_)
    =
    {   /* element/attribute qn */
        ($$ )->core = seqtype (elem_attr ($1$->sem.qname, 
                                       any));
    }
    ;
ElemOrAttrType:         req_ty (req_name, SchemaContext)
    =
    {   /* FIXME: we do not implement schema contexts for now */

        /* element/attribute qn context ... */
        ($$ )->core = seqtype (elem_attr ($1$->sem.qname, 
                                       any));

        PFinfo_loc (OOPS_WARN_NOTSUPPORTED,
                    ($2$)->loc,
                    "schema context (assuming content type `%s')",
                    PFty_str (any));
    }
    ;

SchemaType:             named_ty
    =
    {
        /* is the referenced type a known schema type definition? */
        if (! PFty_schema (PFty_named (($$)->sem.qname)))
            PFoops_loc (OOPS_TYPENOTDEF, ($$)->loc,
                        "`%s'",
                        PFqname_str (($$)->sem.qname));

        ($$ )->core = seqtype (PFty_named (($$)->sem.qname));
    }
    ;

AtomType:               atom_ty (Nil_)
    =
    {
        /* is the referenced type a known schema type? */
        if (! PFty_schema (PFty_named (($$)->sem.qname)))
            PFoops_loc (OOPS_TYPENOTDEF, ($$)->loc,
                        "`%s'",
                        PFqname_str (($$)->sem.qname));

        ($$ )->core = seqtype (PFty_named (($$)->sem.qname));
    }
    ;

IfExpr:                 InstanceofExpr;
IfExpr:                 if_ (Expr, Expr, Expr)
    =
    {
        PFvar_t *v = new_var (0);

        ($$ )->core = let (var (v), ebv (($1$ )->core),
                        ifthenelse (var (v), ($2$ )->core, ($3$ )->core));
                                 
    }
    ;

InstanceofExpr:         CastableExpr;
InstanceofExpr:         instof (CastableExpr, SequenceType)
    =
    {
        PFvar_t *v = new_var (0);

        ($$ )->core = let (var (v), ($1$ )->core,
                        typeswitch (var (v),
                                    cases (case_ (($2$ )->core, true_ ()),
                                           nil ()),
                                    false_ ()));
    }
    ;

CastableExpr:           ComparisonExpr;
CastableExpr:           castable (ComparisonExpr, SingleType);

ComparisonExpr:         RangeExpr;
ComparisonExpr:         eq (RangeExpr, RangeExpr);
ComparisonExpr:         ne (RangeExpr, RangeExpr);
ComparisonExpr:         lt (RangeExpr, RangeExpr);
ComparisonExpr:         le (RangeExpr, RangeExpr);
ComparisonExpr:         gt (RangeExpr, RangeExpr);
ComparisonExpr:         ge (RangeExpr, RangeExpr);
ComparisonExpr:         val_eq (RangeExpr, RangeExpr);
ComparisonExpr:         val_ne (RangeExpr, RangeExpr);
ComparisonExpr:         val_lt (RangeExpr, RangeExpr);
ComparisonExpr:         val_le (RangeExpr, RangeExpr);
ComparisonExpr:         val_gt (RangeExpr, RangeExpr);
ComparisonExpr:         val_ge (RangeExpr, RangeExpr);
ComparisonExpr:         is (RangeExpr, RangeExpr);
ComparisonExpr:         nis (RangeExpr, RangeExpr);
ComparisonExpr:         ltlt (RangeExpr, RangeExpr);
ComparisonExpr:         gtgt (RangeExpr, RangeExpr);

RangeExpr:              AdditiveExpr;
RangeExpr:              range (AdditiveExpr, AdditiveExpr);

AdditiveExpr:           MultiplicativeExpr;
AdditiveExpr:           plus (AdditiveExpr, MultiplicativeExpr);
AdditiveExpr:           minus (AdditiveExpr, MultiplicativeExpr);

MultiplicativeExpr:     UnaryExpr;
MultiplicativeExpr:     mult (MultiplicativeExpr, UnaryExpr);
MultiplicativeExpr:     div_ (MultiplicativeExpr, UnaryExpr);
MultiplicativeExpr:     idiv (MultiplicativeExpr, UnaryExpr);
MultiplicativeExpr:     mod (MultiplicativeExpr, UnaryExpr);

UnaryExpr:              UnionExpr;
UnaryExpr:              uminus (UnionExpr);
UnaryExpr:              uplus (UnionExpr);

UnionExpr:              IntersectExceptExpr;
UnionExpr:              union_ (UnionExpr, IntersectExceptExpr);

IntersectExceptExpr:    ValueExpr;
IntersectExceptExpr:    intersect (IntersectExceptExpr, UnaryExpr);
IntersectExceptExpr:    except (IntersectExceptExpr, UnaryExpr);

ValueExpr:              ValidateExpr;
ValueExpr:              CastExpr;
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

CastExpr:               cast (SingleType, ParenthesizedExpr);

TreatExpr:              treat (SingleType, ParenthesizedExpr);

ParenthesizedExpr:      OptExprSequence_;

Constructor:            ElementConstructor;
Constructor:            XmlComment;
Constructor:            XmlProcessingInstruction;
Constructor:            DocumentConstructor;
Constructor:            AttributeConstructor;
Constructor:            TextConstructor;

ElementConstructor:     elem (TagName, ElementContent);

AttributeConstructor:   attr (TagName, AttributeValue);

TextConstructor:        text (OptExprSequence_);

DocumentConstructor:    doc (OptExprSequence_);

XmlComment:             comment (StringLiteral);

XmlProcessingInstruction: pi (StringLiteral);
                        
TagName:                tag;
TagName:                Expr;

ElementContent:         OptExprSequence_;

AttributeValue:         OptExprSequence_;

PathExpr:               StepExpr;
PathExpr:               LocationPath_;

LocationPath_:          root_
    =
    {
        ($$ )->core = _root ();
    }
    ;
LocationPath_:          dot
    =
    {
        if (fs_dot)
            ($$ )->core = var (fs_dot);
        else
            PFoops_loc (OOPS_NOCONTEXT, ($$)->loc,
                        "`.' is unbound");
    }
    ;
LocationPath_:          locpath (LocationStep_, LocationPath_)
    =
    {
        PFvar_t *v = new_var (0);

        /* This is used to isolate `location step only' path
         * expressions, i.e., path expressions of the form
         *
         *       s0/s1/.../sn
         *
         * with the si (i = 0...n) being location steps of the
         * form axis::node-test (see LocationStep label).
         * Such paths are subject to (chains of) staircase joins.
         */
        ($$ )->core = let (var (v), ($2$ )->core, 
                        locsteps (($1$ )->core, var (v)));
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
        
        ($$ )->core =
        let (var (v1), ($2$ )->core,
             let (var (v2), APPLY (pf_distinct_doc_order, var (v1)),
                  locsteps (($1$ )->core, var (v2))));
    }
    ;
LocationPath_:          locpath (StepExpr, LocationPath_)
    { TOPDOWN; }
    =
    {
        PFvar_t *v = new_var (0);
        PFvar_t *dot;
        
        tDO ($%2$);

        /* save $fs:dot, establish new context item */
        dot = fs_dot;
        fs_dot = new_var ("dot");
        
        tDO ($%1$);
        
        ($$ )->core =
        let (var (v), ($2$ )->core,
             for_ (var (fs_dot),
                   nil (),
                   var (v),
                   ($1$ )->core));
             
        /* restore $fs:dot */
        fs_dot = dot;
    }
    ;
LocationPath_:          locpath (StepExpr, StepExpr)
    { TOPDOWN; }
    =
    {
        PFvar_t *v1 = new_var (0);
        PFvar_t *v2 = new_var (0);
        PFvar_t *dot;
        PFfun_t *pf_distinct_doc_order = 
            function (PFqname (PFns_pf, 
                               "distinct-doc-order"));
        
        tDO ($%2$);

        /* save $fs:dot, establish new context item */
        dot = fs_dot;
        fs_dot = new_var ("dot");
        
        tDO ($%1$);
        
        ($$ )->core =
        let (var (v1), ($2$ )->core,
             let (var (v2), APPLY (pf_distinct_doc_order, var (v1)),
                  for_ (var (fs_dot),
                        nil (),
                        var (v2),
                        ($1$ )->core)));
             
        /* restore $fs:dot */
        fs_dot = dot;
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
        PFvar_t *dot = new_var ("dot");
        PFvar_t *fs_position = new_var ("pos");

        tDO ($%1$);

        /* save $fs:dot, establish new context item */
        dot = fs_dot;
        fs_dot = new_var ("dot");

        tDO ($%2$);

        /* FIXME: replace int_eq () with core equivalent for
         * integer equality
         */
        ($$ )->core = 
        let (var (v1), ($1$ )->core,
             let (var (v5), seq (var (v1), empty ()),
                  for_ (var(fs_dot), var (fs_position), var(v5),
                        let (var (v2), ($2$ )->core,
                             let (var (v3), 
                                  typeswitch (var (v2),
                                      cases (case_ (seqtype (PFty_numeric ()),
                                                    let (var (v4),
                                                         int_eq (var (v2), 
                                                                 var (fs_position)),
                                                         var (v4))),
                                             nil ()),
                                      ebv (var (v2))),
                                  ifthenelse (var (v3), 
                                              var (fs_dot), empty ()))))));

        /* restore $fs:dot */
        fs_dot = dot;
    }
    ;

LocationStep_:          step (NodeTest)
    =
    {
        ($$ )->core = step (($$)->sem.axis, ($1$ )->core);
    }
    ;

NodeTest:               KindTest;
NodeTest:               NameTest;

KindTest:               kindt (Nil_)
    =
    {
        ($$ )->core = kindt (($$)->sem.kind, ($1$ )->core);
        
    }
    ;
KindTest:               kindt (StringLiteral)
    =
    {
        ($$ )->core = kindt (($$)->sem.kind, ($1$ )->core);
        
    }
    ;

NameTest:               namet
    =
    {
        ($$ )->core = namet (($$)->sem.qname);
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
        ($$ )->core = num (($$)->sem.num);
    }
    ; 
DecimalLiteral:         lit_dec
    =
    {
        ($$ )->core = dec (($$)->sem.dec);
    }
    ; 
DoubleLiteral:          lit_dbl
    =
    {
        ($$ )->core = dbl (($$)->sem.dbl);
    }
    ; 

StringLiteral:          lit_str
    =
    {
        ($$ )->core = str (($$)->sem.str);
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

        ($$ )->core = ($1$ )->core;
    }
    ;

FuncArgList_:           args (Expr, FuncArgList_)
    { TOPDOWN; }
    =
    {
        /* NB: we are `casting' the function argument to none here,
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
       
        ($$ )->core = let (var (v), ($1$ )->core, ($2$ )->core);
    }
    ;
FuncArgList_:           Nil_
    =
    {
        ($$ )->core = apply (*(PFfun_t **) PFarray_top (funs),
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
        ($$ )->core = var (($$)->sem.var);
    }
    ;

Nil_:                   nil
    =
    {
        ($$ )->core = nil ();
    }
    ;

/* vim:set shiftwidth=4 expandtab filetype=c: */

prologue { 

/*
 * Include all function declarations and definitions that reside
 * in a separate file to document them nicely with doxygen.
 * If you need further functions, also put them into norm_impl.c.
 */
#include "norm_impl.c"

/* nil (), wire*() */
#include "abssyn.h"

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
      varref       /* variable reference (no scoping yet) */
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
      fun_ref      /* e1 (e2, ...) (function application) */
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
      fun_decl     /* function declaration */
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
      Expr
      OrderSpecList
      OrExpr
      AndExpr
      FLWRExpr
      ForLetClauses_
      ForLetClause_
      ForClause
      LetClause
      VarBindings_
      VarBinding_
      LetBinding_
      OptPositionalVar_
      PositionalVar
      OptTypeDeclaration_
      TypeDeclaration
      OptWhereClause_
      WhereClause
      OptOrderByClause_
      OrderByClause
      QuantifiedExpr
      TypeswitchExpr
      OptVarName_
      CaseClauses_
      CaseClause
      SingleType
      SequenceType
      ItemType
      ElemAtt_
      OptElemOrAttrType_
      ElemOrAttrType
      SchemaType
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
      Context_
      PathExpr
      RelativePathExpr
      StepExpr
      Step_
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
      OptFuncArgList_
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
      OptAs_;

Query:		    	xquery (QueryProlog, QueryBody)
                        { assert ($$);    /* suppress compiler warning */ };

QueryProlog:            prolog (DeclsImports_, FunctionDefns_);

DeclsImports_:          nil;
DeclsImports_:          decl_imps (NamespaceDecl, DeclsImports_);
DeclsImports_:          decl_imps (XMLSpaceDecl, DeclsImports_);
DeclsImports_:          decl_imps (DefaultNamespaceDecl, DeclsImports_);
DeclsImports_:          decl_imps (DefaultCollationDecl, DeclsImports_);
DeclsImports_:          decl_imps (SchemaImport, DeclsImports_);

FunctionDefns_:         nil;
FunctionDefns_:         fun_decls (FunctionDefn, FunctionDefns_);

QueryBody:              OptExprSequence_;

OptExprSequence_:       empty_seq;
OptExprSequence_:       ExprSequence;

ExprSequence:           exprseq (Expr, empty_seq);
ExprSequence:           exprseq (empty_seq, Expr)
                        {
                          cost = 1;
			  REWRITE;
                        } = 
                        {
                          /* in case there is an empty sequence as 
                           * first child of an exprseq node, let the
                           * second child of this node become the node
                           * itself. 
                           */
                          return $2$;
                        };
ExprSequence:           exprseq (Expr, ExprSequence);
ExprSequence:           exprseq (ExprSequence, Expr)
                        {
                          REWRITE;
                        } = 
                        { /* Rewrite expression sequence to be right-deep:
                           * concatenate the sequence and the expression
                           * (discarding the exprseq root).
                           */
			  return concat (p_exprseq, $1$, $2$);
			};

Expr:                   OrExpr;

OrExpr:                 AndExpr;
OrExpr:                 or (OrExpr, AndExpr);

AndExpr:                FLWRExpr;
AndExpr:                and (AndExpr, FLWRExpr);

FLWRExpr:               QuantifiedExpr;
FLWRExpr:               flwr (binds (ForLetClause_, nil), 
			      OptWhereClause_, 
			      OptOrderByClause_,
			      FLWRExpr);
FLWRExpr:	        flwr (binds (ForLetClause_,
                                     ForLetClauses_),
			      OptWhereClause_,
			      OptOrderByClause_,
			      FLWRExpr) 
                        { 
			  REWRITE;
                        } =
                        {
			  /* Current parse tree situation: multi-bind FLWR
			   * (may bind any number n >= 2 variables):
			   *    
			   * for $a as t1 at $b in e1, $c as t2 at $d in e2
                           *     order by o
			   *     where p return e0
			   *
			   *               flwr
			   *              /| | \ 
			   *         binds p o  e0
			   *        /    \
			   *     bind     \
			   *    / || \     \
			   *  t1  ba e1   binds
			   *               /   \
			   *             bind   nil
			   *            / || \
			   *          t2  dc  e2
			   *                 
			   * Rewrite this into n nested single-bind FLWRs:
			   *
			   * for $a as t1 at $b in e1,
			   *     return for $c as t2 at $d in e2
                           *            order by o
			   *            where p 
                           *            return e0
			   *
			   *               flwr
			   *              / || \
			   *         binds nil   flwr
			   *         /  \       / | |\
			   *      bind  nil binds p o e0
			   *     / || \      /  \
			   *   t1  ba e1  bind  nil
			   *             / || \
			   *           t2  dc  e2
			   */
			  return p_wire4 (p_flwr, $$->loc,
					  p_wire2 (p_binds, $1.1$->loc,
						   $1.1$, 
						   p_leaf (p_nil, $1.1$->loc)),
					  p_leaf (p_nil, $$->loc),
					  p_leaf (p_nil, $$->loc),
					  p_wire4 (p_flwr, $1.2$->loc,
						   $1.2$,
						   $2$,
						   $3$, 
						   $4$));
                        };

ForLetClauses_:         binds (ForLetClause_, nil);
ForLetClauses_:         binds (ForLetClause_, ForLetClauses_);

ForLetClause_:          ForClause;
ForLetClause_:          LetClause;

ForClause:              VarBinding_;
LetClause:              LetBinding_;                        

VarBinding_:            bind (OptTypeDeclaration_,
			      OptPositionalVar_,
			      varref,
			      Expr);

LetBinding_:            let (OptTypeDeclaration_, varref, Expr);

VarBindings_:           binds (VarBinding_, nil);
VarBindings_:           binds (VarBinding_, VarBindings_);

OptPositionalVar_:      nil;
OptPositionalVar_:      PositionalVar;

PositionalVar:          varref;

OptTypeDeclaration_:    nil;
OptTypeDeclaration_:    TypeDeclaration;

TypeDeclaration:        SequenceType;

OptWhereClause_:        nil;
OptWhereClause_:        WhereClause;

OptOrderByClause_:      nil;
OptOrderByClause_:      OrderByClause;

OrderByClause:          orderby (OrderSpecList);

OrderSpecList:          orderspecs (Expr, nil);
OrderSpecList:          orderspecs (Expr, OrderSpecList);

WhereClause:            Expr;

QuantifiedExpr:         TypeswitchExpr;
QuantifiedExpr:         some (binds (VarBinding_, nil),
			      QuantifiedExpr);
QuantifiedExpr:         some (binds (VarBinding_, VarBindings_),
			      QuantifiedExpr)
                        {
			  REWRITE;
                        } =
                        {
			  /* Current parse tree situation: multi-bind
			   * quantifier some/every, may bind any
			   * number n >= 2 variables):
			   *
			   * some $a as t1 in e1, $b as t2 in e2
			   *      satisfies e0
			   *
			   *                  some
			   *                  /  \
			   *              binds   e0
			   *              /   \
			   *          bind     \ 
			   *        / |  | \    \
			   *      t1 nil a  e1  binds
			   *                     /  \
			   *                 bind    nil  
			   *               / |  | \
			   *             t2 nil b  e2
			   *
			   * Rewrite this into n nested single-bind
			   * quantifier:
			   *   
			   * some $a as t1 in e1
			   *      satisfies some $b as t2 in e2
			   *                satifies e0
			   *
			   *               some
			   *              /    \
			   *         binds      some 
			   *         /  \       /   \
			   *      bind  nil binds    e0
			   *    / |  | \      /  \
			   *  t1 nil a e1  bind   nil
			   *              /|  | \
			   *           t2 nil b  e2
			   */
			  return p_wire2 (p_some, $$->loc,
					  p_wire2 (p_binds, $1.1$->loc,
						   $1.1$, 
						   p_leaf (p_nil, $1.1$->loc)),
					  p_wire2 (p_some, $1.2$->loc,
						   $1.2$,
						   $2$));
                        };

QuantifiedExpr:         every (binds (VarBinding_, nil),
			       QuantifiedExpr);
QuantifiedExpr:         every (binds (VarBinding_, VarBindings_),
			       QuantifiedExpr)
                        {
			  REWRITE;
                        } =
                        {
			  /* Current parse tree situation: multi-bind
			   * quantifier some/every, may bind any
			   * number n >= 2 variables):
			   *
			   * every $a as t1 in e1, $b as t2 in e2
			   *       satisfies e0
			   *
			   *                 every
			   *                  /  \
			   *              binds   e0
			   *              /   \
			   *          bind     \ 
			   *        / |  | \    \
			   *      t1 nil a  e1  binds
			   *                     /  \
			   *                 bind    nil  
			   *               / |  | \
			   *             t2 nil b  e2
			   *
			   * Rewrite this into n nested single-bind
			   * quantifier:
			   *   
			   * every $a as t1 in e1
			   *       satisfies every $b as t2 in e2
			   *                 satifies e0
			   *
			   *              every
			   *              /    \
			   *         binds      every
			   *         /  \       /   \
			   *      bind  nil binds    e0
			   *    / |  | \      /  \
			   *  t1 nil a e1  bind   nil
			   *              /|  | \
			   *           t2 nil b  e2
			   */
			  return p_wire2 (p_every, $$->loc,
					  p_wire2 (p_binds, $1.1$->loc,
						   $1.1$, 
						   p_leaf (p_nil, $1.1$->loc)),
					  p_wire2 (p_every, $1.2$->loc,
						   $1.2$,
						   $2$));
                        };


TypeswitchExpr:         IfExpr;
TypeswitchExpr:         typeswitch (Expr,
                                    cases (CaseClause, nil),
                                    OptVarName_,
                                    TypeswitchExpr);
TypeswitchExpr:         typeswitch (Expr,
                                    cases (CaseClause, CaseClauses_),
                                    OptVarName_,
                                    TypeswitchExpr)
                        {
                          REWRITE;
                        } =
                        {
                          /*
                           * Unnest typeswitch expressions.
                           *
                           * Rewrite
                           *
                           *   typeswitch e1
                           *     case $a as type1    return ret1
                           *     case $b as type2    return ret2
                           *     case $c as type3    return ret3
                           *     ...
                           *     default $d          return def
                           *
                           * to
                           *
                           *   typeswitch e1
                           *     case $a as type1              return ret1
                           *     default $v1 return
                           *       typeswitch $v1
                           *         case $b as type2          return ret2
                           *         default $v2 return
                           *           typeswitch $v2
                           *             case $c as type3      return ret3
                           *             default $v3 return
                           *               typeswitch
                           *                 ...
                           *                   default $d      return def
                           *
                           * That is:
                           *
                           *           typeswitch
                           *          /    |  |   \ 
                           *        e1  cases d   def
                           *           /     \
                           *      case         cases
                           *     /  | \        /     \
                           * type1 $a ret1   case     ...
                           *                /  | \       \
                           *           type2  $b  ret2    nil
                           *
                           * becomes
                           *
                           *           typeswitch
                           *          /    |  |   \
                           *        e1  cases v1   typeswitch
                           *           /    \      /   |  |  \
                           *       case     nil  v1 cases d  def
                           *      /  | \            /   \
                           *  type1 $a ret1       case    ...
                           *                     /  | \      \
                           *                type2  $b  ret2   nil
                           *
                           *  For our core compilation, each typeswitch must
                           * contain exactly one case clause.
                           */
                          PFqname_t v1 = new_varname ("tsw");

                          return p_wire4 (p_typeswitch, $$->loc,
                                          $1$,
                                          p_wire2 (p_cases, $2.1$->loc,
                                                   $2.1$,
                                                   p_leaf (nil, $2.1$->loc)),
                                          varref (v1, $2.1$->loc),
                                          p_wire4 (p_typeswitch, $2.2$->loc,
                                                   varref (v1, $2.1$->loc),
                                                   $2.2$,
                                                   $3$,
                                                   $4$));
                        };

CaseClauses_:           cases (CaseClause, nil);
CaseClauses_:           cases (CaseClause, CaseClauses_); 

CaseClause:             case_ (SequenceType, OptVarName_, Expr);

SingleType:             seq_ty (atom_ty (nil));

SequenceType:           seq_ty (ItemType);
SequenceType:           seq_ty (empty_ty);

ItemType:               ElemAtt_;
ItemType:               node_ty (nil);
ItemType:               item_ty (nil);
ItemType:               atom_ty (nil);
ItemType:               untyped_ty (nil);
ItemType:               atomval_ty (nil);

ElemAtt_:               node_ty (OptElemOrAttrType_);

OptElemOrAttrType_:     nil;
OptElemOrAttrType_:     ElemOrAttrType;

ElemOrAttrType:         req_ty (req_name, SchemaType);
ElemOrAttrType:         req_ty (req_name, OptSchemaContext_);
ElemOrAttrType:         req_ty (nil, SchemaType);

SchemaType:             named_ty;

OptVarName_:            nil;
OptVarName_:            varref;

IfExpr:                 InstanceofExpr;
IfExpr:                 if_ (Expr, Expr, Expr);

InstanceofExpr:         CastableExpr;
InstanceofExpr:         instof (CastableExpr, SequenceType);

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

OptSchemaContext_:      nil;
OptSchemaContext_:      SchemaContext;

SchemaContext:          schm_path (SchemaGlobalContext, SchemaContextSteps_);

SchemaContextSteps_:    nil;
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

XmlComment:             comment (lit_str);

XmlProcessingInstruction: pi (lit_str);
                        
TagName:                tag;
TagName:                Expr;

ElementContent:         OptExprSequence_;

AttributeValue:         OptExprSequence_;

Context_:               PathExpr;
Context_:               dot;

PathExpr:               root_;                           
PathExpr:               RelativePathExpr;

RelativePathExpr:       StepExpr;
RelativePathExpr:       locpath (StepExpr, Context_);

StepExpr:               Step_;
StepExpr:               pred (StepExpr, Expr);
StepExpr:               pred (RelativePathExpr, Expr);

Step_:                  step (NodeTest);
Step_:                  PrimaryExpr;                         

NodeTest:               KindTest;
NodeTest:               NameTest;

KindTest:               kindt (nil);
KindTest:               kindt (lit_str);

NameTest:               namet;

PrimaryExpr:            Literal;
PrimaryExpr:            FunctionCall;
PrimaryExpr:            varref;
PrimaryExpr:            ParenthesizedExpr;

Literal:                NumericLiteral;
Literal:                StringLiteral;

NumericLiteral:         IntegerLiteral;
NumericLiteral:         DecimalLiteral;
NumericLiteral:         DoubleLiteral;

IntegerLiteral:         lit_int;
DecimalLiteral:         lit_dec;
DoubleLiteral:          lit_dbl;

StringLiteral:          lit_str;

FunctionCall:           fun_ref (OptFuncArgList_);

OptFuncArgList_:        nil;
OptFuncArgList_:        FuncArgList_;

FuncArgList_:           args (Expr, nil);
FuncArgList_:           args (Expr, FuncArgList_);

XMLSpaceDecl:           xmls_decl;

DefaultCollationDecl:   coll_decl (lit_str);

NamespaceDecl:          ns_decl (lit_str);

DefaultNamespaceDecl:   ens_decl (lit_str);
DefaultNamespaceDecl:   fns_decl (lit_str);

FunctionDefn:           fun_decl (OptParamList_, OptAs_, ExprSequence);

OptParamList_:          nil;
OptParamList_:          ParamList;

ParamList:              params (Param, nil);
ParamList:              params (Param, ParamList);

Param:                  param (OptTypeDeclaration_, varref);

OptAs_:                 nil;
OptAs_:                 SequenceType;

SchemaImport:           schm_imp (lit_str, OptSchemaLoc_);

OptSchemaLoc_:          nil;
OptSchemaLoc_:          lit_str;


insert {

};

/* vim:set filetype=c: */

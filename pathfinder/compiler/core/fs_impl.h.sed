/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

/** 
 * @file		
 *	
 * XQuery Formal Semantics: mapping XQuery to XQuery Core.
 * Auxiliary routines.  The mapping process itself is
 * twig-based.
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

#include <assert.h>
#include <string.h>

#include "fs.h"

#include "mem.h"

/* PFpnode_t */
#include "abssyn.h"

/* abstract syntax tree node names (p_id[]) */
#include "abssynprint.h"

/* PFcnode_t */
#include "core.h"

/* PSns_t */
#include "ns.h"

/* PFty_t */
#include "types.h"

/* PFvar_t */
#include "variable.h"

/** twig-generated node type identifiers */
#include "fs.symbols.h"

/** twig: type of tree node */
#define TWIG_NODE PFpnode_t

/** twig: max number of children under a parse tree node */
#define TWIG_MAXCHILD PFPNODE_MAXCHILD

static int TWIG_ID[] = {
    [p_plus]         plus,         /* binary + */
    [p_minus]        minus,        /* binary - */
    [p_mult]         mult,         /* * (multiplication) */
    [p_div]          div_,         /* div (division) */
    [p_idiv]         idiv,         /* idiv (integer division) */
    [p_mod]          mod,          /* mod */
    [p_and]          and,          /* and */
    [p_or]           or,           /* or */
    [p_lt]           lt,           /* < (less than) */
    [p_le]           le,           /* <= (less than or equal) */
    [p_gt]           gt,           /* > (greater than) */
    [p_ge]           ge,           /* >= (greater than or equal) */
    [p_eq]           eq,           /* = (equality) */
    [p_ne]           ne,           /* != (inequality) */
    [p_val_lt]       val_lt,       /* lt (value less than) */
    [p_val_le]       val_le,       /* le (value less than or equal) */
    [p_val_gt]       val_gt,       /* gt (value greater than) */
    [p_val_ge]       val_ge,       /* ge (value greter than or equal) */
    [p_val_eq]       val_eq,       /* eq (value equality) */
    [p_val_ne]       val_ne,       /* ne (value inequality) */
    [p_uplus]        uplus,        /* unary + */
    [p_uminus]       uminus,       /* unary - */
    [p_lit_int]      lit_int,      /* integer literal */
    [p_lit_dec]      lit_dec,      /* decimal literal */
    [p_lit_dbl]      lit_dbl,      /* double literal */
    [p_lit_str]      lit_str,      /* string literal */
    [p_is]           is,           /* is (node identity) */
    [p_nis]          nis,          /* isnot (negated node identity) *grin* */
    [p_step]         step,         /* axis step */
    [p_var]          var,          /* ``real'' scoped variable */
    [p_namet]        namet,        /* name test */
    [p_kindt]        kindt,        /* kind test */
    [p_locpath]      locpath,      /* location path */
    [p_root]         root_,        /* / (document root) */
    [p_dot]          dot,          /* current context node */
    [p_ltlt]         ltlt,         /* << (less than in doc order) */
    [p_gtgt]         gtgt,         /* >> (greater in doc order) */
    [p_flwr]         flwr,         /* for-let-where-return */
    [p_binds]        binds,        /* sequence of variable bindings */
    [p_nil]          nil,          /* end-of-sequence marker */
    [p_empty_seq]    empty_seq,    /* end-of-sequence marker */
    [p_bind]         bind,         /* for/some/every variable binding */
    [p_let]          let,          /* let binding */
    [p_exprseq]      exprseq,      /* e1, e2 (expression sequence) */
    [p_range]        range,        /* to (range) */
    [p_union]        union_,       /* union */
    [p_intersect]    intersect,    /* intersect */
    [p_except]       except,       /* except */
    [p_pred]         pred,         /* e1[e2] (predicate) */
    [p_if]           if_,          /* if-then-else */
    [p_some]         some,         /* some (existential quantifier) */
    [p_every]        every,        /* every (universal quantifier) */
    [p_orderby]      orderby,      /* order by */
    [p_orderspecs]   orderspecs,   /* order criteria */
    [p_instof]       instof,       /* instance of */
    [p_seq_ty]       seq_ty,       /* sequence type */
    [p_empty_ty]     empty_ty,     /* empty type */
    [p_node_ty]      node_ty,      /* node type */
    [p_item_ty]      item_ty,      /* item type */
    [p_atom_ty]      atom_ty,      /* named atomic type */
    [p_atomval_ty]   atomval_ty,   /* atomic value type */
    [p_named_ty]     named_ty,     /* named type */ 
    [p_req_ty]       req_ty,       /* required type */
    [p_req_name]     req_name,     /* required name */
    [p_typeswitch]   typeswitch,   /* typeswitch */
    [p_cases]        cases,        /* list of case branches */
    [p_case]         case_,        /* a case branch */
    [p_schm_path]    schm_path,    /* path of schema context steps */
    [p_schm_step]    schm_step,    /* schema context step */
    [p_glob_schm]    glob_schm,    /* global schema */
    [p_glob_schm_ty] glob_schm_ty, /* global schema type */
    [p_castable]     castable,     /* castable */
    [p_cast]         cast,         /* cast as */
    [p_treat]        treat,        /* treat as */
    [p_validate]     validate,     /* validate */
    [p_apply]        apply,        /* e1 (e2, ...) (function application) */
    [p_args]         args,         /* function argument list (actuals) */
    [p_char]         char_,        /* character content */
    [p_doc]          doc,          /* document constructor (document { }) */
    [p_elem]         elem,         /* XML element constructor */
    [p_attr]         attr,         /* XML attribute constructor */
    [p_text]         text,         /* XML text node constructor */
    [p_tag]          tag,          /* (fixed) tag name */
    [p_pi]           pi,           /* <?...?> content */
    [p_comment]      comment,      /* <!--...--> content */
    [p_contseq]      contseq,      /* constructor content sequence */
    [p_xquery]       xquery,       /* root of the query parse tree */
    [p_prolog]       prolog,       /* query prolog */
    [p_decl_imps]    decl_imps,    /* list of declarations and imports */
    [p_xmls_decl]    xmls_decl,    /* xmlspace declaration */
    [p_coll_decl]    coll_decl,    /* default collation declaration */
    [p_ns_decl]      ns_decl,      /* namespace declaration */
    [p_fun_decls]    fun_decls,    /* list of function declarations */
    [p_fun]          fun,          /* function declaration */
    [p_ens_decl]     ens_decl,     /* default element namespace declaration */
    [p_fns_decl]     fns_decl,     /* default function namespace decl */
    [p_schm_imp]     schm_imp,     /* schema import */
    [p_params]       params,       /* list of (formal) function parameters */
    [p_param]        param         /* (formal) function parameter */
};

/** twig: setup twig */
#include "twig.h"

/* undefine the twig node ids because we introduce
 * core tree constructor functions of the same name below
 */
#undef plus         
#undef minus        
#undef mult         
#undef div_         
#undef idiv         
#undef mod          
#undef and          
#undef or           
#undef lt           
#undef le           
#undef gt           
#undef ge           
#undef eq           
#undef ne           
#undef val_lt       
#undef val_le       
#undef val_gt       
#undef val_ge       
#undef val_eq       
#undef val_ne       
#undef uplus        
#undef uminus       
#undef lit_int      
#undef lit_dec      
#undef lit_dbl      
#undef lit_str      
#undef is           
#undef nis          
#undef step         
#undef var          
#undef namet        
#undef kindt        
#undef locpath      
#undef root_        
#undef dot          
#undef ltlt         
#undef gtgt         
#undef flwr         
#undef binds        
#undef nil          
#undef empty_seq    
#undef bind         
#undef let          
#undef exprseq      
#undef range        
#undef union_       
#undef intersect    
#undef except       
#undef pred         
#undef if_          
#undef some         
#undef every        
#undef orderby      
#undef orderspecs   
#undef instof       
#undef seq_ty       
#undef empty_ty     
#undef node_ty      
#undef item_ty      
#undef atom_ty      
#undef atomval_ty   
#undef named_ty     
#undef req_ty       
#undef req_name     
#undef typeswitch   
#undef cases        
#undef case_        
#undef schm_path    
#undef schm_step    
#undef glob_schm    
#undef glob_schm_ty 
#undef castable     
#undef cast         
#undef treat        
#undef validate     
#undef apply      
#undef args
#undef char_        
#undef doc          
#undef elem         
#undef attr         
#undef text         
#undef tag          
#undef pi           
#undef comment      
#undef contseq
#undef xquery       
#undef prolog       
#undef decl_imps    
#undef xmls_decl    
#undef coll_decl    
#undef ns_decl      
#undef fun_decls    
#undef fun
#undef ens_decl     
#undef fns_decl     
#undef schm_imp     
#undef params       
#undef param         

/** mnemonic XQuery Core constructors */
#include "core_mnemonic.h"

/**
 * The current context items $fs:dot, $fs:position, and $fs:last
 * (see W3C XQuery Formal Semantics 3.1.2)
 */
static PFvar_t *fs_dot;
static PFvar_t *fs_position;
static PFvar_t *fs_last;

/**
 * The top of this stack holds a reference to the current function
 * (whose argument list is currently compiled).
 */
static PFarray_t *funs;
/**
 * The top of this stack holds a representation of the 
 * argument list to be passed to the currently compiled function application:
 *
 *               arg (e1, arg (e2, ..., arg (en, nil)...))
 */
static PFarray_t *args;


PFcnode_t *
PFfs (PFpnode_t *r)
{
    PFcnode_t *core;

    /* initially, there is no function on the current function stack */
    funs = PFarray (sizeof (PFfun_t *));

    /* initially, there is no argument list on the current arguments stack */
    args = PFarray (sizeof (PFcnode_t *));

    /* initially, the context item is undefined */
    fs_dot = 0;
    fs_position = 0;
    fs_last = 0;

    /* return core equivalent of the root node, i.e.,
     * the core-mapped query
     */
    core = [[ rewrite (r, 0) ]];

    /* sanity: current function/argument list stacks need to be empty */
    assert (PFarray_empty (funs));
    assert (PFarray_empty (args));

    return core;
}

/* vim:set filetype=c shiftwidth=4 expandtab: */

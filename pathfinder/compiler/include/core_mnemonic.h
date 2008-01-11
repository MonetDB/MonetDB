/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

/**
 * @file
 *
 * Mnemonic XQuery Core constructor names.
 *
 * This introduces mnemonic abbreviations for the PFcore_... constructors
 * in core/core.c.
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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */


#ifndef CORE_MNEMONIC_H
#define CORE_MNEMONIC_H

#include "core.h"

#undef nil        
#undef new_var    
#undef var        
#undef num        
#undef dec        
#undef dbl        
#undef str        
#undef seqtype    
#undef seqcast    
#undef proof      
#undef subty      
#undef typeswitch 
#undef case_      
#undef cases      
#undef ifthenelse 
#undef flwr       
#undef for_       
#undef let        
#undef letbind
#undef seq        
#undef twig_seq        
#undef empty      
#undef true_      
#undef false_     
#undef _root
#undef constr_elem
#undef constr_attr
#undef constr
#undef constr_tag
#undef locsteps   
#undef step       
#undef kindt      
#undef namet      
#undef function   
#undef apply      
#undef ebv        
#undef error      
#undef error_loc  
#undef fs_convert_op
#undef fn_data
#undef some
#undef cast    

#define nil()                 PFcore_nil ()
#define new_var(v)            PFcore_new_var (v)
#define var(v)                PFcore_var (v)
#define num(n)                PFcore_num (n)
#define dec(d)                PFcore_dec (d)
#define dbl(d)                PFcore_dbl (d)
#define str(s)                PFcore_str (s)
#define seqtype(t)            PFcore_seqtype (t)
#define seqcast(e1,e2)        PFcore_seqcast ((e1), (e2))
#define cast(e1,e2)           PFcore_cast ((e1), (e2))
#define proof(a,b)            PFcore_proof ((a), (b))
#define subty(a,b)            PFcore_subty ((a), (b))
#define stattype(e)           PFcore_stattype (e)
#define typeswitch(a,b)       PFcore_typeswitch ((a), (b))
#define cases(e1,e2)          PFcore_cases ((e1),(e2))
#define case_(e1,e2)          PFcore_case ((e1), (e2))
#define default_(e1)          PFcore_default (e1)
#define if_(cond,ret)         PFcore_if ((cond), (ret))
#define then_else(t,e)        PFcore_then_else ((t), (e))
#define flwr(bind,expr)       PFcore_flwr ((bind), (expr))
#define for_(bind,expr)       PFcore_for ((bind), (expr))
#define forbind(vars,expr)    PFcore_forbind ((vars), (expr))
#define forvars(var,pos)      PFcore_forvars ((var), (pos))
#define let(e1,e2)            PFcore_let ((e1), (e2))
#define letbind(e1,e2)        PFcore_letbind ((e1), (e2))
#define orderby(s,e1,e2)      PFcore_orderby ((s), (e1), (e2))
#define orderspecs(m,e1,e2)   PFcore_orderspecs ((m), (e1), (e2))
#define seq(e1,e2)            PFcore_seq ((e1), (e2))
#define twig_seq(e1,e2)       PFcore_twig_seq ((e1), (e2))
#define ordered(e)            PFcore_ordered (e)
#define unordered(e)          PFcore_unordered (e)
#define empty()               PFcore_empty ()
#define true_()               PFcore_true ()
#define false_()              PFcore_false ()
#define constr_elem(e1,e2)    PFcore_constr_elem((e1),(e2))
#define constr_attr(e1,e2)    PFcore_constr_attr((e1),(e2))
#define constr_pi(e1,e2)      PFcore_constr_pi((e1),(e2))
#define constr(k,e1)          PFcore_constr(k,e1)
#define constr_tag(qn)        PFcore_tag(qn)
#define locsteps(e1,e2)       PFcore_locsteps ((e1), (e2))
#define step(a,e)             PFcore_step ((a), (e))
#define recursion(a,b)        PFcore_recursion ((a), (b))
#define seed(a,b)             PFcore_seed ((a), (b))
#define xrpc(a,b)             PFcore_xrpc ((a), (b))
#define fun_decls(a,b)        PFcore_fun_decls ((a), (b))
#define fun_decl(a,b,c)       PFcore_fun_decl ((a), (b), (c))
#define params(a,b)           PFcore_params ((a), (b))
#define param(a,b)            PFcore_param ((a), (b))
#define function(qn)          PFcore_function (qn)
#define arg(e1,e2)            PFcore_arg((e1), (e2))
#define apply(fn,e)           PFcore_apply ((fn), (e))
#define ebv(e)                PFcore_ebv (e)
                              
#define fs_convert_op_by_type(e,t)    PFcore_fs_convert_op_by_type ((e), (t))
#define fs_convert_op_by_expr(e1,e2)  PFcore_fs_convert_op_by_expr ((e1), (e2))

#define fn_data(e1)           PFcore_fn_data (e1)
#define some(v,e1,e2)         PFcore_some(v,e1,e2)
       
#endif 

/* vim:set shiftwidth=4 expandtab: */

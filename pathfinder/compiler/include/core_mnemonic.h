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
#undef typeswitch 
#undef case_      
#undef cases      
#undef ifthenelse 
#undef for_       
#undef let        
#undef seq        
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

#define nil()                 PFcore_nil ()
#define new_var(v)            PFcore_new_var (v)
#define var(v)                PFcore_var (v)
#define num(n)                PFcore_num (n)
#define dec(d)                PFcore_dec (d)
#define dbl(d)                PFcore_dbl (d)
#define str(s)                PFcore_str (s)
#define seqtype(t)            PFcore_seqtype (t)
#define seqcast(e1,e2)        PFcore_seqcast ((e1), (e2))
#define proof(e1,t,e2)        PFcore_proof ((e1), (t), (e2))
#define stattype(e)           PFcore_stattype (e)
#define typeswitch(e1,e2,e3)  PFcore_typeswitch ((e1), (e2), (e3))
#define case_(e1,e2)          PFcore_case ((e1), (e2))
#define cases(e1,e2)          PFcore_cases ((e1),(e2))
#define ifthenelse(e1,e2,e3)  PFcore_ifthenelse ((e1), (e2), (e3))
#define for_(e1,e2,e3,e4)     PFcore_for ((e1), (e2), (e3), (e4))
#define let(e1,e2,e3)         PFcore_let ((e1), (e2), (e3))
#define seq(e1,e2)            PFcore_seq ((e1), (e2))
#define empty()               PFcore_empty ()
#define true_()               PFcore_true ()
#define false_()              PFcore_false ()
#define constr_elem(e1,e2)    PFcore_constr_elem(e1,e2)
#define constr_attr(e1,e2)    PFcore_constr_attr(e1,e2)
#define constr(k,e1)          PFcore_constr(k,e1)
#define constr_tag(qn)        PFcore_tag(qn)
#define locsteps(e1,e2)       PFcore_locsteps ((e1), (e2))
#define step(a,e)             PFcore_step ((a), (e))
#define kindt(k,e)            PFcore_kindt ((k), (e))
#define namet(qn)             PFcore_namet (qn)
#define function(qn)          PFcore_function (qn)
#define arg(e1,e2)            PFcore_arg((e1), (e2))
#define apply(fn,e)           PFcore_apply ((fn), (e))
#define ebv(e)                PFcore_ebv (e)
#define error(s,...)          PFcore_error ((s), __VA_ARGS__)
#define error_loc(l,s,...)    PFcore_error_loc ((l), (s), __VA_ARGS__)
                              
#define fs_convert_op_by_type(e,t)    PFcore_fs_convert_op_by_type ((e), (t))
#define fs_convert_op_by_expr(e1,e2)  PFcore_fs_convert_op_by_expr ((e1), (e2))

#define fn_data(e1)           PFcore_fn_data (e1)
#define some(v,e1,e2)         PFcore_some(v,e1,e2)
       
#endif 

/* vim:set shiftwidth=4 expandtab: */

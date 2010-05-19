/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Mnemonic abbreviations for subtree accesses.
 *
 * @if COPYRIGHT
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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * @endif COPYRIGHT
 *
 * $Id$
 */

#ifndef MSA_MNEMONIC_H
#define MSA_MNEMONIC_H

/* Mnemonic for msa annotation */
#define OP(n) ((n)->msa_ann->op)
#define PRJ_LIST(n) ((n)->msa_ann->prj_list)
#define SEL_LIST(n) ((n)->msa_ann->sel_list)

/* A list of expressions (actually: column names) */
#define PFmsa_exprlist_t               PFarray_t
/* Constructor for a expression list */
#define PFmsa_exprlist(size)           PFarray (sizeof (PFmsa_expr_t *), (size))
#define PFmsa_exprlist_copy(el)        PFarray_copy ((el))
/* Positional access to a expression list */
#define PFmsa_exprlist_at(el,i)        *(PFmsa_expr_t **) PFarray_at ((el), (i))
#define PFmsa_exprlist_top(el)         *(PFmsa_expr_t **) PFarray_top ((el))
/* Append to a expression list */
#define PFmsa_exprlist_add(el)         *(PFmsa_expr_t **) PFarray_add ((el))
#define PFmsa_exprlist_concat(el1,el2) PFarray_concat ((el1), (el2))
/* Size of a expression list */
#define PFmsa_exprlist_size(el)        PFarray_last ((el))

/** abbreviation for expression list constructor */
#define el(s)       PFmsa_exprlist((s))
/** abbreviation for expression list accessors */
#define elat(el,i)  PFmsa_exprlist_at((el),(i))
#define eltop(el)   PFmsa_exprlist_top((el))
#define eladd(el)   PFmsa_exprlist_add((el))
#define elsize(el)  PFmsa_exprlist_size((el))
#define elcopy(el)  PFmsa_exprlist_copy((el))
#define elconcat(el1, el2)  PFmsa_exprlist_concat((el1),(el2))

#endif

/* vim:set shiftwidth=4 expandtab: */

/**
 * @file
 *
 * Optimize relational algebra expression tree.
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

#ifndef ALGOPT_H
#define ALGOPT_H

#include "logical.h"
#include "load_stats.h"
PFla_op_t * PFalgopt (PFla_op_t *root, bool timing, PFguide_tree_t *guide_tree);

/**
 * Infer property for a DAG rooted in root
 * (The implementation is located in the
 *  corresponding opt/opt_*.[brg|c] file)
 */
PFla_op_t * PFalgopt_complex (PFla_op_t *root);
PFla_op_t * PFalgopt_const (PFla_op_t *root, bool no_attach);
PFla_op_t * PFalgopt_dom (PFla_op_t *root);
PFla_op_t * PFalgopt_general (PFla_op_t *root);
PFla_op_t * PFalgopt_icol (PFla_op_t *root);
PFla_op_t * PFalgopt_join_pd (PFla_op_t *root);
PFla_op_t * PFalgopt_key (PFla_op_t *root);
PFla_op_t * PFalgopt_mvd (PFla_op_t *root, unsigned int noneffective_tries);
PFla_op_t * PFalgopt_reqval (PFla_op_t *root);
PFla_op_t * PFalgopt_set (PFla_op_t *root);
PFla_op_t * PFalgopt_thetajoin (PFla_op_t *root);
PFla_op_t * PFalgopt_guide(PFla_op_t *root, PFguide_tree_t *guide);

#endif  /* ALGOPT_H */

/* vim:set shiftwidth=4 expandtab: */

/**
 * @file
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
 *          Sabine Mayer <mayers@inf.uni-konstanz.de>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include "builtins.h"

/**
 * Build up operator tree for an arithmetic addition.
 *
 * env,loop,delta: e1 => q1,delta1   env,loop,delta1: e2 => q2,delta2
 * ------------------------------------------------------------------
 *                 env,loop,delta: (e1 + e2) =>
 * (proj_iter,pos,item:res(PLUS_res<item,item1>
 *             (q1 |X| (iter,iter1) (proj_iter1:iter,item1:item q2))),
 *              delta2)
 */
PFalg_op_t *
PFbui_op_add (PFalg_op_t *loop __attribute__((unused)),
	      PFalg_op_t **delta __attribute__((unused)),
	      PFalg_op_t **args)
{
    /* TODO: check if this node was already built */

    return project (add (eqjoin (args[0],
				 project (args[1],
					  proj ("iter1", "iter"),
					  proj ("item1", "item")),
				 "iter",
				 "iter1"),
			 "item", "item1", "res"),
		    proj ("iter", "iter"),
		    proj ("pos", "pos"),
		    proj ("item", "res"));
}


/**
 * Build up operator tree for an arithmetic subtraction.
 *
 * env,loop,delta: e1 => q1,delta1   env,loop,delta1: e2 => q2,delta2
 * ------------------------------------------------------------------
 *                 env,loop,delta: (e1 - e2) =>
 * (proj_iter,pos,item:res(MINUS_res<item,item1>
 *             (q1 |X| (iter,iter1) (proj_iter1:iter,item1:item q2))),
 *              delta2)
 */
PFalg_op_t *
PFbui_op_subtract (PFalg_op_t *loop __attribute__((unused)),
		   PFalg_op_t **delta __attribute__((unused)),
		   PFalg_op_t **args)
{
    /* TODO: check if this node was already built */

    return project (subtract (eqjoin (args[0],
				      project (args[1],
					       proj ("iter1", "iter"),
					       proj ("item1", "item")),
				      "iter",
				      "iter1"),
			      "item", "item1", "res"),
		    proj ("iter", "iter"),
		    proj ("pos", "pos"),
		    proj ("item", "res"));
}


PFalg_op_t *
PFbui_op_tyval (PFalg_op_t *loop __attribute__((unused)),
		PFalg_op_t **delta __attribute__((unused)),
		PFalg_op_t **args)
{
    return args[0];
}

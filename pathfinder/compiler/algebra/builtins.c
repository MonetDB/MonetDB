#include "builtins.h"

PFalg_op_t * PFbui_op_add (PFalg_op_t *loop __attribute__((unused)),
			   PFalg_op_t **delta __attribute__((unused)),
			   PFalg_op_t **args)
{
    /* TODO: check if this node was already built */

    return project (add (eqjoin (args[1],
				 project (args[2],
					  proj ("iter1", "iter"),
					  proj ("item1", "item")),
				 "iter",
				 "iter1"),
			 "item", "item1", "res"),
		    proj ("iter", "iter"),
		    proj ("pos", "pos"),
		    proj ("item", "res"));
}

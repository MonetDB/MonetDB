/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

#ifndef BUITLINS_H
#define BUITLINS_H


#include "algebra.h"
#include "algebra_mnemonic.h"

/**
 * Create algebra expressions to represent build-in XQuery
 * functions.
 * 'args' is an array of algebra operators which represents
 * the argument list of the function. It may be of variable
 * length.
 */
PFalg_op_t * PFbui_op_add (PFalg_op_t *loop, PFalg_op_t **delta,
			   PFalg_op_t **args);


#endif   /* BUITLINS_H */

/* vim:set shiftwidth=4 expandtab: */

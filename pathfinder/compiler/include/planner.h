/**
 * @file
 *
 * Interface for planner that compiles logical algebra tree into
 * a physical query plan.
 *
 * $Id$
 */

#ifndef PLANNER_H
#define PLANNER_H

#include "array.h"

#include "logical.h"
#include "physical.h"

PFpa_op_t *PFplan (PFla_op_t *);

#endif  /* PLANNER_H */

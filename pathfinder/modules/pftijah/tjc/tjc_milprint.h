/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

#ifndef MILPRINT_H
#define MILPRINT_H

#include "tjc_abssyn.h"
#include "tjc_normalize.h"

char* 
milprint(tjc_config *tjc_c, TJpnode_t *node);

short
assign_numbers (TJpnode_t *node, short cnt);

#endif  /* MILPRINT_H */

/* vim:set shiftwidth=4 expandtab: */

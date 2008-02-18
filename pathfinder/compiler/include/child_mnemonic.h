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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * @endif COPYRIGHT
 *
 * $Id$
 */

#ifndef CHILD_MNEMONIC_H
#define CHILD_MNEMONIC_H

/* Easily access subtree-parts. */

/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
/** starting from p, make two steps left */
#define LL(p) L(L(p))
/** starting from p, make a step left, then a step right */
#define LR(p) R(L(p))
/** starting from p, make a step right, then a step left */
#define RL(p) L(R(p))
/** starting from p, make two steps right */
#define RR(p) R(R(p))

/* ... and so on ... */
#define LLL(p) L(L(L(p)))
#define LLR(p) R(L(L(p)))
#define LRL(p) L(R(L(p)))
#define LRR(p) R(R(L(p)))
#define RLL(p) L(L(R(p)))
#define RLR(p) R(L(R(p)))
#define RRL(p) L(R(R(p)))
#define RRR(p) R(R(R(p)))

#define LLLL(p) L(L(L(L(p))))
#define LLLR(p) R(L(L(L(p))))
#define LLRL(p) L(R(L(L(p))))
#define LRLL(p) L(L(R(L(p))))
#define RLLL(p) L(L(L(R(p))))
#define LLRR(p) R(R(L(L(p))))
#define LRLR(p) R(L(R(L(p))))
#define RLLR(p) R(L(L(R(p))))
#define RRLR(p) R(L(R(R(p))))
#define RLRL(p) L(R(L(R(p))))
#define RRLL(p) L(L(R(R(p))))
#define RRRL(p) L(R(R(R(p))))

#define LLLLL(p) L(L(L(L(L(p)))))
#define LLLLR(p) R(L(L(L(L(p)))))
#define RRRRL(p) L(R(R(R(R(p)))))

#define RRRRRL(p) L(R(R(R(R(R(p))))))
#define RRRRRR(p) R(R(R(R(R(R(p))))))

#endif

/* vim:set shiftwidth=4 expandtab: */

/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Enum declaration for Monet implementation types
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
 *  created by U Konstanz are Copyright (C) 2000-2003 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#ifndef MILTY_H
#define MILTY_H

#include "pathfinder.h"

/** Simple Monet types (oid, void, bit, int, dbl,...) */
typedef enum PFmty_simpl_t PFmty_simpl_t;

/**
 * Simple Monet types (oid, void, bit, int, dbl,...).
 *
 * The types 'node' and 'item' are provided by the Pathfinder
 * extension module. They are actually implemented as oids.
 * All the others are Monet's predefined data types.
 *
 * Our mapping will try to use the actual type whereever
 * possible, that is, the types bit, int, dbl, or node.
 * If something cannot typed precisely enough during static
 * typing, we will 'box' the value and use the type item.
 * The value is then actually only a reference to a value
 * in one of four global BATs, one for each simple type. A
 * lookup has then to be made in these BATs to receive the
 * actual value.
 *
 * The types 'oid' and 'void' are only used for internal
 * stuff. (Sequence) BATs will have a 'void' head; their
 * sequence base, for instance, must have type 'oid'.
 *
 * @note If you touch this enum definition, make sure you
 *       adapt the @c cast variable in mil/milprint.c
 *       accordingly!
 */
enum PFmty_simpl_t
{
      mty_oid   = 0  /**< Monet's 'oid' type for object identifiers */
    , mty_void  = 1  /**< Monet's 'void' type for virtual oids */
    , mty_bit   = 2  /**< Monet's 'bit' type for booleans */
    , mty_int   = 3  /**< Monet's 'int' type for integers */
    , mty_str   = 4  /**< Monet's 'str' type for strings */
    , mty_dbl   = 5  /**< Monet's 'dbl' type for doubles */
    , mty_node  = 6  /**< 'node' type, provided by the Pathfinder ext. module */
    , mty_item  = 7  /**< 'item' type, provided by the Pathfinder ext. module */
};

/** Quantifier for a Monet item (simple type or BAT) */
typedef enum PFmty_quant_t PFmty_quant_t;

/**
 * Quantifiers that a Monet item may have.
 *
 * In our mapping scheme we use either simple values, the different
 * variants are described by the enum #PFmty_simpl_t. Or we use
 * sequences of them. That is, a BAT that has a void head with
 * sequence base 1@0 and a tail of the sequence's type. This enum
 * represents these two variants.
 */
enum PFmty_quant_t
{
      mty_simple    = false  /**< MIL item is a simple value */
    , mty_sequence  = true   /**< MIL item is a sequence */
};

/**
 * Fully describes the MIL implementation type of any Monet data
 * item we use in our mapping scheme.
 */
typedef struct PFmty_t PFmty_t;

/**
 * Fully describes the MIL implementation type of any Monet data
 * item we use in our mapping scheme.
 *
 * This implementation type consists of two components:
 *  - The simple type of this item
 *  - A quantifier that describes if this is a sequence
 *    (implemented as a BAT) or a simple value.
 */
struct PFmty_t
{
    PFmty_simpl_t  ty;     /**< The item's simple type */
    PFmty_quant_t  quant;  /**< Is this a sequence or a simple value? */
};

#endif    /* MILTY_H */

/* vim:set shiftwidth=4 expandtab: */

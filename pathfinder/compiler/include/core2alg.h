/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
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
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#ifndef CORE2ALG_H
#define CORE2ALG_H

#include "core.h"
#include "algebra.h"


/** Compile XQuery Core into Relational Algebra */
PFalg_op_t *PFcore2alg (PFcnode_t *);


/**
 * Collect info on kind tests within an XPath expression.
 */
PFalg_op_t *PFnameTest (PFqname_t  qname);
PFalg_op_t *PFnodeTest (void);
PFalg_op_t *PFcommTest (void);
PFalg_op_t *PFtextTest (void);
PFalg_op_t *PFpiTest (void);
PFalg_op_t *PFpitarTest (char *target);
PFalg_op_t *PFdocTest (void);
PFalg_op_t *PFelemTest (void);
PFalg_op_t *PFattrTest (void);

/**
 * Collect info on location steps within an XPath expression.
 */
PFalg_op_t *PFanc (PFalg_op_t *n);
PFalg_op_t *PFanc_self (PFalg_op_t *n);
PFalg_op_t *PFattr (PFalg_op_t *n);
PFalg_op_t *PFchild (PFalg_op_t *n);
PFalg_op_t *PFdesc (PFalg_op_t *n);
PFalg_op_t *PFdesc_self (PFalg_op_t *n);
PFalg_op_t *PFfol (PFalg_op_t *n);
PFalg_op_t *PFfol_sibl (PFalg_op_t *n);
PFalg_op_t *PFpar (PFalg_op_t *n);
PFalg_op_t *PFprec (PFalg_op_t *n);
PFalg_op_t *PFprec_sibl (PFalg_op_t *n);
PFalg_op_t *PFself (PFalg_op_t *n);



#endif   /* CORE2ALG_H */

/* vim:set shiftwidth=4 expandtab: */

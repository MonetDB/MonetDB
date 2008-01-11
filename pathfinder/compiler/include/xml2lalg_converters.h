/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Converting data (strings) of XML-serialized logical Algebra 
 * Plans to the corresponding PF data types
 *
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
 * $Id: xml2lalg_converters.h,v 1.0 2007/10/31 22:00:00 ts-tum 
 * Exp $ 
 */


#ifndef XML2LALG_CONVERTERS_H
#define XML2LALG_CONVERTERS_H



#include "pathfinder.h"

#include <stdio.h>
#ifdef HAVE_STDBOOL_H
    #include <stdbool.h>
#endif

#include <string.h>
#include <assert.h>
#include <stdlib.h>



#include "algebra.h"
#include "logical.h"
#include "logical_mnemonic.h"




PFla_op_kind_t 
PFxml2la_conv_2PFLA_OpKind(const char* s);

PFalg_att_t 
PFxml2la_conv_2PFLA_attributeName(const char* s);
PFalg_att_t 
PFxml2la_conv_2PFLA_attributeName_unq(const char* s);


PFalg_simple_type_t 
PFxml2la_conv_2PFLA_atomType(char* typeString);
 

PFalg_atom_t 
PFxml2la_conv_2PFLA_atom(char* typeString, char* valueString);


PFalg_comp_t 
PFxml2la_conv_2PFLA_comparisonType(char* s);

PFalg_fun_t 
PFxml2la_conv_2PFLA_functionType(char* s);

bool 
PFxml2la_conv_2PFLA_orderingDirection(char* s); 

PFalg_axis_t 
PFxml2la_conv_2PFLA_xpathaxis(char* s);

PFty_t 
PFxml2la_conv_2PFLA_sequenceType(char* s);

PFalg_doc_t 
PFxml2la_conv_2PFLA_docType(char* s);

#endif


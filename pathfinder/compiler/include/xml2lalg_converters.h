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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id: xml2lalg_converters.h,v 1.0 2007/10/31 22:00:00 ts-tum 
 * Exp $ 
 */

#ifndef XML2LALG_CONVERTERS_H
#define XML2LALG_CONVERTERS_H

#include "algebra.h"
#include "logical.h"

PFla_op_kind_t PFxml2la_conv_2PFLA_OpKind (const char* s);

PFalg_col_t PFxml2la_conv_2PFLA_attributeName (const char* s);
PFalg_col_t PFxml2la_conv_2PFLA_attributeName_unq (const char* s);

PFalg_simple_type_t PFxml2la_conv_2PFLA_atomType (char* typeString);
 
PFalg_atom_t PFxml2la_conv_2PFLA_atom (PFalg_simple_type_t,
                                       char *prefix, char *uri,
                                       char* valueString);

PFalg_comp_t PFxml2la_conv_2PFLA_comparisonType (char* s);

PFalg_aggr_kind_t PFxml2la_conv_2PFLA_aggregateType (char* s);

PFalg_fun_t PFxml2la_conv_2PFLA_functionType (char* s);

bool PFxml2la_conv_2PFLA_orderingDirection (char* s); 

PFalg_axis_t PFxml2la_conv_2PFLA_xpathaxis (char* s);

PFalg_node_kind_t PFxml2la_conv_2PFLA_nodekind (char* s);

PFalg_fun_call_t PFxml2la_conv_2PFLA_fun_callkind(char* s);

PFalg_doc_t PFxml2la_conv_2PFLA_docType (char* s);

PFalg_doc_tbl_kind_t PFxml2la_conv_2PFLA_doctblType (char* s);

#endif


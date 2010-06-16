/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Compiler Driver definitions for the Ferry/Pathfinder compiler.
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
 */

#ifndef FERRY_LIB_H
#define FERRY_LIB_H

/* the maximum size of the initialized error buffer */
#define ERR_SIZE 4096

/** The output format */
enum PFoutput_format_t {
      PFoutput_format_sql /*< SQL code */
    , PFoutput_format_xml /*< the logical algebra in XML format */
    , PFoutput_format_dot /*< the AT&T dot representation */
};
/** location steps */
typedef enum PFoutput_format_t PFoutput_format_t;

/* the default optimization options if @a format is NULL */
#define PFopt_args "OIKCG_VG_JIS_I_GECSVR_OK_N" \
                     "QU_}MT{JISAI_GYECSVR_"    \
                     "QU_}MT{JISAI_OK_GYECSVR_" \
                     "QU_CGP"

/**
 * Accept a logical query plan bundle in XML format
 * and transform it into one of the output formats.
 *
 * @param res    contains a pointer to the result string after completion
 * @param err    an empty string (with memory initialized) storing the error
 *               messages after completion
 * @param xml    the input XML plan
 * @param format the output format (see definition of PFoutput_format_t)
 * @return       the return code
 */
int PFcompile_ferry (char **res,
                     char *err,
                     char *xml,
                     PFoutput_format_t format);

/**
 * Accept a logical query plan bundle in XML format, optimize it based
 * on the argument @a opt_args or (if missing) the default optimization
 * arguments in PFopt_args, and transform it into one of the output formats.
 *
 * @param res      contains a pointer to the result string after completion
 * @param err      an empty string (with memory initialized) storing the error
 *                 messages after completion
 * @param xml      the input XML plan
 * @param format   the output format (see definition of PFoutput_format_t)
 * @param opt_args the optimization arguments (see pf option -o)
 * @return         the return code
 */
int PFcompile_ferry_opt (char **res, 
                         char *err,
                         char *xml,
                         PFoutput_format_t format,
                         char *opt_args);

#endif

/* vim:set shiftwidth=4 expandtab: */

/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Helper functions for the Pathfinder compiler to deal with strings.
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
 * $Id$
 */

#include "pathfinder.h"

#include <assert.h>
#include <string.h>

#include "pfstrings.h"

#include "mem.h"

/**
 * Escape all backslahes, newlines and quotes in string @a in
 * by a backslash. Use this to print strings for AT&T dot or MIL
 * output.
 *
 * @param in The string to convert.
 * @return The input string with all newlines escaped to '\n' and
 *         all double quotes escaped to '\"'. (Output string will
 *         be at most twice as long as input string.)
 */
char *
PFesc_string (char *in)
{
    char        *out;
    unsigned int len = 0;      /* length of input string */
    unsigned int pos_in = 0;   /* current position in input string */
    unsigned int pos_out = 0;  /* current position in output string */

    len = strlen (in);

    /* Two times the input size + trailing '\0' should be enough */
    out = (char *) PFmalloc (2 * len + 1);

    for (pos_in = 0; pos_in < len; pos_in++)
    {
        switch (in[pos_in])
        {
            case '\\':  out[pos_out]   = '\\';
                        out[pos_out+1] = '\\';
                        pos_out += 2;
                        break;
            case '\n':  out[pos_out]   = '\\';
                        out[pos_out+1] = 'n';
                        pos_out += 2;
                        break;
            case '"':   out[pos_out]   = '\\';
                        out[pos_out+1] = '"';
                        pos_out += 2;
                        break;
            default:    out[pos_out] = in[pos_in];
                        pos_out++;
        }
    }

    out[pos_out] = '\0';

    return out;
}

/* vim:set shiftwidth=4 expandtab: */

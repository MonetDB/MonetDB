/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Helper functions for the Pathfinder compiler to deal with strings.
 *
 * $Id$
 */

#include <assert.h>
#include <string.h>

#include "pathfinder.h"

/**
 * Escape all newlines and quotes in string @a in by a backslash.
 * Use this to print strings for AT&T dot or MIL output.
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

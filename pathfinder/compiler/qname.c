/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Helper functions that should be available globally, without
 * a specific module or processing phase assignment. Declarations
 * for this file go into include/pathfinder.h
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
 *  created by U Konstanz are Copyright (C) 2000-2005 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include "pathfinder.h"

#include <string.h>

#include "qname.h"

/* PFns_t */
#include "ns.h"
#include "mem.h"

/** 
 * Compare two QNames and return integer less than, greater than
 * or equal to zero, similar to @c strcmp semantics.
 *
 * @param qn1 first QName
 * @param qn2 second QName
 * @return result of equality test
 */
int
PFqname_eq (PFqname_t qn1, PFqname_t qn2)
{
    int i = PFns_eq (qn1.ns, qn2.ns);

    /* ns prefixes already decide comparison */
    if (i)
        return i;

    /* ns prefixes equal and both QNames are wildcards? 
     * => consider QNames equal
     */
    if (PFQNAME_WILDCARD (qn1) && PFQNAME_WILDCARD (qn2))
        return 0;

    /* ns prefixes equal and qn2 is wildcard => qn1 is greater 
     */
    if (PFQNAME_WILDCARD (qn2))
        return 1;

    /* ns prefixes equal and qn1 is wildcard => qn2 is greater 
     */
    if (PFQNAME_WILDCARD (qn1))
        return -1;

    /* ns prefixes equal, no wildcards => compare local parts
     */
    return strcmp (qn1.loc, qn2.loc);
}

/**
 * Convert a QName into an equivalent string `ns:loc'
 * (if DEBUG_NSURI is def'd, convert to `uri:loc' instead).
 *
 * @param qn QName to convert
 * @return pointer to string representation of QName
 */
char *
PFqname_str (PFqname_t qn)
{
    char *s;
#ifdef DEBUG_NSURI
    return PFqname_uri_str (qn);
#endif

    s = (char *) PFmalloc ((qn.ns.ns ? strlen (qn.ns.ns) + 1 : 0) 
                           + strlen (qn.loc) + 1);

    return strcat (qn.ns.ns ? strcat (strcpy (s, qn.ns.ns), ":") : s,
                   qn.loc);
}

/**
 * Convert a QName ns:loc into an equivalent string `uri:loc' where
 * `uri' is the URI bound to the namespace ns.
 *
 * @param qn QName to convert
 * @return pointer to string representation of QName (with URI)
 */
char *
PFqname_uri_str (PFqname_t qn)
{
    char *s;

    s = (char *) PFmalloc ((qn.ns.uri ? strlen (qn.ns.uri) + 1 : 0) 
                           + strlen (qn.loc) + 1);

    return strcat (qn.ns.uri ? strcat (strcpy (s, qn.ns.uri), ":") : s,
                   qn.loc);
}


/**
 * Return the URI part of a QName
 *
 * @param qn QName whose URI part is requested
 */
char *
PFqname_uri (PFqname_t qn)
{
    /* is there an URI attached to this QName? */
    if (qn.ns.uri)                
        return PFstrdup (qn.ns.uri);
    
    /* otherwise return the empty string */
    return PFstrdup ("");
}


/**
 * Return the local name of a QName
 *
 * @param qn QName whose local part is requested
 */
char *
PFqname_loc (PFqname_t qn)
{
    return PFstrdup (qn.loc);
}


/**
 * Construct a QName from a string of the form `ns:loc' or `loc'.
 * In the latter case, the @a ns field of the return QName is 0
 * (this is different from a `*' ns).
 *
 * @warning Do not use this function after namespace resolution has
 *   been done! In the namespace resolution phase (see semantics/ns.c),
 *   all namespace prefixes are filled up with their corresponding
 *   URI. If you create any new #PFqname_t variables after this with
 *   this function, they will not carry the URI. Use PFqname() instead
 *   and pass one of the predefined namespaces as a #PFns_t struct.
 *
 * @param n string of the form `ns:loc' or `loc'
 * @return pointer to newly allocated QName
 */
PFqname_t
PFstr_qname (char *n)
{
    char *colon;
    char *nsloc;
    PFqname_t qn;

    /* skip leading `:' */
    n += (*n == ':');

    /* copy `ns:loc' string */
    nsloc = PFstrdup (n);

    if ((colon = strchr (nsloc, ':'))) {
        /* QName = ns:loc */
        *colon = '\0';
        qn.ns.ns   = nsloc;
        qn.ns.uri  = 0;
        qn.loc     = colon + 1;
    }
    else {
        /* QName = loc */
        qn.ns.ns   = 0;
        qn.ns.uri  = 0;
        qn.loc     = nsloc; 
    }

    return qn;
}

/**
 * Construct a QName with a given namespace. The namespace must already
 * be available as a #PFns_t struct.
 *
 * <b>Use this function <em>after</em> namespace resolution has been
 * done. PFstr_qname() cannot be used any more at that time, as
 * namespace prefixes will not be expanded any more.</b>
 *
 * @param ns The namespace part of the resulting QName (corresponds to
 *   the @c ns member of the #PFqname_t struct).
 * @param loc The local part of the resulting QName.
 */
PFqname_t
PFqname (PFns_t ns, char * loc)
{
    return (PFqname_t) { .ns = ns, .loc = loc };
}

/* vim:set shiftwidth=4 expandtab: */

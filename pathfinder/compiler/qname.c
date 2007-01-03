/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Helper functions that should be available globally, without
 * a specific module or processing phase assignment. Declarations
 * for this file go into include/pathfinder.h
 *
 * @section Pathfinder's Namespace Handling/Representation
 *
 * We represent QNames using the struct #PFqname_t, where
 *
 *  - the field @c loc holds the local part of the QName (as a regular
 *    C string) and
 *  - the field @c ns is a #PFns_t that combines prefix and URI of
 *    the QName's namespace.
 *
 * The @c loc field of this struct is the local part of this QName and
 * will usually be filled with a sensible value (never the empty string).
 * We indicate a @b wildcard local name (for name tests and in the type
 * system) with a @c NULL pointer.
 *
 * The struct #PFns_t contains a prefix/URI pair.  Not always do we have
 * all the information at hand right away that we need to fill both fields:
 * During query parsing, we only have the prefix available, the URI part
 * will not be filled before we do namespace resolution (ns.c).  On the
 * other hand, there may be situations, where we only have a URI, but
 * don't have a prefix for it (e.g., if namespace resolution has assigned
 * the default element namespace to an unqualified QName).
 *
 * We use the following conventions:
 *
 *  - If the QName does not have a prefix, we set the empty string as
 *    the prefix.  This will be the case for all unqualified QNames
 *    before and after namespace resolution.
 *  - If the QName does <b>not have</b> any namespace URI (i.e., it is
 *    in no namespace), we use the empty string as its URI.  This will
 *    not be the case before namespace resolution has been completed.
 *  - If we do <b>not yet know</b> the URI of a namespace, we set the
 *    @c uri field to @c NULL.  Such a namespace field will be filled
 *    up during namespace resolution.
 *  - For the prefix part, such a "don't know" situation cannot arise,
 *    but we use a @c NULL prefix to indicate a @b wildcard namespace.
 *    Wildcard namespaces only apply to name tests with wildcards, and
 *    to element/attribute types with undefined name in the type
 *    system.
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"

#include <string.h>
#include <stdio.h>

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
    if (PFQNAME_LOC_WILDCARD (qn1) && PFQNAME_LOC_WILDCARD (qn2))
        return 0;

    /* ns prefixes equal and qn2 is wildcard => qn1 is greater 
     */
    if (PFQNAME_LOC_WILDCARD (qn2))
        return 1;

    /* ns prefixes equal and qn1 is wildcard => qn2 is greater 
     */
    if (PFQNAME_LOC_WILDCARD (qn1))
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
    char *ns;
    char *s;
#ifdef DEBUG_NSURI
    return PFqname_uri_str (qn);
#endif

    /* NULL prefix indicates wildcard ns -> print a star */
    if (! qn.ns.prefix) {
        ns = PFstrdup ("*:");
    }

    else
        /* if we have a prefix, print it */
        if (*(qn.ns.prefix))
            sprintf (ns = (char *) PFmalloc (strlen (qn.ns.prefix) + 2),
                     "%s:", qn.ns.prefix);

        else if (qn.ns.uri && *(qn.ns.uri))
            /* if there's no prefix, but an URI (e.g. default ens),
             * print the URI */
            sprintf (ns = (char *) PFmalloc (strlen (qn.ns.uri) + 4),
                     "\"%s\":", qn.ns.uri);
        else
            /* otherwise no prefix to print */
            ns = PFstrdup ("");
    
    s = (char *) PFmalloc (strlen (ns) + (qn.loc ? strlen (qn.loc) : 1) + 1);

    sprintf (s, "%s%s", ns, qn.loc ? qn.loc : "*");

    return s;
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
 * Return the prefix part of a QName
 *
 * @param qn QName whose prefix part is requested
 */
char *
PFqname_ns (PFqname_t qn)
{
    return qn.ns.prefix ? PFstrdup (qn.ns.prefix) : NULL;
}


/**
 * Return the URI part of a QName
 *
 * @param qn QName whose URI part is requested
 */
char *
PFqname_uri (PFqname_t qn)
{
    return qn.ns.uri ? PFstrdup (qn.ns.uri) : NULL;
}


/**
 * Return the local name of a QName
 *
 * @param qn QName whose local part is requested
 */
char *
PFqname_loc (PFqname_t qn)
{
    return qn.loc ? PFstrdup (qn.loc) : NULL;
}


/**
 * Construct a QName from a string of the form `ns:loc' or `loc'.
 * In the latter case, the @a prefix field of the return QName is "".
 * A wildcard prefix `*:loc' will lead to prefix = NULL.
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
        qn.ns.prefix = nsloc;
        qn.ns.uri    = NULL;
        qn.loc       = colon + 1;
    }
    else {
        /* QName = loc */
        qn.ns.prefix = PFstrdup ("");
        qn.ns.uri    = NULL;
        qn.loc       = nsloc; 
    }

    /*
     * We represents wildcards (prefix as well as local name)
     * by a NULL pointer.  The above procedure will make them
     * the string "*".
     */
    if (!strcmp (qn.loc, "*"))
        qn.loc = NULL;

    if (!strcmp (qn.ns.prefix, "*"))
        qn.ns.prefix = NULL;

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

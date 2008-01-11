/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * @section qname_representation Pathfinder's Namespace Handling/Representation
 *
 * The way we represent QNames in Pathfinder depends on the processing
 * step of the compiler:
 *
 *  # During parsing, we create representations as #PFqname_raw_t,
 *    which is a struct consisting of the components @c prefix and
 *    @c loc (prefix and local name parts, respectively).
 *    .
 *  # During the namespace resolution stage, we convert all the
 *    #PFqname_raw_t entries into #PFqname_t entries.  #PFqname_t
 *    is actually just an integer.  The full QName information (prefix,
 *    URI, and local name) are held in the array #qnames located in
 *    this file (and only visible here).  The #PFqname_t integer is 
 *    an index into this array.
 *
 * To operate on #PFqname_raw_t types, use the PFqname_raw... functions
 *
 *  - PFqname_raw(): Create a #PFqname_raw_t from a `prefix:loc' string.
 *    This is invoked, e.g., during parsing.
 *  - PFqname_raw_str(): Return a printable representation of a raw
 *    QName (for debuggin/error messages).
 *  - PFqname_raw_eq(): Compare two raw QNames.
 *
 * Once namespaces have been resolved, only use the PFqname_... functions
 *
 *  - PFqname(): Given a namespace (as #PFns_t) and a local name, return
 *    the corresponding integer id.  If we haven't seen this QName before,
 *    this function creates a new entry in the #qnames array.
 *  - PFqname_eq(): Compare two QNames.  Note that this is particularly
 *    simple now, since we only need to compare integers.
 *  - PFqname_str(): Return a printable representation of this QName.
 *  - PFqname_ns(), PFqname_prefix(), PFqname_uri(), PFqname_loc():
 *    Return the namespace part (as #PFns_t), the prefix (as a string),
 *    the URI (as a string), or the local name (as a string) of a given
 *    QName.
 *  - PFqname_ns_wildcard(), PFqname_loc_wildcard(): Test whether a
 *    given QName contains wildcard parts.
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
 * $Id$
 */

#include "pathfinder.h"

#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "qname.h"

/* PFns_t */
#include "ns.h"

#include "mem.h"

/** the number of prefix characters we look at when hashing */
#define HASH_CHARS 7

/** the number of hash buckets */
#define HASH_BUCKETS 16

typedef struct qname_internal_t qname_internal_t;

/**
 * Full information about a qualified name.
 *
 * The @c loc field of this struct is the local part of this QName and
 * will usually be filled with a sensible value (never the empty string).
 * We indicate a @b wildcard local name (for name tests and in the type
 * system) with a @c NULL pointer.
 *
 * The struct #PFns_t contains a prefix/URI pair.  There may be situations,
 * where we only have a URI, but don't have a prefix for it (e.g., if
 * namespace resolution has assigned the default element namespace to an
 * unqualified QName).
 *
 * We use the following conventions:
 *
 *  - If the QName does not have a prefix, we set the empty string as
 *    the prefix.  This will be the case for all unqualified QNames
 *    before and after namespace resolution (i.e., in PFqname_raw_t
 *    structs also).
 *  - If the QName does <b>not have</b> any namespace URI (i.e., it is
 *    in no namespace), we use the empty string as its URI.
 *  - We use a @c NULL prefix to indicate a @b wildcard namespace.
 *    Wildcard namespaces only apply to name tests with wildcards, and
 *    to element/attribute types with undefined name in the type
 *    system.
 */
struct qname_internal_t {
    PFns_t      ns;    /**< namespace part */
    const char *loc;   /**< local name */
};

typedef struct qname_hash_entry_t qname_hash_entry_t;

/** Entry in the QName |--> id mapping hash table. */
struct qname_hash_entry_t {
    qname_internal_t  key;    /**< search key (a QName) */
    unsigned int      index;  /**< index of this QName */
};

/** array of qname_internal_t */
static PFarray_t *qnames     = NULL;

/** array of arrays of qname_hash_entry_t */
static PFarray_t *qname_hash = NULL;

/**
 * Hash function for lookups in the #qname_hash table (maps QNames
 * onto their ids).
 */
static unsigned int
hash (const char *s)
{
    unsigned int ret = 0;

    /* We denote a wildcard local name with NULL. Arbitrarily return 1. */
    if (!s)
        return 1;

    for (unsigned int i = 0; s[i] && i < HASH_CHARS; i++)
        ret += s[i];

    return ret % HASH_BUCKETS;
}

/**
 * Construct a QName with a given namespace. The namespace must
 * already be available as a #PFns_t struct.
 *
 * For the same parameter combination, this function will return
 * the same QName identifier for each call.  We use a hash table
 * that maps QNames to their identifier and use that to look up
 * the identifier.  If we haven't seen a QName before, we enter
 * it into both, the hash table as well as the list of known QNames
 * (id |--> QName mapping).
 *
 * @param ns The namespace part of the resulting QName (corresponds to
 *   the @c ns member of the #PFqname_t struct).
 * @param loc The local part of the resulting QName.
 */
PFqname_t
PFqname (PFns_t ns, const char *loc)
{
    unsigned int  h = hash (loc);
    PFarray_t    *bucket;

    /* initialize QName table and inverted (hash) list if neccessary */
    assert (qnames); assert (qname_hash);

    /* see if we can find the given ns/loc pair */
    bucket = *((PFarray_t **) PFarray_at (qname_hash, h));

    for (unsigned int i = 0; i < PFarray_last (bucket); i++) {

        qname_hash_entry_t entry
            = *((qname_hash_entry_t *) PFarray_at (bucket, i));

        if (! PFns_eq (ns, entry.key.ns))
            if ((loc && entry.key.loc && ! strcmp (loc, entry.key.loc))
                    || (! loc && ! entry.key.loc))
            return entry.index;
    }

    /* seems like we haven't found it; create a new entry */
    *((qname_hash_entry_t *) PFarray_add (bucket))
        = (qname_hash_entry_t) { .key = (qname_internal_t) { .ns = ns,
                                                             .loc = loc },
                                 .index = PFarray_last (qnames) };

    *((qname_internal_t *) PFarray_add (qnames))
        = (qname_internal_t) { .ns = ns, .loc = loc };

    /* and return the index of the new entry */
    return PFarray_last (qnames) - 1;
}

/**
 * Initialize the #qnames and #qname_hash structs.
 */
void
PFqname_init (void)
{
    qnames = PFarray (sizeof (qname_internal_t));
    qname_hash = PFarray (sizeof (PFarray_t *));

    for (unsigned int i = 0; i < HASH_BUCKETS; i++)
        *((PFarray_t **) PFarray_at (qname_hash, i))
            = PFarray (sizeof (qname_hash_entry_t));
}

/**
 * Compare two QNames; strcmp() semantics.
 *
 * @deprecated
 *   We are usually interested in equality only (not in the real strcmp()
 *   semantics).  Hence, better use a different comparison method (and
 *   a less confusing function name).
 */
int
PFqname_eq (PFqname_t qn1, PFqname_t qn2)
{
    return qn1 - qn2;
}


/**
 * Convert a QName into an equivalent string `ns:loc'
 * (if DEBUG_NSURI is def'd, convert to `uri:loc' instead).
 *
 * @param qn QName to convert
 * @return pointer to string representation of QName
 */
char *
PFqname_str (PFqname_t q)
{
    char *ns;
    char *s;

    qname_internal_t qn
        = *((qname_internal_t *) PFarray_at (qnames, q));

#ifdef DEBUG_NSURI
    return PFqname_uri_str (q);
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
PFqname_uri_str (PFqname_t q)
{
    char *s;

    qname_internal_t qn
        = *((qname_internal_t *) PFarray_at (qnames, q));

    s = (char *) PFmalloc ((qn.ns.uri ? strlen (qn.ns.uri) + 1 : 0) 
                           + strlen (qn.loc) + 1);

    return strcat (qn.ns.uri ? strcat (strcpy (s, qn.ns.uri), ":") : s,
                   qn.loc);
}


/**
 * Return the namespace part (prefix and URI as a #PFns_t) of a QName
 *
 * @param qn QName whose prefix part is requested
 */
PFns_t
PFqname_ns (PFqname_t q)
{
    return ((qname_internal_t *) PFarray_at (qnames, q))->ns;
}

/**
 * Return the prefix part of a QName
 *
 * @param qn QName whose prefix part is requested
 */
char *
PFqname_prefix (PFqname_t q)
{
    return ((qname_internal_t *) PFarray_at (qnames, q))->ns.prefix
        ? PFstrdup (((qname_internal_t *) PFarray_at (qnames, q))->ns.prefix)
        : NULL;
}


/**
 * Return the URI part of a QName
 *
 * @param qn QName whose URI part is requested
 */
char *
PFqname_uri (PFqname_t q)
{
    return ((qname_internal_t *) PFarray_at (qnames, q))->ns.uri
        ? PFstrdup (((qname_internal_t *) PFarray_at (qnames, q))->ns.uri)
        : NULL;
}


/**
 * Return the local name of a QName
 *
 * @param qn QName whose local part is requested
 */
char *
PFqname_loc (PFqname_t q)
{
    return ((qname_internal_t *) PFarray_at (qnames, q))->loc
        ? PFstrdup (((qname_internal_t *) PFarray_at (qnames, q))->loc)
        : NULL;
}

/** Test if @a q carries the wildcard namespace. */
bool
PFqname_ns_wildcard (PFqname_t q)
{
    return ((qname_internal_t *) PFarray_at (qnames, q))->ns.prefix == NULL;
}

/** Test if @a q carries the wildcard local name. */
bool
PFqname_loc_wildcard (PFqname_t q)
{
    return ((qname_internal_t *) PFarray_at (qnames, q))->loc == NULL;
}

/**
 * Construct a QName from a string of the form `ns:loc' or `loc'.
 * In the latter case, the @a prefix field of the return QName is "".
 * A wildcard prefix `*:loc' will lead to prefix = NULL.
 *
 * @param n string of the form `ns:loc' or `loc'
 * @return pointer to newly allocated raw QName
 */
PFqname_raw_t
PFqname_raw (const char *n)
{
    char          *colon;
    char          *nsloc;
    PFqname_raw_t  qn;

    /* skip leading `:' */
    n += (*n == ':');

    /* copy `ns:loc' string */
    nsloc = PFstrdup (n);

    if ((colon = strchr (nsloc, ':'))) {
        /* QName = ns:loc */
        *colon = '\0';
        qn.prefix = nsloc;
        qn.loc    = colon + 1;
    }
    else {
        /* QName = loc */
        qn.prefix = PFstrdup ("");
        qn.loc    = nsloc; 
    }

    /*
     * We represents wildcards (prefix as well as local name)
     * by a NULL pointer.  The above procedure will make them
     * the string "*".
     */
    if (!strcmp (qn.loc, "*"))
        qn.loc = NULL;

    if (!strcmp (qn.prefix, "*"))
        qn.prefix = NULL;

    return qn;
}

/** compare two raw QNames; strcmp() semantics */
int
PFqname_raw_eq (PFqname_raw_t qn1, PFqname_raw_t qn2)
{
    int i;

    if (qn1.prefix)
        if (qn2.prefix)
            i = strcmp (qn1.prefix, qn2.prefix);
        else
            i = 1;
    else
        if (qn2.prefix)
            i = -1;
        else
            i = 0;

    if (i)
        return i;

    if (qn1.loc)
        if (qn2.loc)
            return strcmp (qn1.loc, qn2.loc);
        else
            return 1;
    else
        if (qn2.loc)
            return -1;
        else
            return 0;
}

/** generate printable representation from a raw QName */
char *
PFqname_raw_str (PFqname_raw_t q)
{
    char *ns;
    char *s;

    /* NULL prefix indicates wildcard ns -> print a star */
    if (! q.prefix) {
        ns = PFstrdup ("*:");
    }

    else
        /* if we have a prefix, print it */
        if (*(q.prefix))
            sprintf (ns = (char *) PFmalloc (strlen (q.prefix) + 2),
                     "%s:", q.prefix);
        else
            /* otherwise no prefix to print */
            ns = PFstrdup ("");
    
    s = (char *) PFmalloc (strlen (ns) + (q.loc ? strlen (q.loc) : 1) + 1);

    sprintf (s, "%s%s", ns, q.loc ? q.loc : "*");

    return s;
}

/* vim:set shiftwidth=4 expandtab: */

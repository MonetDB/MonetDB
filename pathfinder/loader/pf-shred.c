/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file 
 *
 * pf-shred -- Pathfinder's XML document loader.  Maps the nodes of
 * given XML document into the pre/size using (a variant of the
 * pre/post plane).
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
 *
 * 
 * The mapping process writes the following collection of (ASCII
 * represented) relation files which may be subsequently imported into
 * Monet (via the ascii_io facility).  
 *
 * NB: The first column of all relations is dense, i.e., the pre, prop, @
 *     columns are _not_ actually written to the relation files 
 *     (during import, Monet creates the necessary void head columns)
 *
 *    relation  schema                    encodes
 *    --------------------------------------------------------------------
 *    .pre      pre|size|level|prop|kind  preorder rank, subtree size,
 *                                        node level, property ID, node kind
 *                                        for document/element/text/comment/p-i
 *                                        nodes
 *    .tag      prop|ns|loc               property ID, namespace prefix, local
 *                                        part for element nodes
 *    .text     prop|text                 property ID, text content for
 *                                        text nodes
 *    .com      prop|com                  property ID, comment content for
 *                                        comment nodes
 *    .pi       prop|tgt|ins              property ID, target, instruction for
 *                                        p-i nodes
 *    .@        @|own|ns|loc|val          attribute ID, owner, namespace
 *                                        prefix, local part, value for
 *                                        attribute nodes
 */


#if HAVE_CONFIG_H
#include <pf_config.h>
#endif

typedef unsigned int nat;

/**
 * next preorder rank to assign
 */
nat pre;

/**
 * next node property IDs to assign
 */
nat nsloc_id;
nat text_id;
nat com_id;
nat tgtins_id;

/**
 * Monet's representation of oid(nil) on a 32-bit Monet host
 *
 */
#define NIL 0x80000000

/**
 * XML node kinds
 */
#define ELEMENT   0
#define TEXT      1
#define COMMENT   2
#define PI        3
#define DOCUMENT  4


#include <stdlib.h>
#ifdef HAVE_LIBGEN_H
#include <libgen.h>
#endif
#include <stdarg.h>
#include <signal.h>
#include <assert.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

/* SAX parser interface (libxml2) */
#include "libxml/parser.h"
#include "libxml/parserInternals.h"

#if HAVE_LIBDB
/* Berkeley DB interface (libdb) */

#if !HAVE_U_LONG
/**
 * Berkeley DB blindly assumes the BSDism u_long to be defined
 */
typedef unsigned long u_long;
#endif /* !HAVE_U_LONG */

#include <db.h>

/**
 *   Property IDs are ``reused'' if possible:
 *   
 *   - element nodes with identical ns and loc share a property ID
 *   - text nodes with identical content share a property ID
 *   - comment nodes with identical content share a property ID
 *   - p-i nodes with identical tgt and ins share a property ID
 *
 * We maintain already seen node properties and their IDs in four
 * DB files.
 */

/**
 * Berkeley DB handling 
 */
typedef struct db_t db_t;
struct db_t {
    DB  *dbp;                    /**< DB handle */
    char file[FILENAME_MAX];     /**< DB file name */
};

typedef enum {
   nsloc_db
 , text_db
 , com_db
 , tgtins_db } db_id_t;

/** template for temporary directory */
#define TMPDIR_TEMPLATE "pf-shred_XXXXXX"

char tmpdir[] = TMPDIR_TEMPLATE;

db_t dbs[] = {
    /* db id  handle file name  */
    [nsloc_db]  { 0, TMPDIR_TEMPLATE "/nsloc.db"  }
  , [text_db]   { 0, TMPDIR_TEMPLATE "/text.db"   }
  , [com_db]    { 0, TMPDIR_TEMPLATE "/com.db"    } 
  , [tgtins_db] { 0, TMPDIR_TEMPLATE "/tgtins.db" }
};

#endif /* HAVE_LIBDB */

/**
 * do we allow duplicate node properties (default: yes)? 
 */
int prop_dup = 1;

#define MIN(x,y) (((x) < (y)) ? (x) : (y))

/**
 * maximum length of a Monet string
 *
 * @bug: try to identify the real limit Monet imposes
 */
#define MONET_STRLEN_MAX (1 << 12)

/**
 * current node level
 */
nat level;

/**
 * statistics:
 * - number of bytes used for encoded document
 * - depth of document processed
 *
 * - overall number of nodes
 * - number of element nodes
 * - number of text nodes
 * - number of attribute nodes
 * - number of comment nodes
 * - number of processing instruction nodes
 */
nat encoded;
nat depth;

nat nodes;
nat elem_nodes;
nat attr_nodes;
nat text_nodes;
nat com_nodes;
nat pi_nodes;

/**
 * number of text content bytes buffered,
 * the content buffer itself
 */
nat content;
char content_buf[MONET_STRLEN_MAX];

/**
 * XML node
 */
#define XML_TAG_MAX MONET_STRLEN_MAX

typedef struct node_t node_t;
struct node_t {
    nat  pre;                   /**< preorder rank */
    nat  size;                  /**< size of subtree below node */
    int  level;                 /**< tree level of parent 
                                     (0 if root, -1 if document node) */
    nat  prop;                  /**< property ID */
    nat  kind;                  /**< node kind */
};


/**
 * XML node stack maximum depth (use `-d' for deeper XML instances)
 */
#define XML_DEPTH_MAX 256
nat xml_depth_max;
node_t *lifo;

nat sp = 0;
#define PUSH(n) (assert (sp < xml_depth_max + 1),  \
                 lifo[sp++] = (n))
#define POP()   (assert (sp > 0), lifo[--sp])
#define TOP()   (assert (sp > 0), lifo[sp - 1])

/** 
 * relation handling
 */
typedef struct rel_t rel_t;
struct rel_t {
    int  fd;                    /**< file descriptor of relation file */
    char sufx[FILENAME_MAX];    /**< suffix of Unix relation file name */
    nat  bytes;                 /**< bytes/tuple in Monet BAT representation */
};

typedef enum {
   presizelevelpropkind
 , propnsloc
 , proptext
 , propcom
 , proptgtins
 , attownnslocval
} rel_id_t;

/**
 * size of Monet's atom representation
 * (_var: variable-sized atom on the Monet heap)
 */
#define _void 0
#define _oid  4
#define _int  4
#define _sht  2
#define _var  4

rel_t rels[] = {
    [presizelevelpropkind] 
    /*               pre|size      pre|level       pre|prop       pre|kind */
    { 0, "pre",   _void + _int + _void + _sht + _void + _oid + _void + _sht }
  , [propnsloc]
    /*              prop|ns        prop|loc                                */
    { 0, "tag",   _void + _var + _void + _var }
  , [proptext]
    /*              prop|text                                              */
    { 0, "text",  _void + _var }
  , [propcom]
    /*              prop|com                                               */
    { 0, "com",   _void + _var }
  , [proptgtins]
    /*              prop|tgt       prop|ins                                */
    { 0, "pi",    _void + _var + _void + _var }
  , [attownnslocval]
    /*                 @|own          @|ns           @|loc          @|val  */
    { 0, "@",     _void + _oid + _void + _var + _void + _var + _void + _var }
};

/**
 * format and size of a pre|size|level|prop|kind tuple 
 *
 *               size  level prop  kind  
 *                  |    |   |      |         */
#define PRETUPLE   "%10u,%5d,%10u@0,%5u\n"
 
nat pretuples;

/** 
 * seek into pre|size|level|prop|kind relation
 */
#define PRETUPLEOFFS(n) (pretuples * (n))

/**
 * convert ms timing value into string
 */
char *
timer_str (long elapsed)
{
    char *tm, *str;

    tm = str = strdup ("000h 00m 00s 000ms 000us");

    if (elapsed / 3600000000UL) {
        str += sprintf (str, "%03ldh ", elapsed / 3600000000UL);
        elapsed %= 3600000000UL;
    }
  
    if (elapsed / 60000000UL) {
        str += sprintf (str, "%02ldm ", elapsed / 60000000UL);
        elapsed %= 60000000UL;
    }
  
    if (elapsed / 1000000UL) {
        str += sprintf (str, "%02lds ", elapsed / 1000000UL);
        elapsed %= 1000000UL;
    }

    if (elapsed / 1000UL) {
        str += sprintf (str, "%03ldms ", elapsed / 1000UL);
        elapsed %= 1000UL;
    }

    str += sprintf (str, "%03ldus", elapsed);

    return tm;
}

/**
 * extract namespace prefix ns from QName ns:loc
 * (returns "" if QName is of the form loc)
 */
char 
*only_ns (char *qn)
{
    char *ns;
    char *colon;

    colon = strchr (qn, ':');

    if (colon) {
        ns = strdup (qn);
        *(ns + (colon - qn)) = '\0';
        return ns;
    }

    return strdup ("");
}

/**
 * extract local part loc from QName ns:loc
 */
char 
*only_loc (char *qn)
{
    char *colon;

    colon = strchr (qn, ':');

    if (colon) 
        return strdup (colon + 1);

    return strdup (qn);
}

/** 
 * closing a relation identified by relation id
 * (halt on error if err != 0)
 */
void 
close_rel (rel_id_t rel, int err)
{
    if (close (rels[rel].fd) < 0 && err) {
        fprintf (stderr, 
                 "!ERROR: could not close %s relation: %s\n",
                 rels[rel].sufx, strerror (errno));

        exit (EXIT_FAILURE);
    }
}

/**
 * opening a relation identified by relation id
 */
void 
open_rel (const char *out, rel_id_t rel)
{
    char fn[FILENAME_MAX];

    snprintf (fn, sizeof (fn), "%s.%s", out, rels[rel].sufx);
    
    if ((rels[rel].fd = open (fn, O_CREAT | O_TRUNC | O_WRONLY, 0644)) < 0) {
        fprintf (stderr, 
                 "!ERROR: could not open `%s': %s\n",
                 fn, strerror (errno));

        exit (EXIT_FAILURE);
    }
}

#if HAVE_LIBDB
/**
 * create and open a BTree-organized DB identified by DB id
 */
void 
open_db (db_id_t db)
{
    int err;

    if ((err = db_create (&(dbs[db].dbp), 0, 0))) {
        dbs[db].dbp->err (dbs[db].dbp, err,
                          "!ERROR: could not create DB `%s'", dbs[db].file);
        
        exit (EXIT_FAILURE);
    }

    /* replace directory part of database filename */
    memcpy (dbs[db].file, tmpdir, sizeof (TMPDIR_TEMPLATE) - 1);

    /*
     * BerkeleyDB has changed the signature of DB->open somewhere
     * between versions 4.0.14 and 4.1.25. The latter requires an
     * additional argument, `txnid' (transaction id). The configure
     * script checks this and sets BERKELEYDB_HAS_TRANSACTION to 1
     * if the parameter is needed, and to 0 if not.
     */
    if ((err = dbs[db].dbp->open (dbs[db].dbp,
#if BERKELEYDB_HAS_TXN
                                  0,
#endif
                                  dbs[db].file, 0, DB_BTREE, 
                                  DB_CREATE, 0600))) {
        dbs[db].dbp->err (dbs[db].dbp, err,
                          "!ERROR: could not open DB `%s'", dbs[db].file); 

        exit (EXIT_FAILURE);
    }
}

/**
 * close a DB identified by DB id
 * (and emit warning(s) if warn != 0)
 */
void 
close_db (db_id_t db, int warn)
{
    int err;
    
    assert (dbs[db].dbp);

    if ((err = dbs[db].dbp->close (dbs[db].dbp, DB_NOSYNC)) && warn)
        dbs[db].dbp->err (dbs[db].dbp, err,
                          "!WARNING: could not close DB `%s'", dbs[db].file); 

    if (unlink (dbs[db].file) && warn)
        fprintf (stderr, 
                 "!WARNING: could not unlink DB file `%s': %s\n",
                 dbs[db].file, strerror (errno));
}

/**
 * duplicate key check in DB identified by DB id.
 * no duplicate found: return 0, and enter key with prop_id into DB
 * duplicate found:    return 1, nothing entered in DB, prop_id
 *                     modified 
 */
int 
duplicate (db_id_t db, char *buf, nat len, nat *prop_id)
{
    DBT key, data;
    int err;

    /* if we allow for duplicate properties, simply return */
    if (prop_dup)
        return 0;

    assert (dbs[db].dbp);

    memset (&key, 0, sizeof (DBT));
    memset (&data, 0, sizeof (DBT));

    key.data = buf;
    key.size = len;

    data.data = prop_id;
    data.size = sizeof (nat);

    switch ((err = dbs[db].dbp->put (dbs[db].dbp, 0,
                                     &key, &data, DB_NOOVERWRITE))) {
    case DB_KEYEXIST:
        if ((err = dbs[db].dbp->get (dbs[db].dbp, 0,
                                     &key, &data, 0))) {
            dbs[db].dbp->err (dbs[db].dbp, err,
                              "!ERROR: failed to get prop ID from DB `%s'", 
                              dbs[db].file);
            
            exit (EXIT_FAILURE);
        }

        *prop_id = *(nat *)data.data;

        return 1;

    case 0:
        return 0;

    default:
        dbs[db].dbp->err (dbs[db].dbp, err,
                          "!ERROR: failed to store prop ID in DB `%s'", 
                          dbs[db].file);
        
        exit (EXIT_FAILURE);
    }        
}

#else
/**
 * No Berkeley DB support: assume no duplicates
 */
#define duplicate(db, buf, len, prop_id) 0
#endif /* HAVE_LIBDB */

/**
 * write character content buffer to relation, escape non-printable characters
 * via \xxx; writes a maximum of MONET_STRLEN_MAX characters; returns
 * actual number of characters written.
 */
nat 
content2rel (rel_id_t rel, char *buf, nat len)
{   
    nat p;
    char c;
    char oct[4] = "\\000";

    if (len > MONET_STRLEN_MAX) {
        len = MONET_STRLEN_MAX;
        fprintf (stderr,
                 "!WARNING: truncated document content > %u characters (starts with `%.16s...')\n",
                 MONET_STRLEN_MAX,
                 buf);
    }

    for (p = 0; p < len; p++)
        if ((c = buf[p]) < 127 && c)
            if (c < ' ') {
                /* escape C0 characters for Monet */
                oct[1] = ((c >> 6) & 7) | '0';
                oct[2] = ((c >> 3) & 7) | '0';
                oct[3] = (c        & 7) | '0';
                write (rels[rel].fd, oct, 4);
            }
            else
                write (rels[rel].fd, &c, sizeof (char));
        else
            fprintf (stderr, 
                     "!WARNING: skipping non-UTF-8 character with code %u\n",
                     (unsigned) c);

    return len;
}

/**
 * enter new XML node into pre|size|level|prop|kind relation
 */
void
node2rel (node_t node)
{
    char tuple[MONET_STRLEN_MAX];
    int  tuples;
    
    tuples = snprintf (tuple, sizeof (tuple), PRETUPLE,
                       node.size, 
                       node.level, 
                       node.prop, 
                       node.kind);
    
    /* write pre|size|level|prop|kind relation in document order: 
     * seek to offset determined by pre
     */
    assert ((nat) tuples == pretuples);
    
    lseek (rels[presizelevelpropkind].fd, 
           PRETUPLEOFFS (node.pre), SEEK_SET);
    write (rels[presizelevelpropkind].fd, tuple, tuples);
    encoded += rels[presizelevelpropkind].bytes;        
}

void 
shred_start_document (void *ctx)
{
    (void) ctx;

    pre          = 0;   /* next ``node ID'' */
    nsloc_id     = 0;   /*      element tag property ID */
    text_id      = 0;   /*      text content property ID */
    com_id       = 0;   /*      comment content property ID */
    tgtins_id    = 0;   /*      proc. target/instruction property ID */

    level = 0;
    depth = 0;

    encoded    = 0;
    elem_nodes = 0;
    text_nodes = 0;
    attr_nodes = 0;
    com_nodes  = 0;
    pi_nodes   = 0;

    content = 0;

    /* push document node */
    PUSH (((node_t) { .pre   = 0, 
                      .size  = 0, 
                      .level = -1,
                      .prop  = NIL,
                      .kind  = DOCUMENT
                    }));

    pre++;
}


void 
shred_end_document (void *ctx)
{
    (void) ctx;

    /* pop document node and enter into pre|size|level|prop|kind relation */
    node2rel (POP ());
}

/**
 * write buffered text content (if any) to prop|text relation
 */
void 
text2rel ()
{
    node_t node;

    int dup;

    /* is there any buffered text content? */
    if (content) {
        text_nodes++;

        node.pre   = pre;
        pre++;
        node.size  = 0;
        node.level = level;
        node.kind  = TEXT;
        node.prop  = text_id;

        /* this text node contributes to the size of its parent */
        TOP ().size++;

        /* is this duplicate text content? */
        dup = duplicate (text_db, content_buf, content, &(node.prop));

        /* if not, enter text node content into prop|text relation */
        if (! dup) {
            text_id++;
            
            write (rels[proptext].fd, "\"", sizeof ("\"") - 1);
            content = content2rel (proptext, content_buf, content);        
            write (rels[proptext].fd, "\"\n", sizeof ("\"\n") - 1);

            encoded += rels[proptext].bytes + content;
        }

        /* enter text node into pre|size|level|prop|kind relation */
        node2rel (node);
    }

    content = 0;
}

/**
 * SAX callback, invoked whenever `<t ...>' is seen
 */
void 
shred_start_element (void *ctx, 
                     const xmlChar *t, const xmlChar **atts)
{
    node_t node;

    char tuple[MONET_STRLEN_MAX + 16];
    int  tuples;
 
    char *ns;
    char *loc;

    nat len;

    int dup;

    (void) ctx;

    elem_nodes++;

    text2rel ();

    /* (1) assign preorder rank (document order), size, level, and kind */
    node.pre   = pre;
    pre++;
    node.size  = 0;
    node.level = level;
    node.kind  = ELEMENT;
    node.prop  = nsloc_id;

    /* descend one level */
    level++;

    /* keep track of document depth */
    if (level > depth)
        depth = level;

    /* does this element have a duplicate tag name? */
    dup = duplicate (nsloc_db, (char *) t, strlen (t), &(node.prop));

    /* if not, enter element tag name ns:loc 
     * into prop|ns|loc relation
     */
    if (! dup) {
        nsloc_id++;
        
        ns  = only_ns ((char *)t);
        loc = only_loc ((char *)t);
        
        write (rels[propnsloc].fd, "\"", sizeof ("\"") - 1);
        len =  content2rel (propnsloc, ns, strlen (ns));        
        write (rels[propnsloc].fd, "\",\"", sizeof ("\",\"") - 1);
        len += content2rel (propnsloc, loc, strlen (loc));        
        write (rels[propnsloc].fd, "\"\n", sizeof ("\"\n") - 1);

        encoded += rels[propnsloc].bytes + len;

        free (loc);
        free (ns);
    }

    /* push node onto node stack: size to be determined later */
    PUSH (node);

    /* (2) process the element's attribute(s), if any */
    if (atts)
        while (*atts) {            
            attr_nodes++;

            /* add attribute to @|own|ns|loc|val BAT */
            tuples = snprintf (tuple, sizeof (tuple), "%10u@0,", 
                               node.pre);
            write (rels[attownnslocval].fd, tuple, tuples);

            ns  = only_ns ((char *) *atts);
            loc = only_loc ((char *) *atts);

            write (rels[attownnslocval].fd, "\"", sizeof ("\"") - 1);
            len =  content2rel (attownnslocval, ns, strlen (ns));   
            write (rels[attownnslocval].fd, "\",\"", sizeof ("\",\"") - 1);
            len += content2rel (attownnslocval, loc, strlen (loc));
            write (rels[attownnslocval].fd, "\",\"", sizeof ("\",\"") - 1);
            len += content2rel (attownnslocval, (char *) *(atts + 1), 
                                strlen (*(atts + 1)));
            write (rels[attownnslocval].fd, "\"\n", sizeof ("\"\n") - 1);

            encoded += rels[attownnslocval].bytes + len;

            free (loc);
            free (ns);
        
            /* process next attribute */
            atts += 2;
        }
}

/** 
 * SAX callback invoked whenever `</t>' is seen
 */
void 
shred_end_element (void *ctx, const xmlChar *tag)
{
    node_t node;

    (void) ctx;
    (void) tag;

    text2rel ();

    node = POP ();

    /* ascend up one level */
    level--;

    /* the size of the subtree below this element and the element
     * itself contributes to the size of its parent
     */
    TOP ().size += node.size + 1;

    /* enter element node into pre|size|level|prop|kind relation */
    node2rel (node);
}

/**
 * SAX callback invoked whenever text node content is seen,
 * simply buffer the content here
 */
void 
shred_characters (void *ctx, const xmlChar *cs, int n)
{
    int l = MIN (MONET_STRLEN_MAX - (int) content, n);

    (void) ctx;

    memcpy (&(content_buf[content]), cs, l);
    content += l;

    if (l < n)
        fprintf (stderr,
                 "!WARNING: truncated text node > %u characters (starts with `%.16s...')\n",
                 MONET_STRLEN_MAX,
                 cs);
}

/**
 * SAX callback invoked whenever `<![CDATA[...]]>' is seenx
 */
void 
shred_cdata (void *ctx, const xmlChar *cdata, int n)
{
    shred_characters (ctx, cdata, n);
}

/** 
 * SAX callback invoked whenever `<?target ins?>' is seen
 */
void 
shred_pi (void *ctx, const xmlChar *tgt, const xmlChar *ins)
{
    node_t node;

    char pi[MONET_STRLEN_MAX * 2 + 1];
    int  pis;
    
    nat len;

    int dup;

    (void) ctx;

    pi_nodes++;

    text2rel ();

    node.pre   = pre;
    pre++;
    node.size  = 0;
    node.level = level;
    node.kind  = PI;
    node.prop  = tgtins_id;

    /* this comment p-i contributes to the size of its parent */
    TOP ().size++;
        
    /* build "tgt ins" as a key for the p-i DB */
    pis = snprintf (pi, MONET_STRLEN_MAX * 2 + 1, "%s %s", tgt, ins);

    /* does this p-i have a duplicate target/instruction pair? */
    dup = duplicate (tgtins_db, pi, pis, &(node.prop));

    /* if not, enter p-i target/instruction 
     * into prop|tgt|ins relation 
     */
    if (! dup) {
            tgtins_id++;
       
            write (rels[proptgtins].fd, "\"", sizeof ("\"") - 1);
            len =  content2rel (proptgtins, (char *) tgt, strlen (tgt));
            write (rels[proptgtins].fd, "\",\"", sizeof ("\",\"") - 1);
            len += content2rel (proptgtins, (char *) ins, strlen (ins));
            write (rels[proptgtins].fd, "\"\n", sizeof ("\"\n") - 1);

            encoded += rels[proptgtins].bytes + len;
    }
    
    /* enter p-i node into pre|size|level|prop|kind relation */
    node2rel (node);
}

void 
shred_comment (void *ctx, const xmlChar *c)
{
    node_t node;

    nat len;

    int dup;

    (void) ctx;

    com_nodes++;

    text2rel ();

    node.pre   = pre;
    pre++;
    node.size  = 0;
    node.level = level;
    node.kind  = COMMENT;
    node.prop  = com_id;

    /* this comment node contributes to the size of its parent */
    TOP ().size++;

    /* does this comment have duplicate comment content? */
    dup = duplicate (com_db, (char *) c, strlen (c), &(node.prop));
        
    /* if not, enter comment node content 
     * into prop|com relation 
     */
    if (! dup) {
        com_id++;

        write (rels[propcom].fd, "\"", sizeof ("\"") - 1);
        len = content2rel (propcom, (char *) c, strlen (c));        
        write (rels[propcom].fd, "\"\n", sizeof ("\"\n") - 1);

        encoded += rels[propcom].bytes + len;
    }

    /* enter comment node into pre|size|level|prop|kind relation */
    node2rel (node);
}


void 
error (void *ctx, const char *msg, ...)
{
    va_list msgs;
    char errmsg[MONET_STRLEN_MAX];

    fprintf (stderr, "!ERROR (XML parser): ");

    va_start (msgs, msg);
    vsnprintf (errmsg, MONET_STRLEN_MAX, msg, msgs);
    va_end (msgs);

    xmlParserError (ctx, errmsg);
}



/**
 * SAX callback table.
 */
xmlSAXHandler shredder = {
    .startDocument         = shred_start_document
  , .endDocument           = shred_end_document
  , .startElement          = shred_start_element
  , .endElement            = shred_end_element
  , .characters            = shred_characters
  , .processingInstruction = shred_pi
  , .comment               = shred_comment
  , .error                 = error
  , .cdataBlock            = shred_cdata
  , .internalSubset        = 0
  , .isStandalone          = 0
  , .hasInternalSubset     = 0
  , .hasExternalSubset     = 0
  , .resolveEntity         = 0
  , .getEntity             = 0
  , .entityDecl            = 0
  , .notationDecl          = 0
  , .attributeDecl         = 0
  , .elementDecl           = 0
  , .unparsedEntityDecl    = 0
  , .setDocumentLocator    = 0
  , .reference             = 0
  , .ignorableWhitespace   = 0
  , .warning               = 0
  , .fatalError            = 0
  , .getParameterEntity    = 0
  , .externalSubset        = 0
  , .initialized           = 0
};

/**
 * handle interruption (SIGINT) of this process:
 * close relation files and remove Berkeley DB garbage
 */
void 
interrupt (int sig)
{
    (void) sig;

    /* close relation files */
    close_rel (attownnslocval, 0);
    close_rel (proptgtins, 0);
    close_rel (propcom, 0);
    close_rel (proptext, 0);
    close_rel (propnsloc, 0);
    close_rel (presizelevelpropkind, 0);
    
#if HAVE_LIBDB
   /* close and remove DB files */
    close_db (tgtins_db, 0);
    close_db (com_db, 0);
    close_db (text_db, 0);
    close_db (nsloc_db, 0);
#endif

    fprintf (stderr, "interrupted\n");

    exit (EXIT_FAILURE);
}

void 
shred (const char *in, const char *out)
{
    xmlParserCtxtPtr ctx;

#if HAVE_LIBDB
    /* create and open DB files */
    if (mkdtemp (tmpdir) == 0)
        fprintf (stderr, "!ERROR: unable to create temporary directory :%s\n",
                         strerror (errno));
    open_db (nsloc_db);
    open_db (text_db);
    open_db (com_db);
    open_db (tgtins_db);
#endif

    /* open relation files */
    open_rel (out, presizelevelpropkind);
    open_rel (out, propnsloc);
    open_rel (out, proptext);
    open_rel (out, propcom);
    open_rel (out, proptgtins);
    open_rel (out, attownnslocval);

    signal (SIGINT, interrupt);

    /* parse XML input (receive SAX events) */
    ctx = xmlCreateFileParserCtxt (in);
    ctx->sax = &shredder;
    
    (void) xmlParseDocument (ctx);

    /* close relation files */
    close_rel (attownnslocval, 1);
    close_rel (proptgtins, 1);
    close_rel (propcom, 1);
    close_rel (proptext, 1);
    close_rel (propnsloc, 1);
    close_rel (presizelevelpropkind, 1);

#if HAVE_LIBDB    
    /* close and remove DB files */
    close_db (tgtins_db, 1);
    close_db (com_db, 1);
    close_db (text_db, 1);
    close_db (nsloc_db, 1);

    if (rmdir (tmpdir) != 0)
        fprintf (stderr, 
                 "!WARNING: could not delete temporary directory `%s': %s\n",
                 tmpdir, strerror (errno));
#endif

    if (! ctx->wellFormed) {
        fprintf (stderr, 
                 "!ERROR: XML input not well-formed "
                 "(relation files probably contain garbage)\n");

        exit (EXIT_FAILURE);
    }
}

int 
main (int argc, char *argv[])
{
    /* we read the XML input from standard input by default */
    char in[FILENAME_MAX] = "/dev/stdin";
    char out[FILENAME_MAX];

    /* timing */
    struct timeval now;
    long start, stop;

    /* buffer for file I/O status */
    struct stat statbuf;
    
    /* options -o/-v/seen? */
    int o = 0;
    int v = 0;

    int c;

    /* this code is not really Unicode or multi-byte aware (yet) */
    assert (sizeof (char) == sizeof (xmlChar));

    xml_depth_max = XML_DEPTH_MAX;

    /* option parsing */
    opterr = 0;
    
    while (1) {
#if HAVE_LIBDB
        c = getopt (argc, argv, "o:d:hvc");
#else
        c = getopt (argc, argv, "o:d:hv");
#endif
        if (c == -1)
            break;

        switch (c) {
        case 'o':
            strncpy (out, optarg, FILENAME_MAX);
            o = 1;
            break;

        case 'd':
            xml_depth_max = atoi (optarg);
            break;

#if HAVE_LIBDB
        case 'c':
            prop_dup = 0;
            break;
#endif

        case 'v':
            v = 1;
            break;

        case '?':
            fprintf (stderr, "!ERROR: invalid or incomplete option `-%c'\n\n",
                     (char) optopt);
            /* fall through */

        case 'h':
            printf ("Pathfinder XML Shredder ($Revision$, $Date$)\n"
                    "(c) University of Konstanz, DBIS group\n\n"
                    "Usage: %s [-h] [-v]"
#if HAVE_LIBDB
                    " [-c]" 
#endif
                    " [-d <n>] [-o OUTPUT] [FILE]\n\n"
                    "     Reads from standard input if FILE is omitted.\n\n"
                    "     -v: be verbose\n" 
#if HAVE_LIBDB
                    "     -c: compress node properties (40%% encoding speed)\n"
#endif
                    "     -d: set XML node stack depth to <n> (default %d)\n"
                    "     -o: write relations to OUTPUT.<rel> instead of FILE.<rel>\n"
                    "         (mandatory if we read from stdin)\n"
                    "         <rel> = { pre,tag,text,com,pi,@ }\n\n",
                    argv[0], XML_DEPTH_MAX);

            exit (EXIT_FAILURE);
        }
    }

    /* [FILE] parameter present? */
    if (argv[optind]) {
        strncpy (in, argv[optind], FILENAME_MAX);

        /* if -o not seen, use FILE also as OUTPUT */
        if (!o) {
            strncpy (out, in, FILENAME_MAX);
            o = 1;
        }
    }

    /* option sanity checks */

    /* -o seen if we read from stdin? */
    if (!o) {
        fprintf (stderr, 
                 "!ERROR: need `-o OUTPUT' if XML input comes from stdin\n");

        exit (EXIT_FAILURE);
    }

    /* is FILE readable? */
    if (stat (in, &statbuf) < 0) {
        fprintf (stderr,
                 "!ERROR: cannot stat `%s': %s\n",
                 in, strerror (errno));

        exit (EXIT_FAILURE);
    }

    /* allocate XML node stack */
    if (! (lifo = (node_t *) malloc (xml_depth_max * sizeof (node_t)))) {
        fprintf (stderr,
                 "!ERROR: cannot allocate XML node stack: %s\n",
                 strerror (errno));

        exit (EXIT_FAILURE);
    }

    /* compute tuple size in pre|size|level|prop|kind relation
     * (we seek in the relation file)
     */
    pretuples = snprintf (0, 0, PRETUPLE, 0, 0, 0, 0);

    /* start timer */
    (void) gettimeofday (&now, 0);
    start = now.tv_sec * 1000000 + now.tv_usec;

    /* shredding... */
    shred (in, out);

    /* read elapsed time */
    gettimeofday (&now, 0);
    stop = now.tv_sec * 1000000 + now.tv_usec;

    /* statistics if verbose (-v) */
    if (v) {
        /* + 1: document node */
        nodes = elem_nodes + attr_nodes + text_nodes + com_nodes + pi_nodes
            + 1;

        printf ("document size (serialized)     %lu byte(s)\n", 
                statbuf.st_size);
        printf ("document size (encoded)        %u byte(s)", encoded);

        if (statbuf.st_size) {
            double grow;

            grow = (double) encoded / statbuf.st_size;
            if (grow > 1.0)
                printf (", ~ %.2gx larger\n", grow);
            else
                printf (", ~ %.2gx smaller\n", 1 / grow);
        }
        else
            putchar ('\n');

        printf ("document depth                 %u\n" , depth);
        printf ("  document nodes               1\n");
        printf ("+ element nodes                %u" , elem_nodes);
        if (prop_dup)
            putchar ('\n');
        else
            printf (" [%u unique ns:loc pair(s)]\n", nsloc_id);
        printf ("+ attribute nodes              %u\n" , attr_nodes);
        printf ("+ text nodes                   %u" , text_nodes);
        if (prop_dup)
            putchar ('\n');
        else
            printf (" [%u unique text content(s)]\n", text_id);
        printf ("+ comment nodes                %u" , com_nodes);
        if (prop_dup)
            putchar ('\n');
        else
            printf (" [%u unique comment content(s)]\n", com_id);
        printf ("+ processing instruction nodes %u" , pi_nodes);
        if (prop_dup)
            putchar ('\n');
        else
            printf (" [%u unique target/instruction pair(s)]\n", tgtins_id);
        printf ("= nodes                        %u\n" , nodes);
        if (nodes)
            printf ("encoding time                  %s [%s/node]\n\n",
                    timer_str (abs (stop - start)),
                    timer_str (abs (stop - start) / nodes));;
    }
    
    return 0;
}

/* vim:set shiftwidth=4 expandtab: */

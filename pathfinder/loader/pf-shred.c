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
 * The mapping process writes the following collection of (ASCII represented)
 * 13 Monet BAT files which may be subsequently imported into Monet.  The
 * head column of all BATs is dense.
 *
 *    BAT (w/ suffix)    schema      encodes
 *    --------------------------------------------------------------------
 *    .pre               pre|size    preorder rank and subtree size for 
 *                                   element/text/comment/p-i nodes
 *    .level             pre|level   preorder rank and node level for 
 *                                   element/text/comment/p-i nodes
 *    .prop              pre|prop    preorder rank and node property ID for
 *                                   element/text/comment/p-i nodes
 *
 *    .ns                prop|ns     property ID and namespace prefix for
 *                                   element nodes
 *    .loc               prop|loc    property ID and local part of tag name for
 *                                   element nodes
 *    .text              prop|text   property ID and text content for
 *                                   text nodes
 *    .com               prop|com    property ID and comment content for
 *                                   comment nodes
 *    .tgt               prop|tgt    property ID and proc.ins. target for
 *                                   processing instruction nodes
 *    .ins               prop|ins    property ID and proc. instruction for
 *                                   processing instruction nodes
 *
 *    .@own              @|own       attribute ID and preorder rank of 
 *                                   owning element for attribute nodes
 *    .@ns               @|ns        attribute ID and namespace prefix for
 *                                   attriute nodes
 *    .@loc              @|loc       attribute ID and local part of attribute
 *                                   name for attriute nodes
 *    .@val              @|val       attribute ID and attribute value 
 *                                   for attribute nodes
 *
 * NB: the following assumes a 32-bit Monet host.
 */

/*
 * Include all the information we got from the configure script
 */
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

typedef unsigned int nat;

/*
 * - Encoding preorder ranks (pre columns)
 *
 *   Preorder ranks (node IDs) span a 30-bit wide domain:
 *
 *   used by Monet
 *   |
 *   X0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
 *    |
 *    this is an element/text/com/p-i node
 */
#define NODE 0x00000000

nat pre;

#define NODE_ID(id) (NODE | (id))

/*
 * - Encoding attribute IDs (@ columns)
 *
 *   Attribute IDs span a 30-bit wide domain:
 *
 *   used by Monet
 *   |
 *   X1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
 *    |
 *    this is an attribute
 */
#define ATTRIBUTE 0x4000000

nat attribute_id;

#define ATTRIBUTE_ID(id) (ATTRIBUTE | (id))

/* - Encoding property IDs (prop columns)
 *
 *   Property IDs span a 29-bit wide domain. We distinguish four
 *   property types:
 *
 *   used by Monet
 *   |
 *   X00bbbbbbbbbbbbbbbbbbbbbbbbbbbbb   ns/loc of element node tag name
 *   X01bbbbbbbbbbbbbbbbbbbbbbbbbbbbb   content of text node
 *   X10bbbbbbbbbbbbbbbbbbbbbbbbbbbbb   content of comment node
 *   X11bbbbbbbbbbbbbbbbbbbbbbbbbbbbb   tgt/ins of proc. ins. node
 *    \/
 *    property type
 */
#define NSLOC  0x00000000
#define TEXT   0x20000000
#define COM    0x40000000
#define TGTINS 0x60000000 

nat nsloc_id;
nat text_id;
nat com_id;
nat tgtins_id;

#define NSLOC_ID(id)  (NSLOC  | (id))
#define TEXT_ID(id)   (TEXT   | (id))
#define COM_ID(id)    (COM    | (id))
#define TGTINS_ID(id) (TGTINS | (id))


#include <stdlib.h>
#include <libgen.h>
#include <stdarg.h>
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
#include <db.h>
#endif


typedef enum {
   nsloc_db
 , text_db
 , com_db
 , tgtins_db } db_id_t;

#if HAVE_LIBDB
/*
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

/* DB handling 
 */
typedef struct db_t db_t;
struct db_t {
    DB  *dbp;                    /**< DB handle */
    char file[FILENAME_MAX];     /**< DB file name */
};

db_t dbs[] = {
    /* db id  handle file name  */
    [nsloc_db]  { 0, "nsloc_dbXXXXXX"  }
  , [text_db]   { 0, "text_dbXXXXXX"   }
  , [com_db]    { 0, "com_dbXXXXXX"    } 
  , [tgtins_db] { 0, "tgtins_dbXXXXXX" }
};
#endif

/* do we allow duplicate node properties? */
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
    nat  level;                 /**< tree level of parent (0 if root) */
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
 * BAT handling 
 */
typedef struct bat_t bat_t;
struct bat_t {
    int fd;                     /**< file descriptor of BAT file */
    char sufx[FILENAME_MAX];    /**< suffix of Unix BAT file name */
    char head[8];               /**< name of BAT head column */
    char tail[8];               /**< name of BAT tail column */
    nat  bytes;                 /**< bytes/BUN in this BAT */
};

typedef enum { 
   presize
 , prelevel
 , preprop
 , propns
 , proploc
 , proptext
 , propcom
 , proptgt
 , propins
 , attown
 , attns 
 , attloc 
 , attval } bat_id_t;

bat_t bats[] = {
  /* bat id    fd  sufx       head    tail      bytes (0: variable) */
    [presize] { 0, "pre",     "pre",  "size",   sizeof (int) }
  , [prelevel]{ 0, "level",   "pre",  "level",  sizeof (int) }  
  , [preprop] { 0, "prop",    "pre",  "prop",   sizeof (int) }  
  , [propns]  { 0, "ns",      "prop", "ns",     0            }  
  , [proploc] { 0, "loc",     "prop", "loc",    0            }  
  , [proptext]{ 0, "text",    "prop", "text",   0            }  
  , [propcom] { 0, "com",     "prop", "com",    0            }  
  , [proptgt] { 0, "tgt",     "prop", "tgt",    0            }  
  , [propins] { 0, "ins",     "prop", "ins",    0            }  
  , [attown]  { 0, "@own",    "@",    "own",    sizeof (int) }  
  , [attns]   { 0, "@ns",     "@",    "ns",     0            }  
  , [attloc]  { 0, "@loc",    "@",    "loc",    0            }  
  , [attval]  { 0, "@val",    "@",    "val",    0            }  
};

/* format and size of a BUN/BAT head */
#define BUN        "[ %10u@0, %10u@0 ]\n"
#define BATHEAD    "#----------------------------#\n# %-12s| %-12s #\n#----------------------------#\n"
#define PRESIZEBUN "[ %10u@0, %10u   ]\n"

nat presize_buns;
nat bat_heads;

/** 
 * Seek into pre|size table. 
 */
#define PRESIZEOFFS(n) (bat_heads + presize_buns * (n))

/**
 * Convert ms timing value into string.
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
 * Extract namespace prefix ns from QName ns:loc
 * (returns "" if QName is of the form loc).
 */
char *only_ns (char *qn)
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
 * Extract local part loc from QName ns:loc.
 */
char *only_loc (char *qn)
{
    char *colon;

    colon = strchr (qn, ':');

    if (colon) 
        return strdup (colon + 1);

    return strdup (qn);
}

/** 
 * Closing a BAT identified by BAT id.
 */
void close_bat (bat_id_t bat)
{
    if (close (bats[bat].fd) < 0) {
        fprintf (stderr, 
                 "!ERROR: could not close %s BAT: %s\n",
                 bats[bat].sufx, strerror (errno));

        exit (EXIT_FAILURE);
    }
}

/**
 * Opening a BAT identified by BAT id (also writes BAT header).
 */
void open_bat (const char *out, bat_id_t bat)
{
    char fn[FILENAME_MAX];
    char hd[100];

    nat hds;

    snprintf (fn, sizeof (fn), "%s.%s", out, bats[bat].sufx);
    
    if ((bats[bat].fd = open (fn, O_CREAT | O_TRUNC | O_WRONLY, 0644)) < 0) {
        fprintf (stderr, 
                 "!ERROR: could not open `%s': %s\n",
                 fn, strerror (errno));

        exit (EXIT_FAILURE);
    }

    hds = snprintf (hd, sizeof (hd), BATHEAD,
                    bats[bat].head, bats[bat].tail);

    assert (hds == bat_heads);

    write (bats[bat].fd, hd, hds);
}

#if HAVE_LIBDB
/**
 * Create and open a BTree-organized DB identified by DB id
 */
void open_db (db_id_t db)
{
    int err;

    if ((err = db_create (&(dbs[db].dbp), 0, 0))) {
        dbs[db].dbp->err (dbs[db].dbp, err,
                          "!ERROR: could not create DB `%s'", dbs[db].file);
        
        exit (EXIT_FAILURE);
    }

    if (mkstemp (dbs[db].file) < 0) {
        fprintf (stderr, 
                 "!ERROR: could not open temporary DB file: %s\n",
                 strerror (errno));

        exit (EXIT_FAILURE);        
    }

    if ((err = dbs[db].dbp->open (dbs[db].dbp, dbs[db].file, 0,
                                  DB_BTREE, 
                                  DB_TRUNCATE, 0600))) {
        dbs[db].dbp->err (dbs[db].dbp, err,
                          "!ERROR: could not open DB `%s'", dbs[db].file); 

        exit (EXIT_FAILURE);
    }
}

/**
 * Close a DB identified by DB id
 */
void close_db (db_id_t db)
{
    int err;
    
    assert (dbs[db].dbp);

    if ((err = dbs[db].dbp->close (dbs[db].dbp, DB_NOSYNC)))
        dbs[db].dbp->err (dbs[db].dbp, err,
                          "!WARNING: could not close DB `%s'", dbs[db].file); 

    if (unlink (dbs[db].file))
        fprintf (stderr, 
                 "!WARNING: could not unlink DB file `%s': %s\n",
                 dbs[db].file, strerror (errno));
}

/**
 * Duplicate key check in DB identified by DB id.
 * No duplicate found: return 0, and enter key with prop_id into DB
 * Duplicate found:    return 1, nothing entered in DB, prop_id
 *                     modified 
 */
int duplicate (db_id_t db, char *buf, nat len, nat *prop_id)
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
/* Dummy if we don't have BerkeleyDB support */
#define duplicate(a,b,c,d) 0
#endif


/**
 * Write character content buffer to BAT, escape non-printable characters
 * via \xxx.  Writes a maximum of MONET_STRLEN_MAX characters.  Returns
 * actual number of characters written.
 */
nat content2bat (bat_id_t bat, char *buf, nat len)
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
                write (bats[bat].fd, oct, 4);
            }
            else
                write (bats[bat].fd, &c, sizeof (char));
        else
            fprintf (stderr, 
                     "!WARNING: skipping non-UTF-8 character with code %u\n",
                     (unsigned) c);

    return len;
}

void shred_start_document (void *ctx)
{
    (void)ctx;
    pre          = 0;   /* next ``node ID'' */
    attribute_id = 0;   /*      attribute ID */
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

    /* establish virtual root above XML document (virtual parent for root) */
    PUSH (((node_t) { .pre   = -1, 
                      .size  = 0, 
                      .level = 0 
                    }));
}

void shred_end_document (void *ctx)
{
    /* dispose virtual root */
    (void)ctx;
    (void) POP ();
}

/**
 * Write buffered text content (if any) to prop|text BAT.
 */
void text2bat ()
{
    node_t node;

    char bun[MONET_STRLEN_MAX];
    int  buns;

    int dup;
    nat prop_id;

    /* is there any buffered text content? */
    if (content) {
        text_nodes++;

        node.pre   = NODE_ID (pre);
        pre++;
        node.size  = 0;
        node.level = level;
        
        /* this text node contributes to the size of its parent */
        TOP ().size++;
        
        /* enter text node into pre|size BAT */
        buns = snprintf (bun, sizeof (bun), PRESIZEBUN,
                         node.pre, node.size);
        
        /* write pre|size table in document order: seek to offset
         * determined by pre
         */
        assert ((nat) buns == presize_buns);
        
        lseek (bats[presize].fd, PRESIZEOFFS (node.pre), SEEK_SET);
        write (bats[presize].fd, bun, buns);
        encoded += bats[presize].bytes;
        
        /* enter text node into pre|level BAT */
        buns = snprintf (bun, sizeof (bun), "[ %10u@0, %10u   ]\n",
                         node.pre, node.level);
        write (bats[prelevel].fd, bun, buns);
        encoded += bats[prelevel].bytes;
        
        /* enter text node into pre|prop BAT */
        prop_id = TEXT_ID (text_id);
        dup = duplicate (text_db, content_buf, content, &prop_id);

        buns = snprintf (bun, sizeof (bun), BUN,
                         node.pre, prop_id);
        write (bats[preprop].fd, bun, buns);
        encoded += bats[preprop].bytes;

        if (! dup) {
            text_id++;
            
            /* enter text node content into prop|text BAT */
            buns = snprintf (bun, sizeof (bun), "[ %10u@0, \"",
                             prop_id);            
            
            write (bats[proptext].fd, bun, buns);
            content = content2bat (proptext, content_buf, content);        
            write (bats[proptext].fd, "\" ]\n", sizeof ("\" ]\n") - 1);
            encoded += bats[proptext].bytes + content;
        }
            
    }
    
    content = 0;
}

/**
 * SAX callback, invoked whenever `<t ...>' is seen.
 */
void shred_start_element (void *ctx, 
                          const xmlChar *t, const xmlChar **atts)
{
    node_t node;

    char bun[MONET_STRLEN_MAX + 16];
    int  buns;
 
    char *ns;
    char *loc;

    nat len;

    int dup;
    nat prop_id;

    elem_nodes++;

    text2bat ();

    (void)ctx;
    /* (1) assign preorder rank (document order), size and level */
    node.pre   = NODE_ID (pre);
    pre++;
    node.size  = 0;
    node.level = level;

    /* descend one level */
    level++;

    /* keep track of document depth */
    if (level > depth)
        depth = level;

    /* enter element node into pre|level BAT */
    buns = snprintf (bun, sizeof (bun), "[ %10u@0, %10u   ]\n",
                     node.pre, node.level);
    write (bats[prelevel].fd, bun, buns);
    encoded += bats[prelevel].bytes;

    /* enter element node into pre|prop BAT */
    prop_id = NSLOC_ID (nsloc_id);
    dup = duplicate (nsloc_db, (char *) t, strlen (t), &prop_id);

    buns = snprintf (bun, sizeof (bun), BUN,
                     node.pre, prop_id);
    write (bats[preprop].fd, bun, buns);
    encoded += bats[preprop].bytes;

    if (! dup) {
        nsloc_id++;
        
        /* enter element tag name ns:loc 
         * into prop|ns and prop|loc BATs
         */
        ns  = only_ns ((char *)t);
        loc = only_loc ((char *)t);
        
        buns = snprintf (bun, sizeof (bun), "[ %10u@0, \"",
                         prop_id);            
        write (bats[propns].fd, bun, buns);
        write (bats[proploc].fd, bun, buns);
        len =  content2bat (propns, ns, strlen (ns));        
        len += content2bat (proploc, loc, strlen (loc));        
        write (bats[propns].fd, "\" ]\n", sizeof ("\" ]\n") - 1);
        write (bats[proploc].fd, "\" ]\n", sizeof ("\" ]\n") - 1);
        encoded += bats[propns].bytes + bats[proploc].bytes + len;

        free (loc);
        free (ns);
    }

    /* push node onto node stack: size to be determined later */
    PUSH (node);

    /* (2) process the element's attribute(s), if any */
    if (atts)
        while (*atts) {            
            attr_nodes++;

            /* add attribute to @|own BAT */
            buns = snprintf (bun, sizeof (bun), BUN,
                             ATTRIBUTE_ID (attribute_id), node.pre);

            write (bats[attown].fd, bun, buns);
            encoded += bats[attown].bytes;

            /* enter attribute name ns:loc 
             * into @|ns and @|loc BATs
             */
            ns  = only_ns ((char *) *atts);
            loc = only_loc ((char *) *atts);
            
            buns = snprintf (bun, sizeof (bun), "[ %10u@0, \"",
                             ATTRIBUTE_ID (attribute_id));            
            write (bats[attns].fd, bun, buns);
            write (bats[attloc].fd, bun, buns);
            len =  content2bat (attns, ns, strlen (ns));        
            len += content2bat (attloc, loc, strlen (loc));        
            write (bats[attns].fd, "\" ]\n", sizeof ("\" ]\n") - 1);
            write (bats[attloc].fd, "\" ]\n", sizeof ("\" ]\n") - 1);
            encoded += bats[attns].bytes + bats[attloc].bytes + len;

            free (loc);
            free (ns);

            /* add attribute to @|val table */
            buns = snprintf (bun, sizeof (bun), "[ %10u@0, \"",
                             ATTRIBUTE_ID (attribute_id));            
            write (bats[attval].fd, bun, buns);
            len = content2bat (attval, (char *) *(atts + 1), 
                               strlen (*(atts + 1)));
            write (bats[attval].fd, "\" ]\n", sizeof ("\" ]\n") - 1);
            encoded += bats[attval].bytes + len;

            /* new attribute ID */
            attribute_id++;

            /* process next attribute */
            atts += 2;
        }
}

/** 
 * SAX callback invoked whenever `</t>' is seen.
 */
void shred_end_element (void *ctx, const xmlChar *tag)
{
    node_t node;

    char bun[XML_TAG_MAX + 16];
    int  buns;

    (void)ctx; (void)tag;
    text2bat ();

    node = POP ();

    /* ascend up one level */
    level--;

    /* the size of the subtree below this element and the element
     * itself contributes to the size of its parent
     */
    TOP ().size += node.size + 1;

    /* enter element node into pre|size table */
    buns = snprintf (bun, sizeof (bun), PRESIZEBUN,
                     node.pre, node.size);

    /* write pre|size BAT in document order: seek to offset
     * determined by pre
     */
    assert ((nat) buns == presize_buns);

    lseek (bats[presize].fd, PRESIZEOFFS (node.pre), SEEK_SET);
    write (bats[presize].fd, bun, buns);

    encoded += bats[presize].bytes;
}

/**
 * SAX callback invoked whenever text node content is seen.
 * Simply buffer the content here.
 */
void shred_characters (void *ctx, const xmlChar *cs, int n)
{
    int l = MIN (MONET_STRLEN_MAX - (int) content, n);
    
    (void)ctx;
    memcpy (&(content_buf[content]), cs, l);
    content += l;

    if (l < n)
        fprintf (stderr,
                 "!WARNING: truncated text node > %u characters (starts with `%.16s...')\n",
                 MONET_STRLEN_MAX,
                 cs);
}

/**
 * SAX callback invoked whenever `<![CDATA[...]]>' is seen.
 */
void shred_cdata (void *ctx, const xmlChar *cdata, int n)
{
    shred_characters (ctx, cdata, n);
}

/** 
 * SAX callback invoked whenever `<?target ins?>' is seen.
 */
void shred_pi (void *ctx, const xmlChar *tgt, const xmlChar *ins)
{
    node_t node;

    char bun[MONET_STRLEN_MAX * 2 + 1];
    int  buns;
    
    nat len;

    int dup;
    nat prop_id;

    (void)ctx;
    pi_nodes++;

    text2bat ();

    node.pre   = NODE_ID (pre);
    pre++;
    node.size  = 0;
    node.level = level;
    
    /* this comment p-i contributes to the size of its parent */
    TOP ().size++;
        
    /* enter p-i node into pre|size BAT */
    buns = snprintf (bun, sizeof (bun), PRESIZEBUN,
                     node.pre, node.size);
        
    /* write pre|size BAT in document order: seek to offset
     * determined by pre
     */
    assert ((nat) buns == presize_buns);
    
    lseek (bats[presize].fd, PRESIZEOFFS (node.pre), SEEK_SET);
    write (bats[presize].fd, bun, buns);
    encoded += bats[presize].bytes;
        
    /* enter p-i node into pre|level BAT */
    buns = snprintf (bun, sizeof (bun), "[ %10u@0, %10u   ]\n",
                     node.pre, node.level);
    write (bats[prelevel].fd, bun, buns);
    encoded += bats[prelevel].bytes;

    /* build "tgt ins" as a key for the p-i DB */
    buns = snprintf (bun, MONET_STRLEN_MAX * 2 + 1, "%s %s", tgt, ins);

    /* enter p-i node into pre|prop BAT */
    prop_id = TGTINS_ID (tgtins_id);
    dup = duplicate (tgtins_db, bun, buns, &prop_id);

    buns = snprintf (bun, sizeof (bun), BUN,
                     node.pre, prop_id);
    write (bats[preprop].fd, bun, buns);
    encoded += bats[preprop].bytes;

    if (! dup) {
            tgtins_id++;

            /* enter p-i target/instruction into prop|tgt and prop|ins BATs */
            buns = snprintf (bun, sizeof (bun), "[ %10u@0, \"",
                             prop_id);            
            write (bats[proptgt].fd, bun, buns);
            write (bats[propins].fd, bun, buns);
            len =  content2bat (proptgt, (char *) tgt, strlen (tgt));        
            len += content2bat (propins, (char *) ins, strlen (ins));        
            write (bats[proptgt].fd, "\" ]\n", sizeof ("\" ]\n") - 1);
            write (bats[propins].fd, "\" ]\n", sizeof ("\" ]\n") - 1);
            encoded += bats[proptgt].bytes + bats[propins].bytes + len;
    }
}

void shred_comment (void *ctx, const xmlChar *c)
{
    node_t node;

    char bun[MONET_STRLEN_MAX];
    int  buns;
    
    nat len;

    int dup;
    nat prop_id;

    (void)ctx;
    com_nodes++;

    text2bat ();

    node.pre   = NODE_ID (pre);
    pre++;
    node.size  = 0;
    node.level = level;
    
    /* this comment node contributes to the size of its parent */
    TOP ().size++;
        
    /* enter comment node into pre|size BAT */
    buns = snprintf (bun, sizeof (bun), PRESIZEBUN,
                     node.pre, node.size);
        
    /* write pre|size BAT in document order: seek to offset
     * determined by pre
     */
    assert ((nat) buns == presize_buns);
    
    lseek (bats[presize].fd, PRESIZEOFFS (node.pre), SEEK_SET);
    write (bats[presize].fd, bun, buns);
    encoded += bats[presize].bytes;
        
    /* enter comment node into pre|level BAT */
    buns = snprintf (bun, sizeof (bun), "[ %10u@0, %10u   ]\n",
                     node.pre, node.level);
    write (bats[prelevel].fd, bun, buns);
    encoded += bats[prelevel].bytes;
    
    /* enter comment node into pre|prop BAT */
    prop_id = COM_ID (com_id);
    dup = duplicate (com_db, (char *) c, strlen (c), &prop_id);

    buns = snprintf (bun, sizeof (bun), BUN,
                     node.pre, prop_id);
    write (bats[preprop].fd, bun, buns);
    encoded += bats[preprop].bytes;

    if (! dup) {
        com_id++;

        /* enter comment node content into prop|com BAT */
        buns = snprintf (bun, sizeof (bun), "[ %10u@0, \"",
                         prop_id);            
        write (bats[propcom].fd, bun, buns);
        len = content2bat (propcom, (char *) c, strlen (c));        
        write (bats[propcom].fd, "\" ]\n", sizeof ("\" ]\n") - 1);
        encoded += bats[propcom].bytes + len;
    }
}


void error (xmlParserCtxtPtr ctx, const char *msg, ...)
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
        0                                       /* internalSubset        */
  ,	0			                /* isStandalone          */
  ,	0			                /* hasInternalSubset     */
  ,	0			                /* hasExternalSubset     */
  ,	0			                /* resolveEntity         */
  ,	0			                /* getEntity             */
  ,	0			                /* entityDecl            */
  ,	0			                /* notationDecl          */
  ,	0			                /* attributeDecl         */
  ,	0			                /* elementtDecl          */
  ,	0			                /* unparsedEntityDecl    */  
  ,	0			                /* setDocumentLocator    */
  ,	shred_start_document                    /* startDocument         */
  ,	shred_end_document  	                /* endDocument           */
  ,	shred_start_element                     /* startElement          */  
  ,	shred_end_element               	/* endElement            */
  ,	0			                /* reference             */
  ,	shred_characters                        /* characters            */
  ,	0			                /* ignorableWhitespace   */
  ,	shred_pi	                        /* processingInstruction */  
  ,     shred_comment                           /* comment               */
  ,	0			                /* warning               */
  ,     (errorSAXFunc) error                    /* error                 */  
  ,     (errorSAXFunc) error                    /* fatalError            */
  ,	0			                /* getParameterEntity    */
  ,	0                                       /* cdataBlock            */
  ,	0			                /* externalSubset        */  
  ,     0                                       /* initialized           */
};


void shred (const char *in, const char *out)
{
    xmlParserCtxtPtr ctx;

#if HAVE_LIBDB
    /* create and open DB files */
    open_db (nsloc_db);
    open_db (text_db);
    open_db (com_db);
    open_db (tgtins_db);
#endif

    /* open BAT files */
    open_bat (out, presize);
    open_bat (out, prelevel);
    open_bat (out, preprop);
    open_bat (out, propns);
    open_bat (out, proploc);
    open_bat (out, proptext);
    open_bat (out, propcom);
    open_bat (out, proptgt);
    open_bat (out, propins);
    open_bat (out, attown);
    open_bat (out, attns);
    open_bat (out, attloc);
    open_bat (out, attval);

    /* parse XML input (receive SAX events) */
    ctx = xmlCreateFileParserCtxt (in);
    ctx->sax = &shredder;
    
    (void) xmlParseDocument (ctx);

    /* close BAT files */
    close_bat (attval);
    close_bat (attloc);
    close_bat (attns);
    close_bat (attown);
    close_bat (propins);
    close_bat (proptgt);
    close_bat (propcom);
    close_bat (proptext);
    close_bat (proploc);
    close_bat (propns);
    close_bat (preprop);
    close_bat (prelevel);
    close_bat (presize);

    
#if HAVE_LIBDB
    /* close and remove DB files */
    close_db (tgtins_db);
    close_db (com_db);
    close_db (text_db);
    close_db (nsloc_db);
#endif

    if (! ctx->wellFormed) {
        fprintf (stderr, 
                 "!ERROR: XML input not well-formed "
                 "(BAT files probably contain garbage)\n");

        exit (EXIT_FAILURE);
    }
}

int main (int argc, char *argv[])
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
        /* only allow -c if we have BerkeleyDB available */
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
            fprintf (stderr,
"Usage: %s [-h] [-v] [-c] [-d <n>] [-o <output>] [<XML input>]\n"
"     processes <XML input> (if present) or stdin\n"
"     -v: be verbose\n" 
#if HAVE_LIBDB
"     -c: compress node properties (40%% encoding speed)\n"
#endif
"     -d: set XML node stack depth to <n> (default %d)\n"
"     -o: write BATs to <output>.<bat> instead of <XML input>.<bat>\n"
"         (mandatory if we read from stdin)\n"
"         <bat> = { pre,level,prop,ns,loc,text,com,tgt,ins,@own,@ns,@loc,@val }\n"
"\n",
                     argv[0], XML_DEPTH_MAX);

            exit (EXIT_FAILURE);
        }
    }

    /* [<XML input>] parameter present? */
    if (argv[optind]) {
        strncpy (in, argv[optind], FILENAME_MAX);

        /* if -o not seen, use <XML input> also as <output> */
        if (!o) {
            strncpy (out, in, FILENAME_MAX);
            o = 1;
        }
    }

    /* option sanity checks */

    /* -o seen if we read from stdin? */
    if (!o) {
        fprintf (stderr, 
                 "!ERROR: need `-o <output>' if XML input comes from stdin\n");

        exit (EXIT_FAILURE);
    }

    /* is <XML input> readable? */
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

    /* compute BAT head size as well as pre|size BUN size
     * (we seek in the pre|size table)
     */
    bat_heads    = snprintf (0, 0, BATHEAD, "", "");
    presize_buns = snprintf (0, 0, PRESIZEBUN, 0, 0);

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
        nodes = elem_nodes + attr_nodes + text_nodes + com_nodes + pi_nodes;

        printf ("document size (serialized)     %lu byte(s)\n", 
                statbuf.st_size);
        printf ("document size (encoded)        %u byte(s)", encoded);
        if (statbuf.st_size) {
            double grow;

            grow = (double) encoded / statbuf.st_size;
            printf (", ~ %.2gx %s", grow, grow > 1.0 ? "larger" : "smaller");
        }
        putchar ('\n');
        printf ("document depth                 %u\n" , depth);
        printf ("  element nodes                %u" , elem_nodes);
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

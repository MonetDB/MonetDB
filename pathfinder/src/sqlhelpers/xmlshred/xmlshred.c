/**
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

#include <assert.h>
#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <getopt.h>

#include "shred_helper.h"

/* SAX parser interface (libxml2) */
#include "libxml/parser.h"
#include "libxml/parserInternals.h"

#define STACK_MAX 100
#define PROPSIZE 32000 
#define TEXT_SIZE 32000
#define TAG_SIZE 32 

#define MIN(a,b) ((a) < (b) ? (a) : (b))

#define NAME_ID 0

typedef ssize_t nat;

enum kind_t {
      elem
    , attr
    , text
    , comm
    , pi
    , doc
};
typedef enum kind_t kind_t;

#define fb() flush_buffer(NULL, text)

/* definitions for our hashtable */

/* code for no key */
#define NO_KEY -1 

/* returns true if no such key is found */
#define NOKEY(k) (k == NO_KEY)


/**
 * Compression function
 * We use a universal hash function
 */
#define MAD(key) (((123 * key + 593) % PRIME) % HASHTABLE_SIZE)

/* size of the hashtable */
#define HASHTABLE_SIZE 2000 
/* prime number due to bertrands theorem:
 * there exists a prime number p that satisfy,
 * the following condition
 *     HASHTABLE _SIZE < p <= 2 HASHTABLE
 */
#define PRIME 2011

/* 33 has proved  to be a good choice
 * for polynomial hash functions
 */
#define POLY_A 33

/**
 * Hashfunction
 */
int hashfunc(char *str); 

#define HASHFUNCTION(str) MAD(hashfunc(strndup(str, MIN(strlen(str), 10))))

#define GUIDE_PADDING_COUNT 4

/* We use a seperate chaining strategy to
 * mantain out hash_table,
 * So our bucket is a chained list itself,
 * to handle possible collisions.
 */
typedef struct bucket_t bucket_t;
struct bucket_t
{
    char *key;               /**< key as string */
    int id;         /**< name_id */
    bucket_t* next;          /**< next bucket in our list */
};

/* hash table */
bucket_t **hash_table;

/* find element in bucket */
int find_bucket(bucket_t *bucket, char *key);

/* find element in hashtable */
int find_element(bucket_t **hash_table, char *key);

/* add id and key to the bucket list */
bucket_t *bucket_insert(bucket_t *bucket,  char *key, int id);

/* insert key and id to hashtable */
void hashtable_insert(bucket_t **hash_table, char *key, int id);

/* free memory assigned to hash_table */
void free_hash(bucket_t **hash_table);

/* return a brand new name_id */
int new_nameid(void);

typedef struct node_t node_t;
struct node_t {
    nat           pre;
    nat           apre;
    nat           post;
    nat           pre_stretched;
    nat           post_stretched;
    nat           size;
    unsigned int  level;
    int           name_id;
    node_t      * parent;
    kind_t        kind;
    xmlChar     * prop;
    nat           guide;
};

static node_t stack[STACK_MAX];
static int level;
static int max_level;
static nat pre;
static nat post;
static nat rank;
static nat att_id;

static char *format = "%e,%s,%l,%k,%t";
FILE *out;
FILE *out_attr;
char  filename[FILENAME_MAX];
bool  filename_given = false;
char  outfile[FILENAME_MAX];
char  outfile_atts[FILENAME_MAX];
bool  outfile_given = false;
bool  suppress_attributes = false;
bool  sql_atts = false;

static void print_tuple (node_t tuple);
static void flush_buffer (const xmlChar *tagname, kind_t kind);

static xmlChar buf[PROPSIZE + 1];
static int bufpos;

char *strndup(const char *s, size_t n);
char *strdup(const char *s);

FILE  *guide_out;
char   outfile_guide[FILENAME_MAX];

typedef struct child_list_t child_list_t;

typedef struct guide_tree_t guide_tree_t;
struct guide_tree_t {
    char           *tag_name;
    nat             count;
    guide_tree_t   *parent;
    child_list_t   *child_list;
    child_list_t   *last_child;
    nat             guide;
    kind_t          kind;
};

struct child_list_t {
    child_list_t    *next_element;
    guide_tree_t    *node;
};

#define GUIDE_INIT 1

/* current guide count */
nat guide_count = GUIDE_INIT;
/* current node in the guide tree */
guide_tree_t *current_guide_node = NULL;
guide_tree_t *leaf_guide_node = NULL;

/* add a guide child to the parent */
void add_guide_child(guide_tree_t *parent, guide_tree_t *child);

/* insert a node in the guide tree */
guide_tree_t* insert_guide_node(char *tag_name, guide_tree_t 
    *parent, kind_t kind);

/* print the guide tree */
void print_guide_tree(guide_tree_t *root, int tree_depth);


static void
start_document (void *ctx)
{
    /* calling convention */
    (void)ctx;

    pre = 0;
    post = 0;
    rank = 0;
    level = 0;
    max_level = 0;
    att_id = 0;

    current_guide_node = insert_guide_node(filename, NULL, doc);

    stack[level] = (node_t) {
        .pre            = pre,
        .post           = 0,
        .pre_stretched  = rank,
        .post_stretched = 0,
        .size           = 0,
        .level          = level,
        .parent         = NULL,
        .kind           = doc,
        .prop           = filename_given ? (xmlChar*)strndup ((const char*)filename, (size_t)PROPSIZE)
                                                     : (xmlChar*)"",
        .guide          = current_guide_node->guide
    };
}

static void
end_document (void *ctx)
{
    /* calling convention */
    (void)ctx;
    fb ();

    assert (level == 0);

    post++;
    rank++;

    stack[level].post           = post;
    stack[level].post_stretched = rank;
    stack[level].size           = pre - stack[level].pre;

    print_tuple (stack[level]);

    free (stack[level].prop);

    assert (current_guide_node->guide == GUIDE_INIT);
}

static void
start_element (void *ctx, const xmlChar *tagname, const xmlChar **atts)
{
    /* calling convention */
    (void)ctx;

    guide_tree_t *attr_guide_node = NULL;
    current_guide_node = insert_guide_node((char*)tagname, 
        current_guide_node, elem);

    /* check if tagname is larger than TAG_SIZE characters */
    if ( strlen((char*)tagname) > TAG_SIZE) {
        fprintf(stderr, "We support only tagnames with length <= %i\n", 
            TAG_SIZE);

        free_hash (hash_table);
        exit(1);
    }
     
    fb ();

    pre++;
    rank++;
    level++;

    if (level > max_level)
        max_level = level;

    assert (level < STACK_MAX);

    /* try to find the tagname in the
     * hashtable */
    int name_id = -1; 
    if(sql_atts) { 
        name_id = find_element(hash_table, (char*)tagname);

        /* key not found */
        if (NOKEY(name_id)) {

            /* create a new id */
            name_id = new_nameid();     

            hashtable_insert(hash_table, (char*)tagname, name_id);

            fprintf (out_attr, "%i, \"%s\"\n", name_id, 
                strndup((char*)tagname,PROPSIZE));
        }
    }

    stack[level] = (node_t) {
        .pre            = pre,
        .apre           = -1,
        .post           = 0,
        .pre_stretched  = rank,
        .post_stretched = 0,
        .size           = 0,
        .level          = level,
        .parent         = stack + level - 1,
        .name_id        = name_id, 
        .kind           = elem,
        .prop           = (!sql_atts)?(xmlChar*)strndup((const char*)tagname, (size_t)TAG_SIZE):(xmlChar*)NULL,
        .guide          = current_guide_node->guide
    };


    if (!suppress_attributes && atts) {
        if (!sql_atts)
            while (*atts) {
                fprintf (stderr, "foo1\n");
                attr_guide_node = insert_guide_node((char*)atts[0],
                        current_guide_node, attr);

                fprintf (out_attr, SSZFMT ", " SSZFMT ", \"%s\", \"%s\", " SSZFMT "\n", 
                    att_id++, pre, (char*)atts[0], (char*)atts[1], attr_guide_node->guide);

                atts += 2;
            }
    /* handle attributes as we need for sql generation */
    else
       while (*atts) {
            /* check if tagname is larger than TEXT_SIZE characters */
            if ( strlen((char*)atts[1]) > TEXT_SIZE) {
                fprintf(stderr, "We support only attribute content"
                         " with length <= %i\n", TEXT_SIZE);

                free_hash(hash_table);
                     exit(1);
            }
                 /* check if tagname is larger than TAG_SIZE characters */
            if ( strlen((char*)atts[0]) > TAG_SIZE) {
                      fprintf(stderr, "We support only attributes with "
                          "length <= %i\n", TAG_SIZE);

                 free_hash(hash_table);
                      exit(1);
            }
 
            /* try to find the tagname in the
             * hashtable */
            name_id = find_element(hash_table, (char*)atts[0]);

            /* key not found */
            if(NOKEY(name_id)) {

                /* create a new id */
                name_id = new_nameid();     

                hashtable_insert(hash_table, (char*)atts[0], name_id);
                fprintf (out_attr, "%i, \"%s\"\n", name_id, strndup((char*)atts[0],PROPSIZE));
            }

            attr_guide_node = insert_guide_node((char*)atts[0], 
                                        current_guide_node, attr);

            pre++;
                    print_tuple ((node_t) {
                          .pre = pre,
                          .apre = -1,
                          .post = 0,
                          .pre_stretched = 0,
                          .post_stretched = 0,
                          .size = 0,
                          .level = level + 1,
                          .parent = 0,
                          .name_id = name_id,
                          .kind = attr,
                          .prop = (xmlChar*)atts[1],
                          .guide = attr_guide_node->guide,
                       });
            atts += 2;
       }
   }
}

static void
end_element (void *ctx, const xmlChar *name)
{
    (void) ctx;
    (void) name;

    fb ();

    rank++;

    stack[level].post           = post;
    stack[level].post_stretched = rank;
    stack[level].size           = pre - stack[level].pre;

    print_tuple (stack[level]);

    post++;
    free (stack[level].prop);
    level--;
    assert (level >= 0);

    current_guide_node = current_guide_node->parent;
}

void
characters (void *ctx, const xmlChar *chars, int n)
{
    /* calling convention */
    (void)ctx;

    if (bufpos < PROPSIZE) {
        snprintf ((char*)buf + bufpos, MIN (n, PROPSIZE - bufpos) + 1, "%s", (char *)chars);
        bufpos += MIN (n, PROPSIZE - bufpos);
    }

    buf[PROPSIZE] = '\0';
}

void
processing_instruction (void *ctx, const xmlChar *target,  const xmlChar *chars)
{
    /* calling convention */
    (void)ctx;
        (void)chars;
        
    leaf_guide_node = insert_guide_node((char*)target, current_guide_node, pi);

    if (bufpos < PROPSIZE) {
        snprintf ((char*)buf + bufpos, MIN ((int)strlen((char*)target), PROPSIZE - bufpos) + 1, 
                  "%s", (char*)target);
        bufpos += MIN ((int)strlen((char*)target), PROPSIZE - bufpos);
    }

    buf[PROPSIZE] = '\0';

    flush_buffer (target, pi);
}

void
comment (void *ctx,  const xmlChar *chars)
{
    /* calling convention */
        (void)ctx;

    if (bufpos < PROPSIZE) {
        snprintf ((char*)buf + bufpos, MIN ((int)strlen((char*)chars), PROPSIZE - bufpos) + 1, 
            "%s", (char*)chars);
        bufpos += MIN ((int)strlen((char*)chars), PROPSIZE - bufpos);
    }

    buf[PROPSIZE] = '\0';

    flush_buffer (NULL, comm);
}

static void
flush_buffer (const xmlChar *tagname, kind_t kind)
{
    /* check if tagname is larger than TEXT_SIZE characters */
    if (strlen((char*)buf) > TEXT_SIZE) {
        fprintf(stderr, "We support text with length <= %i\n", TEXT_SIZE);

        free_hash(hash_table); 
        exit(1);
    }
    
    if (kind == comm || kind == pi) {
        leaf_guide_node = 
            insert_guide_node(xmlStrdup (tagname), current_guide_node, kind);
        pre++;
        rank += 2;
        level++;

        if (level > max_level)
            max_level = level;
        
        stack[level] = (node_t) {
            .pre            = pre,
            .apre           = -1,
            .post           = post,
            .pre_stretched  = rank - 1,
            .post_stretched = rank,
            .size           = 0,
            .level          = level,
            .parent         = stack + level - 1,
            .name_id        = -1,
            .kind           = leaf_guide_node->kind,
            .prop           = xmlStrdup (tagname),
            .guide          = leaf_guide_node->guide,
        };

        post++;

        print_tuple (stack[level]);

        level--;
    }
    else { 
      if (buf[0]) {
          /* insert leaf guide node */
          leaf_guide_node = 
                insert_guide_node(xmlStrdup (tagname), current_guide_node, kind);

          pre++;
          rank += 2;
          level++;

          if (level > max_level)
              max_level = level;
          
          stack[level] = (node_t) {
              .pre            = pre,
              .apre           = -1,
              .post           = post,
              .pre_stretched  = rank - 1,
              .post_stretched = rank,
              .size           = 0,
              .level          = level,
              .parent         = stack + level - 1,
              .name_id        = -1,
              .kind           = leaf_guide_node->kind,
              .prop           = buf,
              .guide          = leaf_guide_node->guide,
          };

          post++;

          print_tuple (stack[level]);

          level--;
      }

      buf[0] = '\0';
      bufpos = 0;
    }
}

static xmlSAXHandler saxhandler = {
      .startDocument         = start_document
    , .endDocument           = end_document
    , .startElement          = start_element
    , .endElement            = end_element
    , .characters            = characters
    , .processingInstruction = processing_instruction
    , .comment               = comment
    , .error                 = NULL
    , .cdataBlock            = NULL
    , .internalSubset        = NULL
    , .isStandalone          = NULL
    , .hasInternalSubset     = NULL
    , .hasExternalSubset     = NULL
    , .resolveEntity         = NULL
    , .getEntity             = NULL
    , .entityDecl            = NULL
    , .notationDecl          = NULL
    , .attributeDecl         = NULL
    , .elementDecl           = NULL
    , .unparsedEntityDecl    = NULL
    , .setDocumentLocator    = NULL
    , .reference             = NULL
    , .ignorableWhitespace   = NULL
    , .warning               = NULL
    , .fatalError            = NULL
    , .getParameterEntity    = NULL
    , .externalSubset        = NULL
    , .initialized           = false
};

static void
print_help (int argc, char **argv)
{
    (void)argc; 
    printf ("%s - encode XML document in different encodings\n"
            "Usage: %s -h             print this help screen\n"
            "       %s -f <filename>  parse XML file <filename>\n"
            "       %s -o <filename>  output filename\n"
            "       %s -a             suppress attributes\n"
            "       %s -s             sql encoding supported by pathfinder\n"
            "                         \t\t(that is probably what you want)\n"
            "                         Guide node generation.\n",
            argv[0], argv[0], argv[0], argv[0], argv[0], argv[0]);
}

static void
print_kind (FILE *f, kind_t kind) {
    switch (kind) {
       case elem: putc ('1', f); break;
       case attr: putc ('2', f); break;
       case text: putc ('3', f); break;
       case comm: putc ('4', f); break;
       case pi:   putc ('5', f); break;
       case doc:  putc ('6', f); break;
       default: assert (0);
    }
}

static void
print_tuple (node_t tuple)
{
    unsigned int i;
    for (i = 0; format[i]; i++)
        if (format[i] == '%') {
            i++;
	    switch (format[i]) {
		    case 'e':  if(tuple.pre != -1)
				       fprintf (out, SSZFMT , tuple.pre);
			       break;
		    case 'o':  fprintf (out, SSZFMT, tuple.post); break;
		    case 'E':  fprintf (out, SSZFMT, tuple.pre_stretched); break;
		    case 'O':  fprintf (out, SSZFMT, tuple.post_stretched); break;
		    case 's':  fprintf (out, SSZFMT, tuple.size); break;
		    case 'l':  fprintf (out, "%u",  tuple.level); break;

		    case 'p':  
			       if (tuple.parent)
				       fprintf (out, SSZFMT, tuple.parent->pre);
			       else
				       fprintf (out, "NULL");
			       break;
		    case 'P':
			       if (tuple.parent)
				       fprintf (out, SSZFMT, tuple.parent->pre_stretched);
			       else
				       fprintf (out, "NULL");
			       break;
		    case 'k': 
			       print_kind (out, tuple.kind);
			       break;
		    case 'n':
			       if (tuple.name_id != -1)
				       fprintf(out, "%i", tuple.name_id);
			       break;
		    case 'a':
			       if (tuple.apre != -1)
				       fprintf(out, SSZFMT, tuple.apre);
			       break;
		    case 't':
			       if(tuple.prop) {
				       unsigned int i;
				       putc ('"', out);
				       for (i = 0; i < PROPSIZE && tuple.prop[i]; i++)
					       switch (tuple.prop[i]) {
						       case '\n': 
							       putc (' ', out); 
							       break; 
						       case '"':  
							       putc ('"', out); 
							       putc ('"', out); 
							       break;
						       default:   
							       putc (tuple.prop[i], out); 
					       }
				       putc ('"', out);
			       }
			       else {
				       fprintf(out, "NULL");
			       }
	     break;    
                                        case 'g':  fprintf (out, SSZFMT, tuple.guide); break;

                                        default:   putc (format[i], out); break;
                        }
            }
            else
            putc (format[i], out);

    putc ('\n', out);
}

int
main (int argc, char **argv)
{
    xmlParserCtxtPtr  ctx;

    suppress_attributes = false;
    outfile_given = false;

    

    /* parse command line using getopt library */
    while (true) {
        int c = getopt (argc, argv, "F:af:ho:s");

        if (c == -1)
            break;

        switch (c) {

            case 'a':
                suppress_attributes = true;
                break;

            case 'F':
                format = strdup (optarg);
                break;

            case 'f':
                strncpy (filename, optarg, FILENAME_MAX);
                filename_given = true;
                break;

            case 'o':
                strncpy (outfile, optarg, FILENAME_MAX);
                outfile_given = true;
                break;

            case 's':
                sql_atts = true;
                format = "%e, %s, %l, %k, %n, %t, %g";
                break;

            case 'h':
                print_help (argc, argv);
                exit (0);

        }
    }

    /* if we need sql encoding we need to initialize the hashtable */
    if(sql_atts)
        hash_table = (bucket_t**) malloc (HASHTABLE_SIZE * sizeof(bucket_t));

    if (!outfile_given && !suppress_attributes) {
        fprintf (stderr, "Attribute generation requires output filename.\n");
        print_help (argc, argv);
        exit (EXIT_FAILURE);
    }

    if (outfile_given) {

        out = fopen (outfile, "w");
        snprintf (outfile_atts, FILENAME_MAX, (!sql_atts)?"%s_atts":"%s_names", outfile);
        out_attr =  fopen (outfile_atts, "w");

        if (!out || !out_attr) {
            fprintf (stderr, "error opening output file(s).\n");
            exit (EXIT_FAILURE);
        }
    }
    else
        out = stdout;

    /* did we get a filename? */
    if (!filename_given) {
        fprintf (stderr, "No filename given.\n");
        print_help (argc, argv);
        exit (EXIT_FAILURE);
    }

    /* start XML parsing */
    ctx = xmlCreateFileParserCtxt (filename);
    ctx->sax = &saxhandler;

    (void) xmlParseDocument (ctx);

    if (! ctx->wellFormed) {
        fprintf (stderr, "XML input not well-formed\n");
        if (sql_atts)
            free_hash(hash_table);
        exit (EXIT_FAILURE);
    }

    fprintf (stderr, "tree height was %i\n", max_level);
    if (sql_atts)
        fprintf(stderr, "There are %i tagnames and attribute names in "
            "the document\n", new_nameid());

    if (sql_atts)
        free_hash(hash_table);

    /* Open the file */
    if(outfile_given) {
        snprintf (outfile_guide, FILENAME_MAX, "%s_guide.xml", outfile);
        guide_out = fopen(outfile_guide, "w");
        if (!guide_out) {
            fprintf (stderr, "error opening output file(s).\n");
            exit (EXIT_FAILURE);
        }

    } else 
        guide_out = stdout;
    

    print_guide_tree(current_guide_node, 0);
    
    return 0;
}

/**
 * Hashfunction
 * You should use the macro #HASHFUNCTON 
 * to apply the the function only to a
 * fragment of the string
 */
int hashfunc(char *str)
{
    /* appliing horners rule */
    int x;
    int k = strlen(str);
    k--; 

    x = (int)str[k]-'a';
    if(k == 0) {
        return x % PRIME;
    }
    return (x + POLY_A * hashfunc(strndup(str, k))) % PRIME; 
}



/* find element in bucket */
int find_bucket(bucket_t *bucket, char *key)
{
    bucket_t *actbucket = bucket;
    while (actbucket)
    {
        if (strcmp(actbucket->key, key)==0)
        {
           return actbucket->id;
        }
        else
            actbucket = actbucket->next;
    }
    return NO_KEY;
}

/* find element in hashtable */
int find_element(bucket_t **hash_table, char *key)
{
    return find_bucket(hash_table[HASHFUNCTION(key)],key);
}

/* return a brand new name_id */
int new_nameid(void)
{
    static unsigned int name = NAME_ID;
    return name++;
}

/* add id and key to the bucket list */
bucket_t *bucket_insert(bucket_t *bucket,  char *key, int id)
{
    int ident = find_bucket(bucket, key);
    bucket_t *actbucket = NULL;
    /* no key found */
    if( ident == -1) {
        actbucket = (bucket_t*) malloc(sizeof(bucket_t));
        actbucket->id = id;
        actbucket->key = strndup(key,strlen(key));
        /* add actbucket to the front of list */
        actbucket->next = bucket;
        return actbucket;
    }
    else {
        return bucket;
    }
    /* satisfy picky compilers */
    return NULL;
}

/* insert key and id into hashtable*/
void hashtable_insert(bucket_t **hash_table, char *key, int id)
{
     assert (hash_table != NULL);
     int hashkey = HASHFUNCTION(key);
     hash_table[hashkey] = bucket_insert(hash_table[hashkey], key, id); 
     return;
}

/* free memory assigned to hash_table */
void free_hash(bucket_t **hash_table)
{
   assert (hash_table != NULL);
   int i = 0;
   if(hash_table) return;

   for(i = 0; i < HASHTABLE_SIZE; i++) {
        free(hash_table[i]);
   }
}


void 
add_guide_child(guide_tree_t *parent, guide_tree_t *child)
{

     if((parent == NULL) || (child == NULL))
         return;

     /* insert new child in the list */
     child_list_t *new_child_list = (child_list_t*)malloc(sizeof(child_list_t));
     *new_child_list = (child_list_t){
         .next_element = NULL,
         .node  = child,
     };

     if(parent->child_list == NULL) {
         parent->child_list = new_child_list;
     } else {
         parent->last_child->next_element = new_child_list;
     }

     parent->last_child = new_child_list;
     child->parent = parent;

     return;
}

guide_tree_t* 
insert_guide_node(char *tag_name, guide_tree_t *parent, kind_t kind)
{
    child_list_t *child_list = NULL;
    guide_tree_t *child_node = NULL;
    guide_tree_t *new_guide_node = NULL;

    if (parent != NULL) {
        /* Search all children and check if the node already exist */
        child_list = parent->child_list;
            
        while(child_list != NULL) {
            child_node = child_list->node;
            #define TAG(k) \
                  (((k) == (doc)) || ((k) == (elem)) \
                   || ((k) == (pi)) || ((k) == (attr))) 

            if (((!TAG(kind)) && child_node->kind==kind) ||
                 (TAG(kind) && (child_node->kind == kind) &&
                  strcmp (child_node->tag_name, tag_name)==0)) {

                child_node->count = child_node->count + 1;
                return child_node;
            }
            child_list = child_list->next_element;
        } 
    }

    /* create a new guide node */
    new_guide_node = (guide_tree_t*)malloc(sizeof(guide_tree_t));
    *new_guide_node = (guide_tree_t) {
        .tag_name = tag_name,
        .count = 1,
        .parent = parent,
        .child_list = NULL, 
        .last_child = NULL,
        .guide = guide_count,
        .kind = kind,
    };
    /* increment the guide count */ 
    guide_count++;

    /* associate child with the parent */
    add_guide_child(parent, new_guide_node);
    
    return new_guide_node;
}

void 
print_guide_tree(guide_tree_t *root, int tree_depth)
{
    child_list_t  *child_list = root->child_list;
    child_list_t  *child_list_free = NULL;
    bool           print_end_tag = true;
    int            i;

    /* print the padding */
    for(i = 0; i < tree_depth * GUIDE_PADDING_COUNT; i++)
      fprintf(guide_out, " ");

    print_end_tag = child_list == NULL ? false : true;

    /* print the node self */
    fprintf (
        guide_out, 
        "<node guide=\"%i\" count=\"%i\" kind=\"",
        root->guide,
        root->count);
    print_kind (guide_out, root->kind);
    fprintf (guide_out, "\"");
    if (root->tag_name != NULL)
        fprintf (guide_out, " name=\"%s\"", root->tag_name);
    fprintf (guide_out, "%s>\n", print_end_tag ? "" : "/"); 

    while(child_list != NULL) {
       print_guide_tree(child_list->node, tree_depth+1);
       child_list_free = child_list;
       child_list = child_list->next_element;
       if(child_list_free != NULL)
           free(child_list_free);
    }

    /* print the end tag */
    if (print_end_tag) {
        for(i = 0; i < tree_depth * GUIDE_PADDING_COUNT; i++)
            fprintf(guide_out, " ");

        fprintf(guide_out, "</node>\n");
    }

    free(root);
    root = NULL;
}




